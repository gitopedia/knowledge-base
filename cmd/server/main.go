// Package main provides the knowledge-base HTTP API server.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gitopedia/knowledge-base/internal/database"
	"github.com/gitopedia/knowledge-base/internal/embedding"
	"github.com/gitopedia/knowledge-base/internal/vectordb"
)

// Server holds the dependencies for the HTTP API
type Server struct {
	db       *database.DB
	vectorDB *vectordb.Client
	embedder *embedding.Client
}

// SourceRequest is the request body for creating/updating a source
type SourceRequest struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Title     string   `json:"title"`
	Topic     string   `json:"topic"`
	Summary   string   `json:"summary"`
	Language  string   `json:"language,omitempty"`
	Model     string   `json:"model,omitempty"`
	CreatedAt string   `json:"created_at,omitempty"`
	Tags      []string `json:"tags,omitempty"`
}

// SearchRequest is the request body for vector search
type SearchRequest struct {
	Query     string  `json:"query,omitempty"`     // Text to embed and search
	Embedding string  `json:"embedding,omitempty"` // Base64-encoded embedding (alternative to query)
	Limit     int     `json:"limit,omitempty"`
	Topic     string  `json:"topic,omitempty"` // Optional topic filter
}

// SearchResponse is the response for search endpoints
type SearchResponse struct {
	Results []SearchResult `json:"results"`
	Count   int            `json:"count"`
}

// SearchResult represents a single search result
type SearchResult struct {
	ID        string   `json:"id"`
	URL       string   `json:"url,omitempty"`
	Title     string   `json:"title"`
	Topic     string   `json:"topic,omitempty"`
	Summary   string   `json:"summary"`
	Score     float32  `json:"score,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	Language  string   `json:"language,omitempty"`
	Model     string   `json:"model,omitempty"`
	CreatedAt string   `json:"created_at,omitempty"`
}

// HealthResponse is the response for the health endpoint
type HealthResponse struct {
	Status       string `json:"status"`
	SourceCount  int    `json:"source_count"`
	ArticleCount int    `json:"article_count"`
	Version      string `json:"version"`
}

// ErrorResponse is the response for errors
type ErrorResponse struct {
	Error string `json:"error"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Configuration
	port := os.Getenv("KB_PORT")
	if port == "" {
		port = "8081"
	}

	dbPath := os.Getenv("KB_DB_PATH")
	if dbPath == "" {
		dbPath = "/app/data/knowledge.sqlite"
	}

	// Initialize database
	log.Printf("Opening database at %s", dbPath)
	db, err := database.Open(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Initialize Qdrant client
	log.Println("Connecting to Qdrant...")
	vectorDB, err := vectordb.NewClient()
	if err != nil {
		log.Fatalf("Failed to connect to Qdrant: %v", err)
	}
	defer vectorDB.Close()

	// Ensure collections exist
	ctx := context.Background()
	if err := vectorDB.EnsureCollections(ctx); err != nil {
		log.Fatalf("Failed to ensure Qdrant collections: %v", err)
	}
	log.Println("Qdrant collections ready")

	// Initialize embedding client
	embedder := embedding.NewClient()
	log.Printf("Embedding client ready (model: %s)", embedder.Model())

	// Create server
	server := &Server{
		db:       db,
		vectorDB: vectorDB,
		embedder: embedder,
	}

	// Setup routes
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /health", server.handleHealth)

	// Source endpoints
	mux.HandleFunc("POST /sources", server.handleCreateSource)
	mux.HandleFunc("GET /sources/{id}", server.handleGetSource)
	mux.HandleFunc("DELETE /sources/{id}", server.handleDeleteSource)
	mux.HandleFunc("GET /sources", server.handleListSources)

	// Search endpoints
	mux.HandleFunc("POST /sources/search", server.handleSearchSources)
	mux.HandleFunc("GET /sources/search", server.handleSearchSourcesGET)
	mux.HandleFunc("GET /sources/topic/{topic}", server.handleGetSourcesByTopic)

	// Article search (uses existing article index)
	mux.HandleFunc("POST /articles/search", server.handleSearchArticles)
	mux.HandleFunc("GET /articles/search", server.handleSearchArticlesGET)

	// Wrap with logging middleware
	handler := loggingMiddleware(corsMiddleware(mux))

	// Start server
	httpServer := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}()

	log.Printf("Knowledge-base API server listening on port %s", port)
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
	log.Println("Server stopped")
}

// Middleware

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Handlers

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	sourceCount, _ := s.db.CountSources()
	articleCount, _ := s.db.CountArticles()

	version, _ := s.db.GetInfo("version")
	if version == "" {
		version = "unknown"
	}

	resp := HealthResponse{
		Status:       "ok",
		SourceCount:  sourceCount,
		ArticleCount: articleCount,
		Version:      version,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	var req SourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if req.URL == "" || req.Summary == "" {
		writeError(w, http.StatusBadRequest, "url and summary are required")
		return
	}

	// Generate ID if not provided
	if req.ID == "" {
		req.ID = fmt.Sprintf("src-%d", time.Now().UnixNano())
	}

	// Set created_at if not provided
	if req.CreatedAt == "" {
		req.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	// Generate embedding
	ctx := r.Context()
	emb, err := s.embedder.Embed(ctx, req.Summary)
	if err != nil {
		log.Printf("Failed to generate embedding: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to generate embedding")
		return
	}

	// Store in SQLite
	src := database.Source{
		ID:        req.ID,
		URL:       req.URL,
		Title:     req.Title,
		Topic:     req.Topic,
		Summary:   req.Summary,
		Language:  req.Language,
		Model:     req.Model,
		CreatedAt: req.CreatedAt,
		Tags:      req.Tags,
	}
	if err := s.db.InsertSource(src); err != nil {
		log.Printf("Failed to insert source: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to store source")
		return
	}

	// Store in Qdrant
	payload := vectordb.SourcePayload{
		ID:        req.ID,
		URL:       req.URL,
		Title:     req.Title,
		Topic:     req.Topic,
		Summary:   req.Summary,
		Language:  req.Language,
		Model:     req.Model,
		CreatedAt: req.CreatedAt,
	}
	if err := s.vectorDB.UpsertSource(ctx, req.ID, emb, payload); err != nil {
		log.Printf("Failed to store embedding: %v", err)
		// Don't fail the request - SQLite has the data
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID})
}

func (s *Server) handleGetSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	src, err := s.db.GetSource(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Database error")
		return
	}
	if src == nil {
		writeError(w, http.StatusNotFound, "Source not found")
		return
	}

	writeJSON(w, http.StatusOK, src)
}

func (s *Server) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	// Delete from SQLite
	if err := s.db.DeleteSource(id); err != nil {
		log.Printf("Failed to delete source from SQLite: %v", err)
	}

	// Delete from Qdrant
	ctx := r.Context()
	if err := s.vectorDB.DeleteSource(ctx, id); err != nil {
		log.Printf("Failed to delete source from Qdrant: %v", err)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	topic := r.URL.Query().Get("topic")
	var sources []database.Source
	var err error

	if topic != "" {
		sources, err = s.db.GetSourcesByTopic(topic, limit)
	} else {
		// Get all sources (limited)
		sources, err = s.db.GetSourcesByTopic("", limit)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "Database error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sources": sources,
		"count":   len(sources),
	})
}

func (s *Server) handleSearchSources(w http.ResponseWriter, r *http.Request) {
	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.searchSources(w, r, req)
}

func (s *Server) handleSearchSourcesGET(w http.ResponseWriter, r *http.Request) {
	req := SearchRequest{
		Query: r.URL.Query().Get("q"),
		Topic: r.URL.Query().Get("topic"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			req.Limit = l
		}
	}

	s.searchSources(w, r, req)
}

func (s *Server) searchSources(w http.ResponseWriter, r *http.Request, req SearchRequest) {
	if req.Query == "" && req.Embedding == "" {
		writeError(w, http.StatusBadRequest, "query or embedding is required")
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	ctx := r.Context()
	var emb []float32
	var err error

	if req.Embedding != "" {
		// Decode base64 embedding
		emb, err = decodeEmbedding(req.Embedding)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid embedding format")
			return
		}
	} else {
		// Generate embedding from query
		emb, err = s.embedder.Embed(ctx, req.Query)
		if err != nil {
			log.Printf("Failed to generate embedding: %v", err)
			writeError(w, http.StatusInternalServerError, "Failed to generate embedding")
			return
		}
	}

	// Search Qdrant
	results, err := s.vectorDB.SearchSources(ctx, emb, req.Limit, req.Topic)
	if err != nil {
		log.Printf("Vector search failed: %v", err)
		writeError(w, http.StatusInternalServerError, "Search failed")
		return
	}

	// Convert to response format
	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = SearchResult{
			ID:        r.ID,
			Score:     r.Score,
			URL:       getString(r.Payload, "url"),
			Title:     getString(r.Payload, "title"),
			Topic:     getString(r.Payload, "topic"),
			Summary:   getString(r.Payload, "summary"),
			Language:  getString(r.Payload, "language"),
			Model:     getString(r.Payload, "model"),
			CreatedAt: getString(r.Payload, "created_at"),
		}
	}

	writeJSON(w, http.StatusOK, SearchResponse{
		Results: searchResults,
		Count:   len(searchResults),
	})
}

func (s *Server) handleGetSourcesByTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "topic is required")
		return
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	sources, err := s.db.GetSourcesByTopic(topic, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Database error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sources": sources,
		"count":   len(sources),
	})
}

func (s *Server) handleSearchArticles(w http.ResponseWriter, r *http.Request) {
	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.searchArticles(w, r, req)
}

func (s *Server) handleSearchArticlesGET(w http.ResponseWriter, r *http.Request) {
	req := SearchRequest{
		Query: r.URL.Query().Get("q"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			req.Limit = l
		}
	}

	s.searchArticles(w, r, req)
}

func (s *Server) searchArticles(w http.ResponseWriter, r *http.Request, req SearchRequest) {
	if req.Query == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	// Use FTS search for articles
	articles, err := s.db.SearchArticles(req.Query, req.Limit)
	if err != nil {
		log.Printf("Article search failed: %v", err)
		writeError(w, http.StatusInternalServerError, "Search failed")
		return
	}

	// Convert to response format
	results := make([]SearchResult, len(articles))
	for i, a := range articles {
		results[i] = SearchResult{
			ID:      a.ID,
			Title:   a.Title,
			Summary: a.Summary,
			Tags:    a.Tags,
		}
	}

	writeJSON(w, http.StatusOK, SearchResponse{
		Results: results,
		Count:   len(results),
	})
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func decodeEmbedding(encoded string) ([]float32, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}

	if len(data)%4 != 0 {
		return nil, fmt.Errorf("invalid embedding length")
	}

	embedding := make([]float32, len(data)/4)
	for i := range embedding {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		embedding[i] = math.Float32frombits(bits)
	}

	return embedding, nil
}

