// Package vectordb provides a client for Qdrant vector database operations.
package vectordb

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/qdrant/go-client/qdrant"
)

const (
	// SourcesCollection is the collection name for source embeddings
	SourcesCollection = "sources"
	// ArticlesCollection is the collection name for article embeddings
	ArticlesCollection = "articles"
	// DefaultVectorSize is the default embedding dimension (nomic-embed-text)
	DefaultVectorSize = 768
)

// Client provides vector database operations via Qdrant
type Client struct {
	client *qdrant.Client
}

// SourcePayload contains the metadata stored alongside source embeddings
type SourcePayload struct {
	ID        string `json:"id"`
	URL       string `json:"url"`
	Title     string `json:"title"`
	Topic     string `json:"topic"`
	Summary   string `json:"summary"`
	Language  string `json:"language,omitempty"`
	Model     string `json:"model,omitempty"`
	CreatedAt string `json:"created_at"`
}

// ArticlePayload contains the metadata stored alongside article embeddings
type ArticlePayload struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Path     string   `json:"path"`
	Summary  string   `json:"summary"`
	Tags     []string `json:"tags"`
	Category string   `json:"category"`
}

// SearchResult represents a search result with score and payload
type SearchResult struct {
	ID      string
	Score   float32
	Payload map[string]interface{}
}

// NewClient creates a new Qdrant client
func NewClient() (*Client, error) {
	host := os.Getenv("QDRANT_HOST")
	if host == "" {
		host = "localhost"
	}

	portStr := os.Getenv("QDRANT_PORT")
	port := 6334 // Default gRPC port
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	return &Client{client: client}, nil
}

// NewClientWithConfig creates a new Qdrant client with explicit configuration
func NewClientWithConfig(host string, port int) (*Client, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	return &Client{client: client}, nil
}

// EnsureCollections creates the required collections if they don't exist
func (c *Client) EnsureCollections(ctx context.Context) error {
	collections := []string{SourcesCollection, ArticlesCollection}

	for _, name := range collections {
		exists, err := c.collectionExists(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to check collection %s: %w", name, err)
		}

		if !exists {
			if err := c.createCollection(ctx, name); err != nil {
				return fmt.Errorf("failed to create collection %s: %w", name, err)
			}
		}
	}

	return nil
}

func (c *Client) collectionExists(ctx context.Context, name string) (bool, error) {
	collections, err := c.client.ListCollections(ctx)
	if err != nil {
		return false, err
	}

	for _, col := range collections {
		if col == name {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) createCollection(ctx context.Context, name string) error {
	return c.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: name,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     DefaultVectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
	})
}

// UpsertSource stores or updates a source embedding
func (c *Client) UpsertSource(ctx context.Context, id string, embedding []float32, payload SourcePayload) error {
	point := &qdrant.PointStruct{
		Id:      qdrant.NewID(id),
		Vectors: qdrant.NewVectors(embedding...),
		Payload: qdrant.NewValueMap(map[string]interface{}{
			"id":         payload.ID,
			"url":        payload.URL,
			"title":      payload.Title,
			"topic":      payload.Topic,
			"summary":    payload.Summary,
			"language":   payload.Language,
			"model":      payload.Model,
			"created_at": payload.CreatedAt,
		}),
	}

	_, err := c.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: SourcesCollection,
		Points:         []*qdrant.PointStruct{point},
	})
	return err
}

// UpsertArticle stores or updates an article embedding
func (c *Client) UpsertArticle(ctx context.Context, id string, embedding []float32, payload ArticlePayload) error {
	point := &qdrant.PointStruct{
		Id:      qdrant.NewID(id),
		Vectors: qdrant.NewVectors(embedding...),
		Payload: qdrant.NewValueMap(map[string]interface{}{
			"id":       payload.ID,
			"title":    payload.Title,
			"path":     payload.Path,
			"summary":  payload.Summary,
			"tags":     payload.Tags,
			"category": payload.Category,
		}),
	}

	_, err := c.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: ArticlesCollection,
		Points:         []*qdrant.PointStruct{point},
	})
	return err
}

// SearchSources searches for similar sources using vector similarity
func (c *Client) SearchSources(ctx context.Context, embedding []float32, limit int, topicFilter string) ([]SearchResult, error) {
	query := &qdrant.QueryPoints{
		CollectionName: SourcesCollection,
		Query:          qdrant.NewQuery(embedding...),
		Limit:          qdrant.PtrOf(uint64(limit)),
		WithPayload:    qdrant.NewWithPayload(true),
	}

	// Add topic filter if specified
	if topicFilter != "" {
		query.Filter = &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("topic", topicFilter),
			},
		}
	}

	results, err := c.client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	return convertResults(results), nil
}

// SearchArticles searches for similar articles using vector similarity
func (c *Client) SearchArticles(ctx context.Context, embedding []float32, limit int, categoryFilter string) ([]SearchResult, error) {
	query := &qdrant.QueryPoints{
		CollectionName: ArticlesCollection,
		Query:          qdrant.NewQuery(embedding...),
		Limit:          qdrant.PtrOf(uint64(limit)),
		WithPayload:    qdrant.NewWithPayload(true),
	}

	// Add category filter if specified
	if categoryFilter != "" {
		query.Filter = &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("category", categoryFilter),
			},
		}
	}

	results, err := c.client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	return convertResults(results), nil
}

// GetSourcesByTopic retrieves all sources for a specific topic
func (c *Client) GetSourcesByTopic(ctx context.Context, topic string, limit int) ([]SearchResult, error) {
	// Use scroll to get all sources with topic filter (no vector needed)
	points, err := c.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: SourcesCollection,
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("topic", topic),
			},
		},
		Limit:       qdrant.PtrOf(uint32(limit)),
		WithPayload: qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("scroll failed: %w", err)
	}

	var results []SearchResult
	for _, point := range points {
		results = append(results, SearchResult{
			ID:      point.Id.GetUuid(),
			Score:   1.0, // No score for scroll
			Payload: extractPayload(point.Payload),
		})
	}
	return results, nil
}

// DeleteSource removes a source from the vector database
func (c *Client) DeleteSource(ctx context.Context, id string) error {
	_, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: SourcesCollection,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{
					Ids: []*qdrant.PointId{qdrant.NewID(id)},
				},
			},
		},
	})
	return err
}

// Close closes the Qdrant client connection
func (c *Client) Close() error {
	return c.client.Close()
}

// convertResults converts Qdrant scored points to SearchResults
func convertResults(points []*qdrant.ScoredPoint) []SearchResult {
	results := make([]SearchResult, len(points))
	for i, point := range points {
		results[i] = SearchResult{
			ID:      point.Id.GetUuid(),
			Score:   point.Score,
			Payload: extractPayload(point.Payload),
		}
	}
	return results
}

// extractPayload converts Qdrant payload to a map
func extractPayload(payload map[string]*qdrant.Value) map[string]interface{} {
	result := make(map[string]interface{})
	for key, val := range payload {
		result[key] = extractValue(val)
	}
	return result
}

// extractValue converts a Qdrant value to a Go value
func extractValue(val *qdrant.Value) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.Kind.(type) {
	case *qdrant.Value_StringValue:
		return v.StringValue
	case *qdrant.Value_IntegerValue:
		return v.IntegerValue
	case *qdrant.Value_DoubleValue:
		return v.DoubleValue
	case *qdrant.Value_BoolValue:
		return v.BoolValue
	case *qdrant.Value_ListValue:
		list := make([]interface{}, len(v.ListValue.Values))
		for i, item := range v.ListValue.Values {
			list[i] = extractValue(item)
		}
		return list
	default:
		return nil
	}
}

