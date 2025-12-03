// Package vectordb provides a client for Qdrant vector database operations.
package vectordb

import (
	"context"
	"encoding/hex"
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
		Id:      qdrant.NewID(toUUID(id)),
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
		Id:      qdrant.NewID(toUUID(id)),
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
					Ids: []*qdrant.PointId{qdrant.NewID(toUUID(id))},
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

// toUUID converts a ULID string to UUID format.
// ULIDs are 128-bit identifiers that can be represented as UUIDs.
// If the input is already a valid UUID, it's returned as-is.
// If conversion fails, returns the original string (Qdrant will validate).
func toUUID(id string) string {
	// Check if already UUID format (contains hyphens)
	if len(id) == 36 && id[8] == '-' && id[13] == '-' && id[18] == '-' && id[23] == '-' {
		return id
	}

	// Try to parse as ULID (26 characters, Crockford base32)
	if len(id) != 26 {
		return id // Not a ULID, return as-is
	}

	// Decode ULID from Crockford base32 to bytes
	bytes, err := decodeULID(id)
	if err != nil {
		return id // Invalid ULID, return as-is
	}

	// Format as UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(bytes[0:4]),
		hex.EncodeToString(bytes[4:6]),
		hex.EncodeToString(bytes[6:8]),
		hex.EncodeToString(bytes[8:10]),
		hex.EncodeToString(bytes[10:16]))
}

// decodeULID decodes a ULID string (Crockford base32) to 16 bytes
func decodeULID(s string) ([]byte, error) {
	if len(s) != 26 {
		return nil, fmt.Errorf("invalid ULID length: %d", len(s))
	}

	// Crockford's Base32 decoding table (supports uppercase)
	dec := map[byte]byte{
		'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9,
		'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15, 'G': 16, 'H': 17,
		'J': 18, 'K': 19, 'M': 20, 'N': 21, 'P': 22, 'Q': 23, 'R': 24, 'S': 25,
		'T': 26, 'V': 27, 'W': 28, 'X': 29, 'Y': 30, 'Z': 31,
		// Lowercase
		'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15, 'g': 16, 'h': 17,
		'j': 18, 'k': 19, 'm': 20, 'n': 21, 'p': 22, 'q': 23, 'r': 24, 's': 25,
		't': 26, 'v': 27, 'w': 28, 'x': 29, 'y': 30, 'z': 31,
		// Common substitutions
		'O': 0, 'o': 0, 'I': 1, 'i': 1, 'L': 1, 'l': 1,
	}

	// Decode all 26 characters to 5-bit values
	var vals [26]byte
	for i := 0; i < 26; i++ {
		v, ok := dec[s[i]]
		if !ok {
			return nil, fmt.Errorf("invalid ULID character: %c", s[i])
		}
		vals[i] = v
	}

	// Pack 26 * 5 = 130 bits into 16 bytes (128 bits used, 2 MSBs of first char ignored)
	// ULID layout: 10 chars timestamp (48 bits) + 16 chars randomness (80 bits)
	var result [16]byte

	// Timestamp: chars 0-9 -> bytes 0-5 (48 bits)
	result[0] = (vals[0] << 5) | vals[1]
	result[1] = (vals[2] << 3) | (vals[3] >> 2)
	result[2] = (vals[3] << 6) | (vals[4] << 1) | (vals[5] >> 4)
	result[3] = (vals[5] << 4) | (vals[6] >> 1)
	result[4] = (vals[6] << 7) | (vals[7] << 2) | (vals[8] >> 3)
	result[5] = (vals[8] << 5) | vals[9]

	// Randomness: chars 10-25 -> bytes 6-15 (80 bits)
	result[6] = (vals[10] << 3) | (vals[11] >> 2)
	result[7] = (vals[11] << 6) | (vals[12] << 1) | (vals[13] >> 4)
	result[8] = (vals[13] << 4) | (vals[14] >> 1)
	result[9] = (vals[14] << 7) | (vals[15] << 2) | (vals[16] >> 3)
	result[10] = (vals[16] << 5) | vals[17]
	result[11] = (vals[18] << 3) | (vals[19] >> 2)
	result[12] = (vals[19] << 6) | (vals[20] << 1) | (vals[21] >> 4)
	result[13] = (vals[21] << 4) | (vals[22] >> 1)
	result[14] = (vals[22] << 7) | (vals[23] << 2) | (vals[24] >> 3)
	result[15] = (vals[24] << 5) | vals[25]

	return result[:], nil
}

