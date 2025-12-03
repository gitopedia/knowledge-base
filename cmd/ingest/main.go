// Package main provides the source ingestion pipeline for the knowledge-base.
// It reads source markdown files from _incoming/sources/, generates embeddings,
// stores them in SQLite + Qdrant, and optionally deletes the source files.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gitopedia/knowledge-base/internal/database"
	"github.com/gitopedia/knowledge-base/internal/embedding"
	"github.com/gitopedia/knowledge-base/internal/vectordb"
	"gopkg.in/yaml.v3"
)

// SourceFrontMatter represents the YAML front matter of a source file
type SourceFrontMatter struct {
	ID             string   `yaml:"id"`
	Slug           string   `yaml:"slug"`
	Title          string   `yaml:"title"`
	URL            string   `yaml:"url"`
	Type           string   `yaml:"type"`
	RelatedArticle string   `yaml:"related_article"`
	Created        string   `yaml:"created"`
	Tags           []string `yaml:"tags"`
	People         []string `yaml:"people"`
	Orgs           []string `yaml:"orgs"`
	Places         []string `yaml:"places"`
	Summary        string   `yaml:"summary"`
	Model          string   `yaml:"model"`
	Language       string   `yaml:"language"`
}

func main() {
	// Flags
	sourcesDir := flag.String("sources", "", "Path to _incoming/sources directory")
	dbPath := flag.String("db", "", "Path to SQLite database")
	deleteAfter := flag.Bool("delete", false, "Delete source files after ingestion")
	dryRun := flag.Bool("dry-run", false, "Show what would be done without making changes")
	flag.Parse()

	// Determine sources directory
	if *sourcesDir == "" {
		// Try to find it relative to current directory or via env var
		gitopediaDir := os.Getenv("GITOPEDIA_DIR")
		if gitopediaDir == "" {
			// Assume we're in knowledge-base, look for ../gitopedia/Compendium/_incoming/sources
			cwd, _ := os.Getwd()
			gitopediaDir = filepath.Join(cwd, "../gitopedia/Compendium")
		}
		*sourcesDir = filepath.Join(gitopediaDir, "_incoming/sources")
	}

	// Determine database path
	if *dbPath == "" {
		*dbPath = os.Getenv("KB_DB_PATH")
		if *dbPath == "" {
			cwd, _ := os.Getwd()
			*dbPath = filepath.Join(cwd, "out/knowledge.sqlite")
		}
	}

	log.Printf("Sources directory: %s", *sourcesDir)
	log.Printf("Database path: %s", *dbPath)
	log.Printf("Delete after ingestion: %v", *deleteAfter)
	log.Printf("Dry run: %v", *dryRun)

	// Check sources directory exists
	if _, err := os.Stat(*sourcesDir); os.IsNotExist(err) {
		log.Printf("Sources directory does not exist: %s", *sourcesDir)
		log.Println("No sources to ingest.")
		return
	}

	// Initialize database
	var db *database.DB
	var vectorDB *vectordb.Client
	var embedder *embedding.Client

	if !*dryRun {
		var err error
		db, err = database.Open(*dbPath)
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Initialize Qdrant
		vectorDB, err = vectordb.NewClient()
		if err != nil {
			log.Fatalf("Failed to connect to Qdrant: %v", err)
		}
		defer vectorDB.Close()

		ctx := context.Background()
		if err := vectorDB.EnsureCollections(ctx); err != nil {
			log.Fatalf("Failed to ensure Qdrant collections: %v", err)
		}

		// Initialize embedding client
		embedder = embedding.NewClient()
		log.Printf("Embedding model: %s", embedder.Model())
	}

	// Walk sources directory
	var sourceFiles []string
	err := filepath.WalkDir(*sourcesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".md") {
			sourceFiles = append(sourceFiles, path)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to walk sources directory: %v", err)
	}

	log.Printf("Found %d source files", len(sourceFiles))

	// Process each source file
	var processed, skipped, errors int
	var filesToDelete []string

	for _, path := range sourceFiles {
		log.Printf("Processing: %s", filepath.Base(path))

		// Parse source file
		fm, body, err := parseSourceFile(path)
		if err != nil {
			log.Printf("  Error parsing: %v", err)
			errors++
			continue
		}

		// Validate required fields
		if fm.URL == "" {
			log.Printf("  Skipping: no URL")
			skipped++
			continue
		}

		// Use body as summary if not in frontmatter
		summary := fm.Summary
		if summary == "" {
			summary = strings.TrimSpace(body)
		}
		if summary == "" {
			log.Printf("  Skipping: no summary content")
			skipped++
			continue
		}

		// Extract topic from related_article or filename
		topic := fm.RelatedArticle
		if topic == "" {
			// Try to extract from filename (e.g., "quantum-mechanics--example-com-1.md")
			base := filepath.Base(path)
			base = strings.TrimSuffix(base, ".md")
			parts := strings.Split(base, "--")
			if len(parts) > 0 {
				topic = parts[0]
			}
		}

		if *dryRun {
			log.Printf("  Would ingest: ID=%s, URL=%s, Topic=%s", fm.ID, fm.URL, topic)
			processed++
			continue
		}

		// Check if source already exists (by URL)
		existing, err := db.GetSourceByURL(fm.URL)
		if err != nil {
			log.Printf("  Error checking existing: %v", err)
			errors++
			continue
		}
		if existing != nil {
			log.Printf("  Skipping: URL already exists (ID=%s)", existing.ID)
			skipped++
			// Still mark for deletion if requested
			if *deleteAfter {
				filesToDelete = append(filesToDelete, path)
			}
			continue
		}

		// Generate ID if not present
		id := fm.ID
		if id == "" {
			id = fmt.Sprintf("src-%d", time.Now().UnixNano())
		}

		// Set created time
		createdAt := fm.Created
		if createdAt == "" {
			createdAt = time.Now().UTC().Format(time.RFC3339)
		}

		// Generate embedding
		ctx := context.Background()
		emb, err := embedder.Embed(ctx, summary)
		if err != nil {
			log.Printf("  Error generating embedding: %v", err)
			errors++
			continue
		}

		// Store in SQLite
		src := database.Source{
			ID:        id,
			URL:       fm.URL,
			Title:     fm.Title,
			Topic:     topic,
			Summary:   summary,
			Language:  fm.Language,
			Model:     fm.Model,
			CreatedAt: createdAt,
			Tags:      fm.Tags,
		}
		if err := db.InsertSource(src); err != nil {
			log.Printf("  Error storing in SQLite: %v", err)
			errors++
			continue
		}

		// Store in Qdrant
		payload := vectordb.SourcePayload{
			ID:        id,
			URL:       fm.URL,
			Title:     fm.Title,
			Topic:     topic,
			Summary:   summary,
			Language:  fm.Language,
			Model:     fm.Model,
			CreatedAt: createdAt,
		}
		if err := vectorDB.UpsertSource(ctx, id, emb, payload); err != nil {
			log.Printf("  Warning: failed to store in Qdrant: %v", err)
			// Don't fail - SQLite has the data
		}

		log.Printf("  Ingested: ID=%s", id)
		processed++

		if *deleteAfter {
			filesToDelete = append(filesToDelete, path)
		}
	}

	// Delete processed files if requested
	if *deleteAfter && len(filesToDelete) > 0 {
		log.Printf("Deleting %d processed source files...", len(filesToDelete))
		for _, path := range filesToDelete {
			if err := os.Remove(path); err != nil {
				log.Printf("  Failed to delete %s: %v", filepath.Base(path), err)
			} else {
				log.Printf("  Deleted: %s", filepath.Base(path))
			}
		}
	}

	log.Printf("Ingestion complete: %d processed, %d skipped, %d errors", processed, skipped, errors)
}

// parseSourceFile reads and parses a source markdown file
func parseSourceFile(path string) (SourceFrontMatter, string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return SourceFrontMatter{}, "", err
	}

	s := string(content)
	var fm SourceFrontMatter
	body := s

	// Parse YAML front matter
	if strings.HasPrefix(s, "---") {
		parts := strings.SplitN(s, "---", 3)
		if len(parts) >= 3 {
			if err := yaml.Unmarshal([]byte(parts[1]), &fm); err != nil {
				return fm, "", fmt.Errorf("invalid YAML front matter: %w", err)
			}
			body = strings.TrimSpace(parts[2])
		}
	}

	return fm, body, nil
}

