// Package main provides the article indexer for the knowledge-base.
// It walks the Compendium directory, parses markdown files with frontmatter,
// and builds a searchable SQLite index.
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

	"github.com/gitopedia/knowledge-base/internal/database"
	"github.com/gitopedia/knowledge-base/internal/embedding"
	"github.com/gitopedia/knowledge-base/internal/vectordb"
	"gopkg.in/yaml.v3"
)

// FrontMatter represents the YAML front matter of an article
type FrontMatter struct {
	ID      string   `yaml:"id"`
	Title   string   `yaml:"title"`
	Author  string   `yaml:"author"`
	Summary string   `yaml:"summary"`
	Tags    []string `yaml:"tags"`
	// Capture other fields for meta_json
	Rest map[string]interface{} `yaml:",inline"`
}

func main() {
	// Flags
	dbPath := flag.String("db", "", "Path to SQLite database")
	compendiumDir := flag.String("compendium", "", "Path to Compendium directory")
	withEmbeddings := flag.Bool("embeddings", false, "Generate embeddings and store in Qdrant")
	flag.Parse()

	if err := run(*dbPath, *compendiumDir, *withEmbeddings); err != nil {
		log.Fatal(err)
	}
}

func run(dbPath, compendiumDir string, withEmbeddings bool) error {
	// Determine paths
	kbRoot, err := os.Getwd()
	if err != nil {
		return err
	}

	if dbPath == "" {
		dbPath = os.Getenv("KB_DB_PATH")
		if dbPath == "" {
			dbPath = filepath.Join(kbRoot, "out", "knowledge.sqlite")
		}
	}

	if compendiumDir == "" {
		compendiumDir = os.Getenv("GITOPEDIA_DIR")
		if compendiumDir == "" {
			compendiumDir = filepath.Join(kbRoot, "../gitopedia/Compendium")
		}
	}

	log.Printf("Database path: %s", dbPath)
	log.Printf("Compendium directory: %s", compendiumDir)
	log.Printf("Generate embeddings: %v", withEmbeddings)

	if _, err := os.Stat(compendiumDir); os.IsNotExist(err) {
		return fmt.Errorf("compendium dir not found: %s", compendiumDir)
	}

	// Open database (creates if not exists)
	db, err := database.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Set version
	version := os.Getenv("GITOPEDIA_VERSION")
	if version == "" {
		version = "unknown"
	}
	if err := db.SetInfo("version", version); err != nil {
		log.Printf("Warning: failed to set version info: %v", err)
	}

	// Initialize embedding and vector clients if needed
	var embedder *embedding.Client
	var vectorDB *vectordb.Client
	if withEmbeddings {
		embedder = embedding.NewClient()
		log.Printf("Embedding model: %s", embedder.Model())

		vectorDB, err = vectordb.NewClient()
		if err != nil {
			return fmt.Errorf("failed to connect to Qdrant: %w", err)
		}
		defer vectorDB.Close()

		ctx := context.Background()
		if err := vectorDB.EnsureCollections(ctx); err != nil {
			return fmt.Errorf("failed to ensure Qdrant collections: %w", err)
		}
	}

	// Walk and index articles
	var count, skipped, errors int
	err = filepath.WalkDir(compendiumDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip _incoming and _debug directories
			if d.Name() == "_incoming" || d.Name() == "_debug" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".md") {
			return nil
		}
		if strings.ToLower(d.Name()) == "index.md" {
			return nil
		}

		if err := processArticle(db, embedder, vectorDB, compendiumDir, path, withEmbeddings); err != nil {
			log.Printf("Error processing %s: %v", filepath.Base(path), err)
			errors++
		} else {
			count++
		}
		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("Indexing complete: %d articles indexed, %d skipped, %d errors", count, skipped, errors)

	// Log stats
	articleCount, _ := db.CountArticles()
	sourceCount, _ := db.CountSources()
	log.Printf("Database stats: %d articles, %d sources", articleCount, sourceCount)

	return nil
}

func processArticle(db *database.DB, embedder *embedding.Client, vectorDB *vectordb.Client, root, path string, withEmbeddings bool) error {
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	fm, body, err := parse(contentBytes)
	if err != nil {
		return err
	}

	// Defaults
	if fm.Title == "" {
		// Fallback to H1
		lines := strings.Split(body, "\n")
		for _, l := range lines {
			if strings.HasPrefix(l, "# ") {
				fm.Title = strings.TrimSpace(l[2:])
				break
			}
		}
		if fm.Title == "" {
			fm.Title = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		}
	}

	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	// Normalize path separators to slash
	relPath = filepath.ToSlash(relPath)

	id := fm.ID
	if id == "" {
		id = relPath
	}

	// Extract category from path
	category := ""
	parts := strings.Split(relPath, "/")
	if len(parts) > 1 {
		category = strings.Join(parts[:len(parts)-1], "/")
	}

	// Build meta map
	meta := make(map[string]interface{})
	meta["id"] = fm.ID
	meta["title"] = fm.Title
	meta["author"] = fm.Author
	meta["summary"] = fm.Summary
	meta["tags"] = fm.Tags
	meta["category"] = category
	for k, v := range fm.Rest {
		meta[k] = v
	}

	// Insert into database
	article := database.Article{
		ID:      id,
		Title:   fm.Title,
		Path:    relPath,
		Author:  fm.Author,
		Summary: fm.Summary,
		Tags:    fm.Tags,
		Meta:    meta,
		Content: body,
	}

	if err := db.InsertArticle(article); err != nil {
		return fmt.Errorf("insert article failed: %w", err)
	}

	// Generate and store embedding if enabled
	if withEmbeddings && embedder != nil && vectorDB != nil {
		// Create text for embedding (title + summary + first part of content)
		embeddingText := fm.Title
		if fm.Summary != "" {
			embeddingText += " " + fm.Summary
		}
		if len(body) > 0 {
			// Add first 1000 chars of body
			bodyPreview := body
			if len(bodyPreview) > 1000 {
				bodyPreview = bodyPreview[:1000]
			}
			embeddingText += " " + bodyPreview
		}

		ctx := context.Background()
		emb, err := embedder.Embed(ctx, embeddingText)
		if err != nil {
			log.Printf("Warning: failed to generate embedding for %s: %v", id, err)
		} else {
			payload := vectordb.ArticlePayload{
				ID:       id,
				Title:    fm.Title,
				Path:     relPath,
				Summary:  fm.Summary,
				Tags:     fm.Tags,
				Category: category,
			}
			if err := vectorDB.UpsertArticle(ctx, id, emb, payload); err != nil {
				log.Printf("Warning: failed to store embedding for %s: %v", id, err)
			}
		}
	}

	return nil
}

func parse(content []byte) (FrontMatter, string, error) {
	s := string(content)
	var fm FrontMatter
	body := s

	if strings.HasPrefix(s, "---") {
		parts := strings.SplitN(s, "---", 3)
		if len(parts) >= 3 {
			if err := yaml.Unmarshal([]byte(parts[1]), &fm); err != nil {
				// Continue with empty frontmatter
				log.Printf("Warning: failed to parse frontmatter: %v", err)
			}
			body = strings.TrimSpace(parts[2])
		}
	}
	return fm, body, nil
}
