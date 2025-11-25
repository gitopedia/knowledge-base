package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
)

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
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Default paths
	// knowledge-base/cmd/indexer -> knowledge-base
	kbRoot, err := os.Getwd() 
	if err != nil {
		return err
	}
	
	// assume run from knowledge-base root
	outDir := filepath.Join(kbRoot, "out")
	dbPath := filepath.Join(outDir, "index.sqlite")
	
	// Default compendium path: ../gitopedia/Compendium
	compendiumDir := os.Getenv("GITOPEDIA_DIR")
	if compendiumDir == "" {
		compendiumDir = filepath.Join(kbRoot, "../gitopedia/Compendium")
	}
	
	if _, err := os.Stat(compendiumDir); os.IsNotExist(err) {
		return fmt.Errorf("compendium dir not found: %s", compendiumDir)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	// Remove old DB
	os.Remove(dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		return err
	}

	version := os.Getenv("GITOPEDIA_VERSION")
	if version == "" {
		version = "unknown"
	}
	if _, err := db.Exec("INSERT OR REPLACE INTO db_info (key, value) VALUES ('version', ?)", version); err != nil {
		log.Printf("Failed to insert version info: %v", err)
	}

	count := 0
	err = filepath.WalkDir(compendiumDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".md") {
			return nil
		}
		if strings.ToLower(d.Name()) == "index.md" {
			return nil
		}

		if err := processFile(db, compendiumDir, path); err != nil {
			log.Printf("Error processing %s: %v", path, err)
		} else {
			count++
		}
		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("Built index with %d articles at %s", count, dbPath)
	return nil
}

func initDB(db *sql.DB) error {
	cmds := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=OFF;",
		"CREATE TABLE articles (id TEXT PRIMARY KEY, title TEXT, path TEXT, author TEXT, summary TEXT, tags TEXT, meta_json TEXT);",
		"CREATE TABLE db_info (key TEXT PRIMARY KEY, value TEXT);",
		"CREATE VIRTUAL TABLE article_index USING fts5(content, title, summary, tags, id UNINDEXED);",
	}
	for _, cmd := range cmds {
		if _, err := db.Exec(cmd); err != nil {
			return err
		}
	}
	return nil
}

func processFile(db *sql.DB, root, path string) error {
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

	tagsJSON, _ := json.Marshal(fm.Tags)
	// Reconstruct full meta map for storage
	meta := make(map[string]interface{})
	meta["id"] = fm.ID
	meta["title"] = fm.Title
	meta["author"] = fm.Author
	meta["summary"] = fm.Summary
	meta["tags"] = fm.Tags
	for k, v := range fm.Rest {
		meta[k] = v
	}
	metaJSON, _ := json.Marshal(meta)

	tagsStr := strings.Join(fm.Tags, " ")

	_, err = db.Exec("INSERT INTO articles (id, title, path, author, summary, tags, meta_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
		id, fm.Title, relPath, fm.Author, fm.Summary, string(tagsJSON), string(metaJSON))
	if err != nil {
		return fmt.Errorf("insert article failed: %w", err)
	}

	_, err = db.Exec("INSERT INTO article_index (content, title, summary, tags, id) VALUES (?, ?, ?, ?, ?)",
		body, fm.Title, fm.Summary, tagsStr, id)
	if err != nil {
		return fmt.Errorf("insert index failed: %w", err)
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
				// log warning?
			}
			body = parts[2]
		}
	}
	return fm, body, nil
}

