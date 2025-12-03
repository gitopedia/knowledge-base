// Package database provides SQLite database operations for the knowledge-base.
package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database connection
type DB struct {
	conn *sql.DB
	path string
}

// Source represents a source document in the database
type Source struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Title     string   `json:"title"`
	Topic     string   `json:"topic"`
	Summary   string   `json:"summary"`
	Language  string   `json:"language,omitempty"`
	Model     string   `json:"model,omitempty"`
	CreatedAt string   `json:"created_at"`
	Tags      []string `json:"tags,omitempty"`
}

// Article represents an article in the database
type Article struct {
	ID       string                 `json:"id"`
	Title    string                 `json:"title"`
	Path     string                 `json:"path"`
	Author   string                 `json:"author,omitempty"`
	Summary  string                 `json:"summary"`
	Tags     []string               `json:"tags"`
	Meta     map[string]interface{} `json:"meta,omitempty"`
	Content  string                 `json:"content,omitempty"` // Full body text for FTS
}

// Open opens or creates a SQLite database at the given path
func Open(path string) (*DB, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db := &DB{conn: conn, path: path}
	if err := db.init(); err != nil {
		conn.Close()
		return nil, err
	}

	return db, nil
}

// init creates the database schema if it doesn't exist
func (db *DB) init() error {
	cmds := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",

		// Articles table (existing)
		`CREATE TABLE IF NOT EXISTS articles (
			id TEXT PRIMARY KEY,
			title TEXT,
			path TEXT UNIQUE,
			author TEXT,
			summary TEXT,
			tags TEXT,
			meta_json TEXT
		);`,

		// Articles FTS index (existing)
		`CREATE VIRTUAL TABLE IF NOT EXISTS article_fts USING fts5(
			content,
			title,
			summary,
			tags,
			id UNINDEXED
		);`,

		// Sources table (new)
		`CREATE TABLE IF NOT EXISTS sources (
			id TEXT PRIMARY KEY,
			url TEXT UNIQUE,
			title TEXT,
			topic TEXT,
			summary TEXT,
			language TEXT,
			model TEXT,
			created_at TEXT,
			tags TEXT
		);`,

		// Sources FTS index (new)
		`CREATE VIRTUAL TABLE IF NOT EXISTS source_fts USING fts5(
			summary,
			title,
			topic,
			id UNINDEXED
		);`,

		// Indexes
		`CREATE INDEX IF NOT EXISTS idx_sources_topic ON sources(topic);`,
		`CREATE INDEX IF NOT EXISTS idx_sources_url ON sources(url);`,
		`CREATE INDEX IF NOT EXISTS idx_articles_path ON articles(path);`,

		// Metadata table
		`CREATE TABLE IF NOT EXISTS db_info (
			key TEXT PRIMARY KEY,
			value TEXT
		);`,
	}

	for _, cmd := range cmds {
		if _, err := db.conn.Exec(cmd); err != nil {
			return fmt.Errorf("failed to execute '%s': %w", cmd[:min(50, len(cmd))], err)
		}
	}

	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}

// InsertSource inserts a new source into the database
func (db *DB) InsertSource(src Source) error {
	tagsJSON, _ := json.Marshal(src.Tags)

	_, err := db.conn.Exec(`
		INSERT OR REPLACE INTO sources (id, url, title, topic, summary, language, model, created_at, tags)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, src.ID, src.URL, src.Title, src.Topic, src.Summary, src.Language, src.Model, src.CreatedAt, string(tagsJSON))
	if err != nil {
		return fmt.Errorf("failed to insert source: %w", err)
	}

	// Update FTS index
	_, err = db.conn.Exec(`
		INSERT OR REPLACE INTO source_fts (id, summary, title, topic)
		VALUES (?, ?, ?, ?)
	`, src.ID, src.Summary, src.Title, src.Topic)
	if err != nil {
		return fmt.Errorf("failed to update source FTS: %w", err)
	}

	return nil
}

// GetSource retrieves a source by ID
func (db *DB) GetSource(id string) (*Source, error) {
	var src Source
	var tagsJSON string

	err := db.conn.QueryRow(`
		SELECT id, url, title, topic, summary, language, model, created_at, tags
		FROM sources WHERE id = ?
	`, id).Scan(&src.ID, &src.URL, &src.Title, &src.Topic, &src.Summary,
		&src.Language, &src.Model, &src.CreatedAt, &tagsJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if tagsJSON != "" {
		json.Unmarshal([]byte(tagsJSON), &src.Tags)
	}

	return &src, nil
}

// GetSourceByURL retrieves a source by URL
func (db *DB) GetSourceByURL(url string) (*Source, error) {
	var src Source
	var tagsJSON string

	err := db.conn.QueryRow(`
		SELECT id, url, title, topic, summary, language, model, created_at, tags
		FROM sources WHERE url = ?
	`, url).Scan(&src.ID, &src.URL, &src.Title, &src.Topic, &src.Summary,
		&src.Language, &src.Model, &src.CreatedAt, &tagsJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if tagsJSON != "" {
		json.Unmarshal([]byte(tagsJSON), &src.Tags)
	}

	return &src, nil
}

// GetSourcesByTopic retrieves all sources for a given topic
func (db *DB) GetSourcesByTopic(topic string, limit int) ([]Source, error) {
	rows, err := db.conn.Query(`
		SELECT id, url, title, topic, summary, language, model, created_at, tags
		FROM sources WHERE topic = ? LIMIT ?
	`, topic, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []Source
	for rows.Next() {
		var src Source
		var tagsJSON string
		if err := rows.Scan(&src.ID, &src.URL, &src.Title, &src.Topic, &src.Summary,
			&src.Language, &src.Model, &src.CreatedAt, &tagsJSON); err != nil {
			return nil, err
		}
		if tagsJSON != "" {
			json.Unmarshal([]byte(tagsJSON), &src.Tags)
		}
		sources = append(sources, src)
	}

	return sources, rows.Err()
}

// SearchSources performs a full-text search on sources
func (db *DB) SearchSources(query string, limit int) ([]Source, error) {
	rows, err := db.conn.Query(`
		SELECT s.id, s.url, s.title, s.topic, s.summary, s.language, s.model, s.created_at, s.tags
		FROM sources s
		JOIN source_fts f ON s.id = f.id
		WHERE source_fts MATCH ?
		ORDER BY rank
		LIMIT ?
	`, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []Source
	for rows.Next() {
		var src Source
		var tagsJSON string
		if err := rows.Scan(&src.ID, &src.URL, &src.Title, &src.Topic, &src.Summary,
			&src.Language, &src.Model, &src.CreatedAt, &tagsJSON); err != nil {
			return nil, err
		}
		if tagsJSON != "" {
			json.Unmarshal([]byte(tagsJSON), &src.Tags)
		}
		sources = append(sources, src)
	}

	return sources, rows.Err()
}

// DeleteSource removes a source from the database
func (db *DB) DeleteSource(id string) error {
	_, err := db.conn.Exec("DELETE FROM sources WHERE id = ?", id)
	if err != nil {
		return err
	}
	_, err = db.conn.Exec("DELETE FROM source_fts WHERE id = ?", id)
	return err
}

// CountSources returns the total number of sources
func (db *DB) CountSources() (int, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM sources").Scan(&count)
	return count, err
}

// InsertArticle inserts or updates an article
func (db *DB) InsertArticle(art Article) error {
	tagsJSON, _ := json.Marshal(art.Tags)
	metaJSON, _ := json.Marshal(art.Meta)

	_, err := db.conn.Exec(`
		INSERT OR REPLACE INTO articles (id, title, path, author, summary, tags, meta_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, art.ID, art.Title, art.Path, art.Author, art.Summary, string(tagsJSON), string(metaJSON))
	if err != nil {
		return fmt.Errorf("failed to insert article: %w", err)
	}

	// Update FTS index
	tagsStr := ""
	for i, tag := range art.Tags {
		if i > 0 {
			tagsStr += " "
		}
		tagsStr += tag
	}

	_, err = db.conn.Exec(`
		INSERT OR REPLACE INTO article_fts (id, content, title, summary, tags)
		VALUES (?, ?, ?, ?, ?)
	`, art.ID, art.Content, art.Title, art.Summary, tagsStr)
	if err != nil {
		return fmt.Errorf("failed to update article FTS: %w", err)
	}

	return nil
}

// GetArticle retrieves an article by ID
func (db *DB) GetArticle(id string) (*Article, error) {
	var art Article
	var tagsJSON, metaJSON string

	err := db.conn.QueryRow(`
		SELECT id, title, path, author, summary, tags, meta_json
		FROM articles WHERE id = ?
	`, id).Scan(&art.ID, &art.Title, &art.Path, &art.Author, &art.Summary, &tagsJSON, &metaJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if tagsJSON != "" {
		json.Unmarshal([]byte(tagsJSON), &art.Tags)
	}
	if metaJSON != "" {
		json.Unmarshal([]byte(metaJSON), &art.Meta)
	}

	return &art, nil
}

// SearchArticles performs a full-text search on articles
func (db *DB) SearchArticles(query string, limit int) ([]Article, error) {
	rows, err := db.conn.Query(`
		SELECT a.id, a.title, a.path, a.author, a.summary, a.tags, a.meta_json
		FROM articles a
		JOIN article_fts f ON a.id = f.id
		WHERE article_fts MATCH ?
		ORDER BY rank
		LIMIT ?
	`, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var articles []Article
	for rows.Next() {
		var art Article
		var tagsJSON, metaJSON string
		if err := rows.Scan(&art.ID, &art.Title, &art.Path, &art.Author, &art.Summary, &tagsJSON, &metaJSON); err != nil {
			return nil, err
		}
		if tagsJSON != "" {
			json.Unmarshal([]byte(tagsJSON), &art.Tags)
		}
		if metaJSON != "" {
			json.Unmarshal([]byte(metaJSON), &art.Meta)
		}
		articles = append(articles, art)
	}

	return articles, rows.Err()
}

// CountArticles returns the total number of articles
func (db *DB) CountArticles() (int, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM articles").Scan(&count)
	return count, err
}

// SetInfo stores a key-value pair in the db_info table
func (db *DB) SetInfo(key, value string) error {
	_, err := db.conn.Exec("INSERT OR REPLACE INTO db_info (key, value) VALUES (?, ?)", key, value)
	return err
}

// GetInfo retrieves a value from the db_info table
func (db *DB) GetInfo(key string) (string, error) {
	var value string
	err := db.conn.QueryRow("SELECT value FROM db_info WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

