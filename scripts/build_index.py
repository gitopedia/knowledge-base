#!/usr/bin/env python3
import os
import sqlite3
import re
import json
import yaml
from pathlib import Path

DEFAULT_GITOPEDIA_DIR = Path(__file__).resolve().parents[2] / "gitopedia" / "gitopedia" / "Compendium"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "out"
DB_PATH = OUTPUT_DIR / "index.sqlite"


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_compendium_dir() -> Path:
    override = os.environ.get("GITOPEDIA_DIR")
    if override:
        return Path(override)
    return DEFAULT_GITOPEDIA_DIR


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("DROP TABLE IF EXISTS articles;")
    conn.execute("DROP TABLE IF EXISTS article_index;")
    conn.execute(
        """
        CREATE TABLE articles (
            id TEXT PRIMARY KEY,
            title TEXT,
            path TEXT,
            author TEXT,
            summary TEXT,
            tags TEXT,
            meta_json TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE VIRTUAL TABLE article_index USING fts5(
            content,
            title,
            summary,
            tags,
            id UNINDEXED
        );
        """
    )


def parse_frontmatter(text: str) -> tuple[dict, str]:
    """
    Parses YAML front matter from the text.
    Returns a tuple of (metadata_dict, content_without_frontmatter).
    """
    # Regex to match YAML front matter enclosed in ---
    pattern = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
    match = pattern.match(text)
    
    if match:
        yaml_content = match.group(1)
        try:
            meta = yaml.safe_load(yaml_content)
            if not isinstance(meta, dict):
                meta = {}
            content = text[match.end():]
            return meta, content
        except yaml.YAMLError:
            pass
    
    return {}, text


def read_markdown(md_path: Path) -> dict:
    """
    Reads markdown file and returns a dictionary with parsed data.
    """
    text = md_path.read_text(encoding="utf-8", errors="ignore")
    meta, content = parse_frontmatter(text)
    
    # Defaults
    title = meta.get("title")
    if not title:
        title = md_path.stem
        # Try to find H1 title if missing in front matter
        for line in content.splitlines():
            if line.startswith("# "):
                title = line[2:].strip()
                break
    
    # ID preference: Front matter ID > Filename/Path (synthetic)
    # For now, we'll stick to synthetic ID if missing, but roadmap wants ULID from front matter.
    # We'll return what we found.
    
    return {
        "id": meta.get("id"),
        "title": title,
        "author": meta.get("author", ""),
        "summary": meta.get("summary", ""),
        "tags": meta.get("tags", []),
        "content": content,
        "meta": meta # raw meta for json storage
    }


def walk_articles(compendium_dir: Path):
    for md in compendium_dir.rglob("*.md"):
        if md.name.lower() == "index.md":
            continue
        yield md


def main() -> int:
    ensure_dirs()
    compendium_dir = get_compendium_dir()
    if not compendium_dir.exists():
        print(f"Compendium dir not found: {compendium_dir}")
        return 1
    
    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        count = 0
        for md in walk_articles(compendium_dir):
            data = read_markdown(md)
            
            # Determine ID: use front matter ID if present, else relative path
            rel_path = md.relative_to(compendium_dir).as_posix()
            article_id = str(data.get("id") or rel_path)
            
            tags_json = json.dumps(data["tags"]) if data["tags"] else "[]"
            meta_json = json.dumps(data["meta"])
            
            conn.execute(
                """
                INSERT INTO articles (id, title, path, author, summary, tags, meta_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article_id,
                    data["title"],
                    rel_path,
                    data["author"],
                    data["summary"],
                    tags_json,
                    meta_json
                ),
            )
            
            # Index content, title, summary, tags
            # For FTS, we concatenate tags into a space-separated string or similar
            tags_str = " ".join(data["tags"]) if isinstance(data["tags"], list) else str(data["tags"])
            
            conn.execute(
                """
                INSERT INTO article_index (content, title, summary, tags, id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    data["content"],
                    data["title"],
                    data["summary"],
                    tags_str,
                    article_id
                ),
            )
            count += 1
        conn.commit()
        print(f"Built index with {count} articles at {DB_PATH}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
