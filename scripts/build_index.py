#!/usr/bin/env python3
import os
import sqlite3
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
        "CREATE TABLE articles (id TEXT PRIMARY KEY, title TEXT, path TEXT);"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE article_index USING fts5(content, title, id UNINDEXED);"
    )


def read_markdown(md_path: Path) -> tuple[str, str]:
    # naive read: title as first heading or filename; content as raw text
    text = md_path.read_text(encoding="utf-8", errors="ignore")
    title = md_path.stem
    for line in text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break
    return title, text


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
            title, content = read_markdown(md)
            # Use a synthetic id for now: path relative to compendium
            rel = md.relative_to(compendium_dir).as_posix()
            article_id = rel  # KB can later replace with ULID from front matter
            conn.execute(
                "INSERT INTO articles (id, title, path) VALUES (?, ?, ?)",
                (article_id, title, rel),
            )
            conn.execute(
                "INSERT INTO article_index (content, title, id) VALUES (?, ?, ?)",
                (content, title, article_id),
            )
            count += 1
        conn.commit()
        print(f"Built index with {count} articles at {DB_PATH}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


