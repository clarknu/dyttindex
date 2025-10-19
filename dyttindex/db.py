from __future__ import annotations

import os
import sqlite3
import datetime as dt
from typing import Iterable, List, Optional, Dict, Any

from .config import SQLITE_PATH


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    _ensure_dir(SQLITE_PATH)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_db(drop: bool = False) -> None:
    if drop and os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            original_title TEXT,
            year INTEGER,
            kind TEXT,
            country TEXT,
            language TEXT,
            director TEXT,
            actors TEXT,
            rating_source TEXT,
            rating_value REAL,
            rating_votes INTEGER,
            tags_text TEXT,
            description TEXT,
            cover_url TEXT,
            detail_url TEXT NOT NULL UNIQUE,
            raw_html TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS movie_tags (
            movie_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (movie_id, tag_id),
            FOREIGN KEY(movie_id) REFERENCES movies(id) ON DELETE CASCADE,
            FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS download_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            kind TEXT,
            label TEXT,
            UNIQUE(movie_id, url),
            FOREIGN KEY(movie_id) REFERENCES movies(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()
    conn.close()


def _ensure_tags(conn: sqlite3.Connection, tag_names: Iterable[str]) -> List[int]:
    ids: List[int] = []
    cur = conn.cursor()
    for name in {t.strip() for t in tag_names if t and t.strip()}:
        cur.execute("INSERT OR IGNORE INTO tags(name) VALUES(?)", (name,))
        cur.execute("SELECT id FROM tags WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            ids.append(row[0])
    return ids


def upsert_movie(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    assert data.get("detail_url"), "detail_url is required"
    # UPSERT 基本信息
    cur = conn.cursor()
    tags_text = ",".join([t for t in (data.get("tags") or [])]) if data.get("tags") else None
    cur.execute(
        """
        INSERT INTO movies(
            title, original_title, year, kind, country, language, director, actors,
            rating_source, rating_value, rating_votes, tags_text, description,
            cover_url, detail_url, raw_html, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
        ON CONFLICT(detail_url) DO UPDATE SET
            title=excluded.title,
            original_title=excluded.original_title,
            year=excluded.year,
            kind=excluded.kind,
            country=excluded.country,
            language=excluded.language,
            director=excluded.director,
            actors=excluded.actors,
            rating_source=excluded.rating_source,
            rating_value=excluded.rating_value,
            rating_votes=excluded.rating_votes,
            tags_text=excluded.tags_text,
            description=excluded.description,
            cover_url=excluded.cover_url,
            raw_html=excluded.raw_html,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            data.get("title") or "",
            data.get("original_title"),
            data.get("year"),
            data.get("kind"),
            data.get("country"),
            data.get("language"),
            data.get("director"),
            data.get("actors"),
            data.get("rating_source"),
            data.get("rating_value"),
            data.get("rating_votes"),
            tags_text,
            data.get("description"),
            data.get("cover_url"),
            data.get("detail_url"),
            data.get("raw_html"),
        ),
    )
    # 获取 movie_id
    cur.execute("SELECT id FROM movies WHERE detail_url=?", (data.get("detail_url"),))
    row = cur.fetchone()
    movie_id = int(row[0])

    # 标签关联
    tag_ids = _ensure_tags(conn, data.get("tags") or [])
    for tid in tag_ids:
        cur.execute(
            "INSERT OR IGNORE INTO movie_tags(movie_id, tag_id) VALUES(?,?)",
            (movie_id, tid),
        )

    # 下载链接
    for dl in (data.get("download_links") or []):
        url = dl.get("url")
        if not url:
            continue
        cur.execute(
            "INSERT OR IGNORE INTO download_links(movie_id, url, kind, label) VALUES(?,?,?,?)",
            (movie_id, url, dl.get("kind"), dl.get("label")),
        )

    conn.commit()
    return movie_id


def get_movie(conn: sqlite3.Connection, movie_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM movies WHERE id=?", (movie_id,))
    return cur.fetchone()


def get_download_links(conn: sqlite3.Connection, movie_id: int) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT kind, url, label FROM download_links WHERE movie_id=?", (movie_id,))
    return cur.fetchall()


def search_movies(
    conn: sqlite3.Connection,
    title: Optional[str] = None,
    kind: Optional[str] = None,
    country: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    rating_min: Optional[float] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    limit: int = 50,
) -> List[sqlite3.Row]:
    sql = "SELECT id, title, kind, year, country, rating_source, rating_value, tags_text, detail_url FROM movies WHERE 1=1"
    params: List[Any] = []
    if title:
        sql += " AND title LIKE ?"
        params.append(f"%{title}%")
    if kind:
        sql += " AND kind = ?"
        params.append(kind)
    if country:
        sql += " AND country LIKE ?"
        params.append(f"%{country}%")
    if rating_min is not None:
        sql += " AND rating_value >= ?"
        params.append(rating_min)
    if year_from is not None:
        sql += " AND year >= ?"
        params.append(year_from)
    if year_to is not None:
        sql += " AND year <= ?"
        params.append(year_to)
    if tags:
        for t in tags:
            sql += " AND (tags_text LIKE ?)"
            params.append(f"%{t}%")
    sql += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()