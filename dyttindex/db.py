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


# 新增：简单迁移，确保下载链接表有 episode 列
def _migrate_download_links_episode(cur: sqlite3.Cursor) -> None:
    cur.execute("PRAGMA table_info(download_links)")
    cols = [row[1] for row in cur.fetchall()]
    if "episode" not in cols:
        cur.execute("ALTER TABLE download_links ADD COLUMN episode INTEGER")


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
        -- 断点续爬相关表
        CREATE TABLE IF NOT EXISTS crawl_sessions (
            id TEXT PRIMARY KEY,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS crawl_visits (
            session_id TEXT NOT NULL,
            url TEXT NOT NULL,
            kind TEXT NOT NULL,
            visited_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(session_id, url, kind),
            FOREIGN KEY(session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS crawl_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event TEXT,
            section TEXT,
            url TEXT,
            detail_url TEXT,
            message TEXT,
            count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS crawl_queue (
            session_id TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            enqueued_at TEXT DEFAULT CURRENT_TIMESTAMP,
            dequeued_at TEXT,
            PRIMARY KEY(session_id, url),
            FOREIGN KEY(session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
        );
        """
    )
    # 迁移：为已存在的 download_links 增加 episode 列
    _migrate_download_links_episode(cur)
    # 迁移：确保 movies 表存在 alt_titles_text 列
    cur.execute("PRAGMA table_info(movies)")
    cols = [row[1] for row in cur.fetchall()]
    if "alt_titles_text" not in cols:
        cur.execute("ALTER TABLE movies ADD COLUMN alt_titles_text TEXT")
    conn.commit()
    conn.close()

# 会话与访问记录 API

def ensure_session(conn: sqlite3.Connection, session_id: Optional[str]) -> Optional[str]:
    """确保会话存在，返回会话ID；若未提供则返回 None。"""
    if not session_id:
        return None
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO crawl_sessions(id) VALUES(?)", (session_id,))
    cur.execute("UPDATE crawl_sessions SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (session_id,))
    conn.commit()
    return session_id


def get_visited(conn: sqlite3.Connection, session_id: Optional[str], kind: str) -> set:
    if not session_id:
        return set()
    cur = conn.cursor()
    cur.execute("SELECT url FROM crawl_visits WHERE session_id=? AND kind=?", (session_id, kind))
    return {row[0] for row in cur.fetchall()}


def mark_visited(conn: sqlite3.Connection, session_id: Optional[str], url: str, kind: str) -> None:
    if not session_id:
        return
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO crawl_visits(session_id, url, kind) VALUES(?,?,?)",
        (session_id, url, kind),
    )
    cur.execute("UPDATE crawl_sessions SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (session_id,))
    conn.commit()

# 持久化前沿队列（断点续跑）
from typing import List

def enqueue_urls(conn: sqlite3.Connection, session_id: Optional[str], urls: List[str]) -> None:
    if not session_id or not urls:
        return
    cur = conn.cursor()
    for u in urls:
        cur.execute(
            "INSERT OR IGNORE INTO crawl_queue(session_id, url, status) VALUES(?, ?, 'queued')",
            (session_id, u),
        )
    cur.execute("UPDATE crawl_sessions SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (session_id,))
    conn.commit()

def get_frontier_urls(conn: sqlite3.Connection, session_id: Optional[str], limit: int = 1000) -> List[str]:
    if not session_id:
        return []
    cur = conn.cursor()
    cur.execute(
        "SELECT url FROM crawl_queue WHERE session_id=? AND status='queued' ORDER BY enqueued_at ASC LIMIT ?",
        (session_id, limit),
    )
    return [row[0] for row in cur.fetchall()]

def mark_queue_done(conn: sqlite3.Connection, session_id: Optional[str], url: str) -> None:
    if not session_id:
        return
    cur = conn.cursor()
    cur.execute(
        "UPDATE crawl_queue SET status='done', dequeued_at=CURRENT_TIMESTAMP WHERE session_id=? AND url=?",
        (session_id, url),
    )
    cur.execute("UPDATE crawl_sessions SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (session_id,))
    conn.commit()


def append_event(conn: sqlite3.Connection, session_id: Optional[str], event: dict) -> None:
    if not session_id:
        return
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO crawl_events(session_id, event, section, url, detail_url, message, count)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            session_id,
            event.get("event"),
            event.get("section") or event.get("category"),
            event.get("url"),
            event.get("detail_url"),
            event.get("message"),
            event.get("count"),
        ),
    )
    cur.execute("UPDATE crawl_sessions SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (session_id,))
    conn.commit()


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
            cover_url, detail_url, raw_html, alt_titles_text, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
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
            alt_titles_text=excluded.alt_titles_text,
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
            ",".join(data.get("alt_titles") or []) if data.get("alt_titles") else data.get("alt_titles_text"),
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

    # 下载链接（增加 episode 字段）
    for dl in (data.get("download_links") or []):
        url = dl.get("url")
        if not url:
            continue
        cur.execute(
            "INSERT OR IGNORE INTO download_links(movie_id, url, kind, label, episode) VALUES(?,?,?,?,?)",
            (movie_id, url, dl.get("kind"), dl.get("label"), dl.get("episode")),
        )

    conn.commit()
    return movie_id


def get_movie(conn: sqlite3.Connection, movie_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM movies WHERE id=?", (movie_id,))
    return cur.fetchone()


def get_download_links(conn: sqlite3.Connection, movie_id: int) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT kind, url, label, episode FROM download_links WHERE movie_id=?", (movie_id,))
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
    language: Optional[str] = None,
    director: Optional[str] = None,
    actors_substr: Optional[str] = None,
    rating_source: Optional[str] = None,
    limit: int = 50,
    keyword: Optional[str] = None,
    offset: int = 0,
    order_by: Optional[str] = None,
    order_dir: str = "desc",
) -> List[sqlite3.Row]:
    sql = "SELECT id, title, kind, year, country, director, actors, rating_source, rating_value, tags_text, detail_url FROM movies WHERE 1=1"
    params: List[Any] = []
    if title:
        sql += " AND title LIKE ?"
        params.append(f"%{title}%")
    if keyword:
        sql += " AND (title LIKE ? OR original_title LIKE ? OR alt_titles_text LIKE ? OR description LIKE ? OR actors LIKE ? OR country LIKE ?)"
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw, kw, kw])
    if kind:
        if kind == "movie":
            sql += " AND kind LIKE ?"
            params.append("movie%")
        else:
            sql += " AND kind = ?"
            params.append(kind)
    if country:
        sql += " AND country LIKE ?"
        params.append(f"%{country}%")
    if language:
        sql += " AND language LIKE ?"
        params.append(f"%{language}%")
    if director:
        sql += " AND director LIKE ?"
        params.append(f"%{director}%")
    if actors_substr:
        sql += " AND actors LIKE ?"
        params.append(f"%{actors_substr}%")
    if rating_source:
        sql += " AND rating_source = ?"
        params.append(rating_source)
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
    # 排序与分页
    allowed_order = {
        "updated_at": "updated_at",
        "created_at": "created_at",
        "year": "year",
        "rating": "rating_value",
        "title": "title",
        "id": "id",
    }
    ob = allowed_order.get((order_by or "updated_at").lower(), "updated_at")
    dir_sql = "ASC" if (order_dir or "").lower() == "asc" else "DESC"
    sql += f" ORDER BY {ob} {dir_sql} LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

def count_movies(
    conn: sqlite3.Connection,
    title: Optional[str] = None,
    kind: Optional[str] = None,
    country: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    rating_min: Optional[float] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    language: Optional[str] = None,
    director: Optional[str] = None,
    actors_substr: Optional[str] = None,
    rating_source: Optional[str] = None,
    keyword: Optional[str] = None,
) -> int:
    sql = "SELECT COUNT(*) FROM movies WHERE 1=1"
    params: List[Any] = []
    if title:
        sql += " AND title LIKE ?"
        params.append(f"%{title}%")
    if keyword:
        sql += " AND (title LIKE ? OR original_title LIKE ? OR alt_titles_text LIKE ? OR description LIKE ? OR actors LIKE ? OR country LIKE ?)"
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw, kw, kw])
    if kind:
        if kind == "movie":
            sql += " AND kind LIKE ?"
            params.append("movie%")
        else:
            sql += " AND kind = ?"
            params.append(kind)
    if country:
        sql += " AND country LIKE ?"
        params.append(f"%{country}%")
    if language:
        sql += " AND language LIKE ?"
        params.append(f"%{language}%")
    if director:
        sql += " AND director LIKE ?"
        params.append(f"%{director}%")
    if actors_substr:
        sql += " AND actors LIKE ?"
        params.append(f"%{actors_substr}%")
    if rating_source:
        sql += " AND rating_source = ?"
        params.append(rating_source)
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
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] or 0)