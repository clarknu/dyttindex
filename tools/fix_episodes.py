import sqlite3
import re

SQLITE_PATH = r"c:\\Code\\dyttindex\\data\\movies.db"

# Patterns similar to scraper._parse_episode
PATTERNS = [
    re.compile(r"第\s*(\d{1,3})\s*[集话]"),
    re.compile(r"[Ee][Pp]?\s*(\d{1,3})"),
    re.compile(r"[Ss]\d{1,2}[Ee](\d{1,2})"),
]

def infer_episode(label: str, href: str):
    text = (label or "") + " " + (href or "")
    for pat in PATTERNS:
        m = pat.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    m2 = re.search(r"[-_\s](\d{1,3})(?!\d)", text)
    if m2:
        try:
            num = int(m2.group(1))
            if 1 <= num <= 150:
                return num
        except Exception:
            pass
    m3 = re.search(r"(\d{1,3})\s*(?:集|话|期)", text)
    if m3:
        try:
            num = int(m3.group(1))
            if 1 <= num <= 150:
                return num
        except Exception:
            pass
    return None

conn = sqlite3.connect(SQLITE_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT id, movie_id, kind, label, url FROM download_links WHERE episode IS NULL")
rows = cur.fetchall()
updated = 0
for r in rows:
    ep = infer_episode(r["label"], r["url"]) if (r["label"] or r["url"]) else None
    if ep is not None:
        cur.execute("UPDATE download_links SET episode=? WHERE id=?", (ep, r["id"]))
        updated += 1
        print(f"updated id={r['id']} movie_id={r['movie_id']} kind={r['kind']} episode={ep} label={r['label']}")
conn.commit()
print(f"Done. Updated {updated} rows.")
conn.close()