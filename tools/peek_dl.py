import sqlite3

SQLITE_PATH = r"c:\\Code\\dyttindex\\data\\movies.db"

conn = sqlite3.connect(SQLITE_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT d.id, m.title, d.kind, d.label, d.url, d.episode FROM download_links d JOIN movies m ON d.movie_id=m.id ORDER BY d.id DESC LIMIT 15")
rows = cur.fetchall()
for r in rows:
    print(f"#{r['id']} [{r['kind']}] episode={r['episode']} title={r['title']} label={r['label']}\n  {r['url']}")
conn.close()