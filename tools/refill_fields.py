import os, sys
# 将项目根目录加入 sys.path，便于导入 dyttindex 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dyttindex.scraper import parse_detail_page
from dyttindex.db import get_conn, upsert_movie
import sqlite3

conn = get_conn()
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("select id, detail_url, raw_html from movies where (year is null or country is null) and raw_html is not null")
rows = cur.fetchall()
print(f"待回填条目: {len(rows)}")

fixed = 0
for r in rows:
    html = r['raw_html'] or ''
    url = r['detail_url']
    try:
        data = parse_detail_page(html, url)
        upsert_movie(conn, data)
        fixed += 1
    except Exception as e:
        print(f"回填失败 id={r['id']} url={url}: {e}")

print(f"已回填: {fixed}")
conn.close()

