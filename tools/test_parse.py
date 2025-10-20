import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
from bs4 import BeautifulSoup
from dyttindex.scraper import parse_detail_page, FIELD_PATTERNS

DB_PATH = r'c:/Code/dyttindex/data/movies.db'
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute('select id, title, detail_url, raw_html from movies order by id desc limit 1')
r = cur.fetchone()
print('ID:', r['id'], r['title'])
html = r['raw_html']
soup = BeautifulSoup(html, 'lxml')
zoom = soup.select_one('#Zoom') or soup
text = zoom.get_text('\n', strip=True)
lines = [l for l in text.split('\n') if l.strip()]
print('---原始行前20---')
for l in lines[:20]:
    print(l)
print('---规范化后前20---')
for l in lines[:20]:
    l2 = l.replace('\u3000','').replace('\xa0',' ')
    print(l2)
print('---匹配测试---')
for l in lines[:30]:
    l2 = l.replace('\u3000','').replace('\xa0',' ')
    for key, pat in FIELD_PATTERNS.items():
        m = pat.match(l2)
        if m:
            if key in ('year','release','country','language'):
                print('hit', key, '=>', m.group(2))
            break

parsed = parse_detail_page(html, r['detail_url'])
print('parsed:', parsed)
print('parsed year:', parsed.get('year'), 'country:', parsed.get('country'))

conn.close()
