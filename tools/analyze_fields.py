import sqlite3
from bs4 import BeautifulSoup
from collections import Counter

DB_PATH = r'c:/Code/dyttindex/data/movies.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('数据库:', DB_PATH)
cur.execute('select count(*) as c from movies')
TOTAL = cur.fetchone()['c'] or 0
print('总条目数:', TOTAL)

# 字段覆盖率
cur.execute('select count(*) as c from movies where year is not null')
y_has = cur.fetchone()['c'] or 0
cur.execute('select count(*) as c from movies where country is not null')
c_has = cur.fetchone()['c'] or 0
cur.execute('select count(distinct movie_id) as c from download_links')
dl_has = cur.fetchone()['c'] or 0

print('字段覆盖率:')
print(f' - 年份非空: {y_has} / {TOTAL} ({round(100*y_has/max(TOTAL,1), 1)}%)')
print(f' - 国家非空: {c_has} / {TOTAL} ({round(100*c_has/max(TOTAL,1), 1)}%)')
print(f' - 含下载链接: {dl_has} / {TOTAL} ({round(100*dl_has/max(TOTAL,1), 1)}%)')

# 类别分布（截断显示）
cur.execute('select kind, count(*) as c from movies group by kind order by c desc')
print('类别分布Top10:')
for r in cur.fetchall()[:10]:
    print(f" - {r['kind']}: {r['c']}")

# 抽样：缺失国家/年份的详情页，输出Zoom文本片段
samples = []
cur.execute('select id, title, detail_url, raw_html from movies where country is null or year is null order by id desc limit 6')
for r in cur.fetchall():
    soup = BeautifulSoup(r['raw_html'] or '', 'lxml')
    zoom = soup.select_one('#Zoom') or soup
    text = (zoom.get_text('\n', strip=True) or '')
    head = '\n'.join(text.split('\n')[:20])  # 前20行
    samples.append({
        'id': r['id'],
        'title': r['title'],
        'url': r['detail_url'],
        'zoom_head': head,
    })

print('缺失字段样本(最多6条):')
for s in samples:
    print('-'*60)
    print(f"ID {s['id']}  {s['title']}\n{ s['url'] }")
    print(s['zoom_head'])

# 下载链接类型占比
cur.execute('select kind, count(*) as c from download_links group by kind order by c desc')
print('下载链接类型:')
kinds = cur.fetchall()
for r in kinds:
    print(f" - {r['kind']}: {r['c']}")

conn.close()
