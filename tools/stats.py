import sqlite3
from collections import Counter

DB_PATH = r'c:/Code/dyttindex/data/movies.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('数据库:', DB_PATH)
cur.execute('select count(*) as c from movies')
print('总电影数:', cur.fetchone()['c'])

cur.execute('select kind, count(*) as c from movies group by kind order by c desc')
print('按类别计数:')
for r in cur.fetchall():
    print(f" - {r['kind']}: {r['c']}")

cur.execute("select rating_source, count(*) as c from movies where rating_value is not null group by rating_source order by c desc")
print('有评分的条目（按来源）:')
for r in cur.fetchall():
    print(f" - {r['rating_source'] or '未知'}: {r['c']}")

cur.execute('select avg(rating_value) as avg_rating from movies where rating_value is not null')
avg = cur.fetchone()['avg_rating']
print('平均评分:', round(avg or 0, 2))

cur.execute('''
select t.name as tag, count(*) as c
from movie_tags mt join tags t on t.id = mt.tag_id
group by t.name
order by c desc
limit 10
''')
print('热门标签 Top10:')
for r in cur.fetchall():
    print(f" - {r['tag']}: {r['c']}")

cur.execute('select kind, count(*) as c from download_links group by kind order by c desc')
print('下载链接（按类型）:')
for r in cur.fetchall():
    print(f" - {r['kind']}: {r['c']}")

cur.execute('select avg(cnt) as avg_dl from (select movie_id, count(*) as cnt from download_links group by movie_id)')
avg_dl = cur.fetchone()['avg_dl']
print('平均每部下载链接数:', round(avg_dl or 0, 2))

cur.execute('select count(*) as dup from (select detail_url, count(*) as c from movies group by detail_url having c>1)')
print('重复详情页条目数:', cur.fetchone()['dup'])

conn.close()