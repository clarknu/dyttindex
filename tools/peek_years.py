import sqlite3
DB_PATH = r'c:/Code/dyttindex/data/movies.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('select id, title, year, country, detail_url from movies order by id desc limit 10')
for row in cur.fetchall():
    print(row)
cur.execute('select count(*) from movies where year is null')
print('year_null_count:', cur.fetchone()[0])
cur.execute('select count(*) from movies where year is not null')
print('year_not_null_count:', cur.fetchone()[0])
conn.close()
