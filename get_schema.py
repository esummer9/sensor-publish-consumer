import sqlite3
conn = sqlite3.connect('farmtos_of.db')
cursor = conn.cursor()
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='tb_farmtos_of_collect_raindrop'")
row = cursor.fetchone()
if row:
    print(row[0])
else:
    print("Table not found")
conn.close()
