from impala.dbapi import connect
conn = connect(host='192.168.101.69', port=21050)
cursor = conn.cursor()
cursor.execute('select count(*) from frv.frv_tras_parq')
print(cursor.description)  # prints the result set's schema

results = cursor.fetchall()
print(results)