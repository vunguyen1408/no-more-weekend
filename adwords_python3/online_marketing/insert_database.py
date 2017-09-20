import cx_Oracle
import json
import datetime

#============================== Connect database =============================
conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

query = 'select * \
		from DTM_GG_PIVOT_DETAIL'

cursor.execute(query)
row = cursor.fetchall()
print (row)