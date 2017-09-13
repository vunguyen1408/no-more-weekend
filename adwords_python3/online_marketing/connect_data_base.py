import cx_Oracle
import json
import datetime

conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

query = 'select * from STG_FA_DATA_GG'
cursor.execute(query)

row = cursor.fetchall()
temp = list(row)
unmap = {}
print (len(row))
for i in temp:
	for j in i:
		if isinstance(j, datetime.datetime):
			print (type(j))
			j = j.strftime('%Y-%m-%d')
			print (type(j))
unmap['plan'] = temp

with open ('plan.json','w') as f:
	json.dump(unmap, f) 