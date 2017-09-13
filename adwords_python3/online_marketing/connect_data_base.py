import cx_Oracle
import json

conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

query = 'select * from STG_FA_DATA_GG'
cursor.execute(query)

row = cursor.fetchall()
unmap['plan'] = row
with open ('plan.json','w') as f:
	json.dump(unmap, f)
	
print (row)