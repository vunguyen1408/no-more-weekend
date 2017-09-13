import cx_Oracle
import json
import datetime

conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

query = 'select Year, month, Legal, Department, Dept, Product, Reason code-Oracle, Eform No, Start Day, End Day-Estimate, \
                Channel, Eform Type, Unit _Option, \
                Unit cost, Amount USD, Value, \
		Engagement, Impressions, Click, Views, Install, NRU from STG_FA_DATA_GG'
cursor.execute(query)

row = cursor.fetchall()
temp = list(row)
unmap = {}
print (row)
	json.dump(unmap, f) 