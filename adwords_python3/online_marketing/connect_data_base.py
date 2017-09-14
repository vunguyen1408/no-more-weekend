import cx_Oracle
import json
import datetime

#============================== Connect database =============================
conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

#======================= Get data from database ==============================
query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, PRODUCT, REASON_CODE_ORACLE, EFORM_NO, \
				START_DAY, END_DAY_ESTIMATE, CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, CVALUE, \
				ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, INSTALL, NRU, INSERT_DATE \
		from STG_FA_DATA_GG'

cursor.execute(query)
row = cursor.fetchall()
temp = list(row)
#print (row)


#===================== Convert data into json =================================
unmap = {}
list_key = ['CYEAR', 'CMONTH', 'LEGAL', 'DEPARTMENT', 'DEPARTMENT_NAME', 'PRODUCT', 
			'REASON_CODE_ORACLE', 'EFORM_NO', 'START_DAY', 'END_DAY_ESTIMATE', 'CHANNEL', 
			'FORM_TYPE', 'UNIT_OPTION', 'UNIT_COST', 'AMOUNT_USD', 'CVALUE', 'ENGAGEMENT', 
			'IMPRESSIONS', 'CLIKE', 'CVIEWS', 'INSTALL', 'NRU', 'INSERT_DATE']

list_temp = []
for plan in temp:
	for value in plan:
		if (value.isdigit() or value.replace(".", "").isdigit()):
			value = float(value)
		if isinstance(value, datetime.datetime):            
            value = value.strftime('%Y-%m-%d')
print (temp)