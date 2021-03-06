import cx_Oracle
import json
import os
from datetime import datetime

	

def InsertMCCList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into ODS_GG_ACCOUNT_LIST ( \
	MCC, MCC_ID, ENTITY, DEPT, STATUS, CONTACT_POINT) \
	values (:1, :2, :3, :4, :5, :6)'	
		
	cursor.execute(statement, (value['MCC'], value['MCC_ID'], \
		None, value['DEPT'], None, None))
	
	print("A row inserted!.......")

def InsertMCCListToDatabase(path_data, connect):
	#================ Get full account get from Adwords ===============
	path_mcc  = os.path.join(path_data, 'MCC.json')
	path_wpl  = os.path.join(path_data, 'WPL.json')
	list_acc = []
	list_acc_id = []
	list_dept = []

	with open(path_mcc, 'r') as fi:
		data = json.load(fi)
	for value in data:
		list_acc.append(value['name'])
		list_acc_id.append(str(value['customerId']))
		list_dept.append(value['dept'])

	with open(path_wpl, 'r') as fi:
		data = json.load(fi)
	for value in data:
		list_acc.append(value['name'])
		list_acc_id.append(str(value['customerId']))
		list_dept.append(value['dept'])

	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	# ================ Get account from database =================
	statement = 'select * from ODS_GG_ACCOUNT_LIST'	
		
	cursor.execute(statement)
	res = list(cursor.fetchall())
	list_mcc_id = []	
	for acc in res:
		acc = list(acc)
		list_mcc_id.append(acc[1])
		# list_dept.append(acc[3])


	for i in range(len(list_acc)):
		if (list_acc_id[i] not in list_mcc_id):			
			if (list_dept[i] is not None):
				value = {
					'MCC': list_acc[i], 
					'MCC_ID': list_acc_id[i], 
					'ENTITY': None, 
					'DEPT': list_dept[i], 
					'STATUS': None, 
					'CONTACT_POINT': None
				}
			else:
				value = {
					'MCC': list_acc[i], 
					'MCC_ID': list_acc_id[i], 
					'ENTITY': None, 
					'DEPT': 'Unidentified', 
					'STATUS': None, 
					'CONTACT_POINT': None
				}
				
			print(value)
			try:
				InsertMCCList(value, cursor)
			except UnicodeEncodeError as e:				
				value['MCC'] = value['MCC'].encode('utf-8')
				InsertMCCList(value, cursor)



	conn.commit()
	print("Committed!.......")
	cursor.close()



path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
InsertMCCListToDatabase(path_data, connect)



