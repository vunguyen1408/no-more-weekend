import sys
import json
import os
import time
import cx_Oracle
from datetime import datetime , timedelta, date


def ManualFlow(connect, path_data, date):
#=================== Read flag running =============================	
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	path = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEMP_DATA/2017-08-30/ACCOUNT_ID'

	list_folder = next(os.walk(path))[1]
	for folder in list_folder:	
		path_file = os.path.join(path, folder)	
		list_file = next(os.walk(path_file))[2]	
		for file in list_file:
			f = os.path.join(path_file, file)		
			with open(f , 'r') as fi:
				data = json.load(fi)
			
			for value in data:					
				name = value['Campaign']
				name = value['Campaign'].encode('utf-8').strip()

	
				statement = "insert into DTM_GG_RUN_FLAG (FLAG_RUNNING, FINAL_RUNTIME) values (:1, :2)"
				cursor.execute(statement, (name, None))
				conn.commit()	
	cursor.close()



path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'