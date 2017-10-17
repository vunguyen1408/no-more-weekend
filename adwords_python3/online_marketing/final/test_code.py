import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

def InsertCampList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into STG_CAMPAIGN_LIST_GG ( \
	ACCOUNT_ID, CAMPAIGN_ID, CAMPAIGN_NAME, INSERT_DATE, \
	UPDATE_DATE, STATUS, IMPORT_DATE) \
	values (:1, :2, :3, :4, :5, :6, :7) '
	
	try:
		cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'], \
			datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), None, None))
	except UnicodeEncodeError as e:
		cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'].encode('utf-8'), \
			datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), None, None))
		# print (e)
	
	# print("A row inserted!.......")

def UpdateCampList(value, cursor):
	#==================== Insert data into database =============================

	statement = 'update STG_CAMPAIGN_LIST_GG \
	set CAMPAIGN_NAME = :1, INSERT_DATE = :2, UPDATE_DATE = :3 \
	where ACCOUNT_ID = :4 and CAMPAIGN_ID = :5'
	
	try:
		cursor.execute(statement, (value['CAMPAIGN_NAME'], datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), \
			datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), value['ACCOUNT_ID'], value['CAMPAIGN_ID']))
	except UnicodeEncodeError as e:
		cursor.execute(statement, (value['CAMPAIGN_NAME'].encode('utf-8'), datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), \
			datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), value['ACCOUNT_ID'], value['CAMPAIGN_ID']))
		# print (e)

	# print("   A row updated!.......")


def MergerCampList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'select CAMPAIGN_NAME from STG_CAMPAIGN_LIST_GG \
	where ACCOUNT_ID = :1 and CAMPAIGN_ID = :2 and INSERT_DATE = :3'	
		
	cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], datetime.strptime(value['DATE_GET'], '%Y-%m-%d')))
	res = cursor.fetchall()

	if (len(res) == 0):
		InsertCampList(value, cursor)
	elif (value['CAMPAIGN_NAME'] != res[0][0]):
		print ("---------------------------- UPDATE NAME ----------------------------------")
		UpdateCampList(value, cursor)
	# print("	A row mergered!.......")

def InsertHistoryName(connect, path_data, list_account, date):
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()
	# for account in list_account:
	# 	AccountFromCampaign(account, path_data, date)
	path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
	print (path_data_his)
	if os.path.exists(path_data_his):
		with open (path_data_his,'r') as f:
			data = json.load(f)
		for i in data['HISTORY']:
			print ('==============================')
			MergerCampList(i, cursor)
	conn.commit()
	# print("Committed!.......")
	cursor.close()

list_account = []
end_date = '2017-08-31'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
InsertHistoryName(connect, path_data, list_account, date)