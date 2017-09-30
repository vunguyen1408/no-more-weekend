import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date


def AccountFrmCampaign(customer, path_data, date):
	path = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + customer)
	path_data_map = os.path.join(path, 'campaign_' + str(date) + '.json')
	path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)

	if os.path.exists(path_data_map):
		with open (path_data_map,'r') as f:
			data = json.load(f)

		path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
		if not os.path.exists(path_data_his):
			i = 0
			find = True
			date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
			path_data_his = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
			while not os.path.exists(path_data_his):
				i = i + 1
				date_before = date_before - timedelta(1)
				path_data_his = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
				if i == 30:
					find = False
					break
			if not find:
				path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
				data_total = {}
				data_total['HISTORY'] = []
				with open (path_data_his,'w') as f:
					json.dump(data_total, f)

		with open (path_data_his,'r') as f:
			data_total = json.load(f)
		for camp in data:
			flag = True
			for name in data_total['HISTORY']:
				if str(camp['Campaign ID']) == str(name['CAMPAIGN_ID']):
					flag = False
					if camp['Campaign'] != name['CAMPAIGN_NAME']:
						name['CAMPAIGN_NAME'] = camp['Campaign']
						print ("trung ====================================")
			if flag:
				# ----------------- Add new -----------------------
				# print (camp)
				temp = {
					'ACCOUNT_ID': camp['Account ID'],
					'CAMPAIGN_ID' : camp['Campaign ID'],

					'CAMPAIGN_NAME' :camp['Campaign'],
					'DATE_GET' :camp['Date'],
					'UPDATE_DATE': str(date)
				}
				data_total['HISTORY'].append(temp)
		path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
		with open (path_data_his,'w') as f:
			json.dump(data_total, f)


def InsertCampList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into STG_CAMPAIGN_LIST_GG ( \
	ACCOUNT_ID, CAMPAIGN_ID, CAMPAIGN_NAME, INSERT_DATE, \
	UPDATE_DATE, STATUS, IMPORT_DATE \
	values (:1, :2, :3, :4, :5, :6) '
		
	cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'], \
		value['INSERT_DATE'], value['UPDATE_DATE'], value['STATUS'], value['IMPORT_DATE']))
	
	print("A row inserted!.......")

def UpdateCampList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'update STG_CAMPAIGN_LIST_GG \
	set CAMPAIGN_NAME = :1, INSERT_DATE = :2, UPDATE_DATE = :3, \
	where ACCOUNT_ID = :4 and CAMPAIGN_ID = :5'
	
		
	cursor.execute(statement, (value['CAMPAIGN_NAME'], value['INSERT_DATE'], \
		value['UPDATE_DATE'], value['ACCOUNT_ID'], value['CAMPAIGN_ID'],))

	print("   A row updated!.......")

def ConvertJsonPlan(value):
	json_ = {}
		
	json_['ACCOUNT_ID'] = value['ACCOUNT_ID']
	json_['CAMPAIGN_ID'] = value['CAMPAIGN_ID']
	json_['CAMPAIGN_NAME'] = value['CAMPAIGN_NAME']

	json_['INSERT_DATE'] = value['DATE_GET']
	json_['UPDATE_DATE'] = value['UPDATE_DATE']
	json_['STATUS'] = ''
	json_['IMPORT_DATE'] = datetime.now().date()
	
	return json_


def MergerCampList(value, cursor):
	#==================== Insert data into database =============================
	statement = 'select CAMPAIGN_NAME from STG_CAMPAIGN_LIST_GG \
	where ACCOUNT_ID = :1 and CAMPAIGN_ID = :2'	
		
	cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID']))
	res = cursor.fetchall()
	
	if (res is None):
		InsertCampList(value, cursor)
	elif (value['CAMPAIGN_NAME'] == res):
		UpdateCampList(value, cursor)
	print("	A row mergered!.......")

def InsertHistoryName(connect, path_data, list_account, date):
	for account in list_account:
		AccountFrmCampaign(account, path_data, date)
		path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
		if os.path.exists(path_data_his):
			with open (path_data_his,'r') as f:
				data = json.load(f)
			for i in date['HISTORY']:
				MergerCampList(value, cursor)


