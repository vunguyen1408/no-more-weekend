import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

def FindNameNew(data_total, camp_id, camp_name):
	flag = 0
	date_max = '0001-01-01'
	name_max = ''
	camp_id_ = ''
	index = 0
	if str(camp_id) == '734063969' or str(camp_id) == '772872164':
		return -1
	for i, name in enumerate(data_total):
		# print (i)
		if str(camp_id) == str(name['CAMPAIGN_ID']):
			flag = 1
			if datetime.strptime(date_max, '%Y-%m-%d').date() < datetime.strptime(name['UPDATE_DATE'], '%Y-%m-%d').date():
				name_max = name['CAMPAIGN_NAME']
				date_max = str(name['UPDATE_DATE'])
				camp_id_ = str(name['CAMPAIGN_ID'])
				index = i

	if flag == 0:
		return 0
	else:
		if camp_name != name_max:
			# print (date_max)
			# print (name_max)
			# print (camp_id_)
			# print (index)
			flag = -1
			return flag
		else:
			flag = 1
			return flag
	

def AccountFromCampaign(customer, path_data, date):
	list_temp = []
	path = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + customer)
	path_data_map = os.path.join(path, 'campaign_' + str(date) + '.json')
	path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)

	if os.path.exists(path_data_map):
		with open (path_data_map,'r') as f:
			data = json.load(f)
		if data != []:
			f_ = False
			path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')

			if os.path.exists(path_data_his):
				with open (path_data_his,'r') as f:
					data_total = json.load(f)
				if data_total['HISTORY'] == []:
					print ("file rong")
					f_ = True

			if (not os.path.exists(path_data_his)) or f_:
				i = 0
				find = True
				date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
				path_data_his = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
				while not os.path.exists(path_data_his):
					i = i + 1
					date_before = date_before - timedelta(1)
					path_data_his = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
					if i == 60:
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
				if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
					flag = FindNameNew(data_total['HISTORY'], camp['Campaign ID'], camp['Campaign'])	
					if flag == 0 or flag == -1:
						if flag == -1:
							list_temp.append(camp)
						# ----------------- Add new -----------------------
						# print (camp)
						temp = {
							'ACCOUNT_ID': camp['Account ID'],
							'CAMPAIGN_ID' : camp['Campaign ID'],

							'CAMPAIGN_NAME' :camp['Campaign'],
							'DATE_GET' :camp['Date'],
							'UPDATE_DATE': str(date),
							'IMPORT_DATE' : None
						}
						data_total['HISTORY'].append(temp)
			print (date)
			print (len(data_total['HISTORY']))
			print ("=================================================")
			path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
			with open (path_data_his,'w') as f:
				json.dump(data_total, f)
	return list_temp

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
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()
	list_diff = []


	path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)
	# path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
	# data_total = {}
	# data_total['HISTORY'] = []
	# with open (path_data_his,'w') as f:
	# 	json.dump(data_total, f)

		
	for account in list_account:
		list_temp = AccountFromCampaign(account, path_data, date)
		list_diff.append(list_temp)

	#=========================================================
	# path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
	# if os.path.exists(path_data_his):
	# 	with open (path_data_his,'r') as f:
	# 		data = json.load(f)
	# 	for i in data['HISTORY']:
	# 		MergerCampList(i, cursor)
	# conn.commit()
	# # print("Committed!.......")
	# cursor.close()
	#===========================================================

	return list_diff



