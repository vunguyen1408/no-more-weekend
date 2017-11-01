import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

import mapping_campaign_plan as mapping_data
import insert_data_map_to_total as insert_to_total

def GetInstallAppsFlyer(connect, start_date, end_date, media_source, list_product_alias):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	day = start_date[8:]
	month = start_date[5:-3]
	year = start_date[:4]
	start_date = month + '/' + day + '/' + year

	day = end_date[8:]
	month = end_date[5:-3]
	year = end_date[:4]
	end_date = month + '/' + day + '/' + year


	statement = "select * from ods_appsflyer where SNAPSHOT_DATE >= to_date('" + start_date + "', 'mm/dd/yyyy') \
	and SNAPSHOT_DATE <= to_date('" + end_date + "', 'mm/dd/yyyy') and MEDIA_SOURCE like '%" + media_source +  "%'"

	cursor.execute(statement)
	print (statement)

	list_install = cursor.fetchall()
	list_install_for_product = []
	number_install = 0
	print (list_product_alias)
	for i in list_install:
		if i[5] in list_product_alias:
			# print (i)
			# print (int(i[3]))
			list_install_for_product.append(i)
			# number_install += i[3]
	# print (number_install)
	return list_install_for_product


def CaculatorInstallForPlan(list_install_for_product, plan, start_date, end_date):
	number_install = 0

	date_ = datetime.strptime(start_date, '%Y-%m-%d').date()
	to_date_ = datetime.strptime(end_date, '%Y-%m-%d').date()

	if 'CAMPAIGN' not in plan:
		return number_install
	list_campaign_id = []
	for camp in plan['CAMPAIGN']:
		date_camp = datetime.strptime(camp['Date'], '%Y-%m-%d').date()
		if date_camp >= date_ and date_camp <= to_date_:
			if str(camp['CAMPAIGN_ID']) not in list_campaign_id:
				list_campaign_id.append(str(camp['CAMPAIGN_ID']))
				for install in list_install_for_product:
					d = str(install[0])[:10]
					d = datetime.strptime(d, '%Y-%m-%d').date()
					if d >= date_ and d <= to_date_ and str(camp['CAMPAIGN_ID']) == str(install[2]):
						number_install += int(install[3])

	return number_install



def CaculatorStartEndDate(plan, start, end):
	from datetime import datetime , timedelta, date
	# Get start end
	month_start = int(start[5:-3])
	month_end = int(end[5:-3])
	year_end = end[:4]
	for month in plan['MONTHLY']:
		if (int(month['MONTH']) == month_start):
			start_date = datetime.strptime(start, '%Y-%m-%d').date()
			end_date = start_date + timedelta(int(month['DAY']) - 1)
			start_date = start_date.strftime('%Y-%m-%d')
			end_date = end_date.strftime('%Y-%m-%d')
		else:
			start_date = year_end + '-' + str(month['MONTH']) + '-01'
			start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
			end_date = start_date + timedelta(int(month['DAY']) - 1)
			start_date = start_date.strftime('%Y-%m-%d')
			end_date = end_date.strftime('%Y-%m-%d')
		month['START_DATE'] = start_date
		month['END_DATE'] = end_date
	return plan


def InsertInstallToPlan(path_data, connect, date):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	# mapping_data.ReadProductAlias(connect, path_data, date)
	# # Get list product alias
	# file_product_alias = os.path.join(path_data, str(date) + '/PLAN/product_alias.json')
	# with open (file_product_alias,'r') as f:
	# 	data_product_alias = json.load(f)


	# list_diff = ReadTableManualMap(connect, path_data, date)
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')

	if not os.path.exists(path_data_total_map):
		i = 0
		find = True
		date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
		path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
		while not os.path.exists(path_data_total_map):
			i = i + 1
			date_before = date_before - timedelta(1)
			path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
			if i == 60:
				find = False
				break
		# ---- Neu tim thay file total truoc do -----
	else:
		find = True

	if find:
		media_source = 'oogle'
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)
		# print (len(data_total['TOTAL']))
		loop = 0
		for plan in data_total['TOTAL']:
			# if loop == 2:
			# 	break
			# print (plan)
			if plan['UNIT_OPTION'] == 'CPI':
				# loop += 1
				start_date, end_date = mapping_data.ChooseTime(plan)
				# temp = GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source1, media_source2, plan['APPSFLYER_PRODUCT'])
				install_before = plan['TOTAL_CAMPAIGN'].get('INSTALL_CAMP', 0)
				list_install_for_product = GetInstallAppsFlyer(connect, start_date, end_date, media_source, plan['APPSFLYER_PRODUCT'])
				plan['TOTAL_CAMPAIGN']['INSTALL_CAMP'] = CaculatorInstallForPlan(list_install_for_product, plan, start_date, end_date)

				# print (len(list_install_for_product))
				# number_install = CaculatorInstallForPlan(list_install_for_product, plan, start_date, end_date)
				# print("============================+++++++++==================================")
				# print (plan)
				# print (number_install)
				# print("================================================")


				plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = plan['TOTAL_CAMPAIGN']['INSTALL_CAMP']
				if ('MONTHLY' in plan):
					plan = CaculatorStartEndDate(plan, start_date, end_date)
					# print (plan['MONTHLY'])
					for month in plan['MONTHLY']:
						install_before = month['TOTAL_CAMPAIGN_MONTHLY'].get('INSTALL_CAMP', 0)


						# number_install = CaculatorInstallForPlan(list_install_for_product, plan, month['START_DATE'], month['END_DATE'])
						# print("================================================")
						# print (plan['MONTHLY'])
						# print (number_install)
						# print("================================================")


						month['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP'] = CaculatorInstallForPlan(list_install_for_product, plan, month['START_DATE'], month['END_DATE'])
						month['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = month['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP']
						# print ("--")
		# path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		# with open (path_data_total_map,'w') as f:
		# 	json.dump(data_total, f)
		print ("ok")


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
date = '2017-09-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'

InsertInstallToPlan(path_data, connect, date)