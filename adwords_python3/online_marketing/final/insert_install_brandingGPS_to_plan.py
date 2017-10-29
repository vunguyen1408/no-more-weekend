import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

import mapping_campaign_plan as mapping_data
import insert_data_map_to_total as insert_to_total

def GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source1, media_source2, list_product_alias):
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


	statement = "select * from ods_appsflyer where CAMPAIGN_ID = '{" + "BrandingGPS}' and SNAPSHOT_DATE >= to_date('" + start_date + "', 'mm/dd/yyyy') \
	and SNAPSHOT_DATE <= to_date('" + end_date + "', 'mm/dd/yyyy') \
	and (MEDIA_SOURCE like '" + media_source1 +  "' or MEDIA_SOURCE like '" + media_source2 +  "')"

	cursor.execute(statement)
	# print (statement)

	list_install = cursor.fetchall()
	print (len(list_install))
	number_install = 0
	print (list_product_alias)
	for i in list_install:
		if i[5] in list_product_alias:
			# print (i)
			# print (int(i[3]))
			number_install += int(i[3])

	print (number_install)
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


def AddBrandingGPSToPlan(path_data, connect, date):
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
		media_source1 = 'googleadwords_int'
		media_source2 = 'googleadwords_sem'
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)
		# print (len(data_total['TOTAL']))
		for plan in data_total['TOTAL']:
			# print (plan)
			if plan['UNIT_OPTION'] == 'CPI':
				start_date, end_date = mapping_data.ChooseTime(plan)
				# temp = GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source1, media_source2, plan['APPSFLYER_PRODUCT'])
				plan['TOTAL_CAMPAIGN']['INSTALL_CAMP'] += GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source1, media_source2, plan['APPSFLYER_PRODUCT'])
				plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = plan['TOTAL_CAMPAIGN']['INSTALL_CAMP']
				if ('MONTHLY' in plan):
					print ("==========================")
					print (plan['MONTHLY'])
					print ("==========================")
					plan = CaculatorStartEndDate(plan, start_date, end_date)
					# print (plan['MONTHLY'])
					for month in plan['MONTHLY']:
						month['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP'] += GetDataSummaryAppsFlyer(connect, month['START_DATE'], month['END_DATE'], media_source1, media_source2, plan['APPSFLYER_PRODUCT'])
						month['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = month['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP']
						# print ("--")
		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		with open (path_data_total_map,'w') as f:
			json.dump(data_total, f)


