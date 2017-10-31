import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date
import time

#======================================================================================================
#                                 Add NRU sum for each plan 
#======================================================================================================

def Read_NRU_for_total(cursor, start_date, end_date, product):
	#==================== Get NRU =============================
	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
	and  SNAPSHOT_DATE >= :1 and SNAPSHOT_DATE <= :2"
	cursor.execute(statement, (start_date, end_date))
	list_NRU = list(cursor.fetchall())  
	# print(list_NRU)

	#==================== Get product ID ===================
	statement = "Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT where PRODUCT_ID = '" + product +  "'"
	cursor.execute(statement)
	list_product = list(cursor.fetchall())

	ccd_nru = 0  
	list_nru = []
	for i in range(len(list_NRU)):
		list_NRU[i] = list(list_NRU[i])    
		for pro in list_product:
			if (list_NRU[i][1] == pro[1]):
				data = [list_NRU[i][0], list_NRU[i][1], list_NRU[i][2], pro[0], pro[1]]
				if data not in list_nru:					
					list_nru.append(data)
					ccd_nru += list_NRU[i][2] 
	
	return ccd_nru


def ChooseTime(plan):
  if plan['REAL_START_DATE'] is not None:
    start_plan = plan['REAL_START_DATE']
  else:
    start_plan = plan['START_DAY']
    
  if plan['REAL_END_DATE'] is not None:
    end_plan = plan['REAL_END_DATE']
  else:
    end_plan = plan['END_DAY_ESTIMATE']

  return (start_plan, end_plan)


def ConvertDate(date):   
	"""
		Date has format: YYYY-MM-DD
		Return result having format: MM/DD/YYYY
	"""
	result = date
	day = date[8:]
	month = date[5:-3]
	year = date[:4]
	result = month + '/' + day + '/' + year
	result = datetime.strptime(result, '%m/%d/%Y')

	return result


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


def Add_NRU_into_plan(connect, path_data, date):
	file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')
	with open(file_plan, 'r') as fi:
		data = json.load(fi)
	
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	for plan in data['plan']:
		start_date, end_date = ChooseTime(plan)
		data['plan'][data['plan'].index(plan)]['CCD_NRU'] = Read_NRU_for_total(cursor, ConvertDate(start_date), ConvertDate(end_date), plan['PRODUCT'])
		
	with open(file_plan, 'w') as fo:
		json.dump(data, fo)

	cursor.close()
	return data['plan']
	


# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
# date = '2017-03-01' 
# Add_NRU_into_plan(connect, path_data, date)



#======================================================================================================
#                                 Add NRU monthly for each plan 
#======================================================================================================

def Add_NRU_into_monthly(connect, path_data, date):
	file_total = os.path.join(path_data, str(date) + '/DATA_MAPPING/total_mapping.json')
	with open(file_total, 'r') as fi:
		list_plan = json.load(fi)


	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	for plan in list_plan['TOTAL']:
		start_date, end_date = ChooseTime(plan)
		if ('MONTHLY' in plan):		
			CaculatorStartEndDate(plan, start_date, end_date)	
			for i in range(len(plan['MONTHLY'])):
				start_date = ConvertDate(plan['MONTHLY'][i]['START_DATE'])
				end_date = ConvertDate(plan['MONTHLY'][i]['END_DATE'])
				list_plan['TOTAL'][list_plan['TOTAL'].index(plan)]['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])

				

	for plan in list_plan['UN_PLAN']:
		start_date, end_date = ChooseTime(plan)
		if ('MONTHLY' in plan):
			CaculatorStartEndDate(plan, start_date, end_date)				
			for i in range(len(plan['MONTHLY'])):
				start_date = ConvertDate(plan['MONTHLY'][i]['START_DATE'])
				end_date = ConvertDate(plan['MONTHLY'][i]['END_DATE'])
				list_plan['UN_PLAN'][list_plan['UN_PLAN'].index(plan)]['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])

	cursor.close()

	with open(file_total, 'w') as fo:
		json.dump(list_plan, fo)


# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
# date = '2017-09-01' 
# Add_NRU_into_monthly(connect, path_data, date)















