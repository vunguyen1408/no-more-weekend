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
	print("Add NRU into plan success.........")


# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
# date = '2017-03-01' 
# Add_NRU_into_plan(connect, path_data, date)



#======================================================================================================
#                                 Add NRU monthly for each plan 
#======================================================================================================


def TEST(connect, path_data, date):
	file_plan = os.path.join(path_data, str(date) + '/DATA_MAPPING/total_mapping.json')
	with open(file_plan, 'r') as fi:
		data = json.load(fi)

	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	for plan in data['TOTAL']:
		start_date, end_date = ChooseTime(plan)
		# data['plan'][data['plan'].index(plan)]['CCD_NRU'] = Read_NRU_for_total(cursor, ConvertDate(start_date), ConvertDate(end_date), plan['PRODUCT'])
		CaculatorStartEndDate(plan, start_date, end_date)
		print()
		print(plan)
		print()

	# with open(file_plan, 'w') as fo:
	# 	json.dump(data, fo)

	cursor.close()
	# print("Add NRU into plan success.........")


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
date = '2017-09-01' 
TEST(connect, path_data, date)


# def Read_NRU_for_month(cursor, year, month, product):
# 	#==================== Get NRU =============================
# 	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
# 	and  extract (Year from SNAPSHOT_DATE) = :1 and extract (Month from SNAPSHOT_DATE) = :2"
# 	cursor.execute(statement, (year, month))
# 	list_NRU = list(cursor.fetchall())  
# 	# print(list_NRU)


# 	#==================== Get product ID ===================
# 	statement = "Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT where PRODUCT_ID = '" + product +  "'"
# 	cursor.execute(statement)
# 	list_product = list(cursor.fetchall())

# 	ccd_nru = 0  
# 	list_nru = []
# 	for i in range(len(list_NRU)):
# 		list_NRU[i] = list(list_NRU[i])    
# 		for pro in list_product:
# 			if (list_NRU[i][1] == pro[1]):
# 				data = [list_NRU[i][0], list_NRU[i][1], list_NRU[i][2], pro[0], pro[1]]
# 				if data not in list_nru:					
# 					list_nru.append(data)
# 					ccd_nru += list_NRU[i][2] 
	
# 	return ccd_nru


# def Add_NRU_for_monthly(connect, list_plan):
# # ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
# 	cursor = conn.cursor()

# 	for plan in list_plan['TOTAL']:
# 		if ('MONTHLY' in plan):
# 			for i in range(len(plan['MONTHLY'])):
# 				plan['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_month(cursor, str(plan['MONTHLY'][i]['MONTH']), '20' + str(plan['CYEAR']), plan['PRODUCT'])

# 	for plan in list_plan['UN_PLAN']:
# 		if ('MONTHLY' in plan):
# 			for i in range(len(plan['MONTHLY'])):
# 				plan['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_month(cursor, str(plan['MONTHLY'][i]['MONTH']), '20' + str(plan['CYEAR']), plan['PRODUCT'])

# 		day = plan['START_DAY'][8:]
# 		month = plan['START_DAY'][5:-3]
# 		year = plan['START_DAY'][:4]
# 		start_date = month + '/' + day + '/' + year
# 		start_date = datetime.strptime(start_date, '%m/%d/%Y')

# 		day = plan['END_DAY_ESTIMATE'][8:]
# 		month = plan['END_DAY_ESTIMATE'][5:-3]
# 		year = plan['END_DAY_ESTIMATE'][:4]
# 		end_date = month + '/' + day + '/' + year
# 		end_date = datetime.strptime(end_date, '%m/%d/%Y')	

# 		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])


# 	for plan in list_plan['MAP']:

# 		day = plan['START_DAY'][8:]
# 		month = plan['START_DAY'][5:-3]
# 		year = plan['START_DAY'][:4]
# 		start_date = month + '/' + day + '/' + year
# 		start_date = datetime.strptime(start_date, '%m/%d/%Y')

# 		day = plan['END_DAY_ESTIMATE'][8:]
# 		month = plan['END_DAY_ESTIMATE'][5:-3]
# 		year = plan['END_DAY_ESTIMATE'][:4]
# 		end_date = month + '/' + day + '/' + year
# 		end_date = datetime.strptime(end_date, '%m/%d/%Y')	

# 		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])

# 	cursor.close()
# 	return list_plan



# def Add_Data_To_Plan(connect, path_data, date):
# 	#============= Add Plan to Total================================
# 	# ============ Add Plan To Monthly ============================

# 	file_plan = os.path.join(path_data, str(date) + '/DATA_MAPPING/total_mapping.json')
# 	with open(file_plan, 'r') as fi:
# 		list_plan = json.load(fi)

# 	start = time.time()
# 	list_plan = Add_NRU_for_monthly(connect, list_plan)
# 	print ("Time add NRU in function", (time.time() - start))
	
# 	with open (file_plan,'w') as f:
# 		json.dump(list_plan, f)
# 	# print('Add nru====================')




# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
# date = '2017-08-30'
# Add_Data_To_Plan(connect, path_data, date)



# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# conn = cx_Oracle.connect(connect)
# cursor = conn.cursor()
# start_date = datetime.strptime('08/01/2017', '%m/%d/%Y')
# end_date = datetime.strptime('09/01/2017', '%m/%d/%Y')
# nru = Read_NRU_for_total(cursor, start_date, end_date, '219')
# print(nru)













