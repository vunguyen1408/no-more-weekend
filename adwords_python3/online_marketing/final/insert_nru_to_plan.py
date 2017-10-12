import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date



# def Read_NRU_for_total(connect, path_data, date):  
#   file_nru = os.path.join(path_data, str(date) + '/PLAN/nru.json')
#   # ==================== Connect database =======================
#   conn = cx_Oracle.connect(connect)
#   cursor = conn.cursor()

#   #==================== Get NRU =============================
#   statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google'"
#   cursor.execute(statement)
#   list_NRU = list(cursor.fetchall())  
  
#   #==================== Get product ID ===================
#   statement = 'Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT'
#   cursor.execute(statement)
#   list_product = list(cursor.fetchall())

#   list_json = []
#   for i in range(len(list_NRU)):
#     list_NRU[i] = list(list_NRU[i])
#     list_NRU[i].append(None)
#     for pro in list_product:
#       if (list_NRU[i][1] == pro[1]):    
#         list_NRU[i][3] = pro[0]    
  
#     json_ = {
#       'SNAPSHOT_DATE': list_NRU[i][0].strftime('%Y-%m-%d'),
#       'PRODUCT_CODE': list_NRU[i][1],
#       'NRU': list_NRU[i][2],
#       'PRODUCT_ID': list_NRU[i][3]
#     }
#     list_json.append(json_)

#   data_json = {}
#   data_json['NRU'] = list_json
#   with open(file_nru, 'w') as fo:
#     json.dump(data_json, fo)
#   cursor.close()


# def Add_NRU_for_total(path_folder, list_plan, date):
#   #================ Add product id to plan =================
#   file_product = os.path.join(path_folder, str(date) + '/PLAN/nru.json')
#   with open(file_product, 'r') as fi:
#     data = json.load(fi)  

#   list_temp = []
#   for plan in list_plan['plan']:    
#     for nru in data['NRU']:
#       date = datetime.strptime(nru['SNAPSHOT_DATE'], '%Y-%m-%d')
#       if (nru['PRODUCT_ID'] is not None) \
#       and (int(plan['PRODUCT']) == int(nru['PRODUCT_ID'])) \
#       and (date >= datetime.strptime(plan['START_DAY'], '%Y-%m-%d')) \
#       and (date <= datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d')):
#         plan['CCD_NRU'] = nru['NRU']    
  
#   return list_plan


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


def Add_NRU_for_total(connect, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	for plan in list_plan['UN_PLAN']:

		day = plan['START_DAY'][8:]
    	month = plan['START_DAY'][5:-3]
    	year = plan['START_DAY'][:4]
    	start_date = month + '/' + day + '/' + year
		start_date = datetime.strptime(start_date, '%m/%d/%Y')

		day = plan['END_DAY_ESTIMATE'][8:]
    	month = plan['END_DAY_ESTIMATE'][5:-3]
    	year = plan['END_DAY_ESTIMATE'][:4]
    	end_date = month + '/' + day + '/' + year
		end_date = datetime.strptime('08/01/2017', '%m/%d/%Y')	

		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])
		

	cursor.close()
	return list_plan



def Add_NRU_for_map(connect, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	for plan in list_plan['MAP']:

		day = plan['START_DAY'][8:]
    	month = plan['START_DAY'][5:-3]
    	year = plan['START_DAY'][:4]
    	start_date = month + '/' + day + '/' + year
		start_date = datetime.strptime(start_date, '%m/%d/%Y')

		day = plan['END_DAY_ESTIMATE'][8:]
    	month = plan['END_DAY_ESTIMATE'][5:-3]
    	year = plan['END_DAY_ESTIMATE'][:4]
    	end_date = month + '/' + day + '/' + year
		end_date = datetime.strptime('08/01/2017', '%m/%d/%Y')	
		
		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])
			

	cursor.close()
	return list_plan





def Read_NRU_for_month(cursor, year, month, product):
	#==================== Get NRU =============================
	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
	and  extract (Year from SNAPSHOT_DATE) = :1 and extract (Month from SNAPSHOT_DATE) = :2"
	cursor.execute(statement, (year, month))
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


def Add_NRU_for_monthly(connect, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	for plan in list_plan['TOTAL']:
		if ('MONTHLY' in plan):
			for i in range(len(plan['MONTHLY'])):
				plan['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_month(cursor, str(plan['MONTHLY'][i]['MONTH']), '20' + str(plan['CYEAR']), plan['PRODUCT'])

	for plan in list_plan['UN_PLAN']:
		if ('MONTHLY' in plan):
			for i in range(len(plan['MONTHLY'])):
				plan['MONTHLY'][i]['CCD_NRU'] = Read_NRU_for_month(cursor, str(plan['MONTHLY'][i]['MONTH']), '20' + str(plan['CYEAR']), plan['PRODUCT'])

		day = plan['START_DAY'][8:]
    	month = plan['START_DAY'][5:-3]
    	year = plan['START_DAY'][:4]
    	start_date = month + '/' + day + '/' + year
		start_date = datetime.strptime(start_date, '%m/%d/%Y')

		day = plan['END_DAY_ESTIMATE'][8:]
    	month = plan['END_DAY_ESTIMATE'][5:-3]
    	year = plan['END_DAY_ESTIMATE'][:4]
    	end_date = month + '/' + day + '/' + year
		end_date = datetime.strptime('08/01/2017', '%m/%d/%Y')	

		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])


	for plan in list_plan['MAP']:

		day = plan['START_DAY'][8:]
    	month = plan['START_DAY'][5:-3]
    	year = plan['START_DAY'][:4]
    	start_date = month + '/' + day + '/' + year
		start_date = datetime.strptime(start_date, '%m/%d/%Y')

		day = plan['END_DAY_ESTIMATE'][8:]
    	month = plan['END_DAY_ESTIMATE'][5:-3]
    	year = plan['END_DAY_ESTIMATE'][:4]
    	end_date = month + '/' + day + '/' + year
		end_date = datetime.strptime('08/01/2017', '%m/%d/%Y')	
		
		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])

	cursor.close()
	return list_plan



def Add_Data_To_Plan(connect, path_data, date):
	#============ Add Plan to Total================================



	# ============ Add Plan To Monthly ============================
	file_plan = os.path.join(path_data, str(date) + '/DATA_MAPPING/total_mapping.json')
	with open(file_plan, 'r') as fi:
		list_plan = json.load(fi)

	list_plan = Add_NRU_for_monthly(connect, list_plan)
	
	with open (file_plan,'w') as f:
		json.dump(list_plan, f)
	print('Add nru====================')




connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
date = '2017-08-31'
Add_Data_To_Plan(connect, path_data, date)



# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# conn = cx_Oracle.connect(connect)
# cursor = conn.cursor()
# start_date = datetime.strptime('08/01/2017', '%m/%d/%Y')
# end_date = datetime.strptime('09/01/2017', '%m/%d/%Y')
# nru = Read_NRU_for_total(cursor, start_date, end_date, '219')
# print(nru)




