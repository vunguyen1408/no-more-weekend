import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

def GetDataSummaryAppsFlyer(connect, date, media_source1, media_source2, path_file):
    # ==================== Connect database =======================
    conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
    cursor = conn.cursor()

    day = date[8:]
    month = date[5:-3]
    year = date[:4]
    date = month + '-' + day + '-' + year
    statement = "select * from ods_appsflyer where SNAPSHOT_DATE \
    = to_date('" + date + "', 'mm/dd/yyyy') and (MEDIA_SOURCE like '" + media_source1 +  "' or MEDIA_SOURCE like '" + media_source2 +  "')"

    cursor.execute(statement)

    list_install = cursor.fetchall()
    list_out = []
    for i in list_install:
        d = str(i[0])[:10]
        d = str(datetime.strptime(d, '%Y-%m-%d').date())
        temp = []
        temp.append(d)
        temp.append(i[1])
        temp.append(i[2])
        temp.append(i[3])
        temp.append(i[4])
        temp.append(i[5])
        temp.append(i[6])
        list_out.append(temp)
    install = {}
    install['list_install'] = list_out
    with open(path_file, 'w') as f:
        json.dump(install, f)


def CaculatorStartEndDate():
	for month in json_['M']:
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



def Add_NRU_for_map(connect, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
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
		end_date = datetime.strptime(end_date, '%m/%d/%Y')	

		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])
			

	cursor.close()
	return list_plan





def CaculatorS


def Add_NRU_for_monthly(connect, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
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
		end_date = datetime.strptime(end_date, '%m/%d/%Y')	

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
		end_date = datetime.strptime(end_date, '%m/%d/%Y')	

		plan['CCD_NRU'] = Read_NRU_for_total(cursor, start_date, end_date, plan['PRODUCT'])

	cursor.close()
	return list_plan



def Add_Data_To_Plan(connect, path_data, date):
	#============= Add Plan to Total================================
	# ============ Add Plan To Monthly ============================

	file_plan = os.path.join(path_data, str(date) + '/DATA_MAPPING/total_mapping.json')
	with open(file_plan, 'r') as fi:
		list_plan = json.load(fi)

	list_plan = Add_NRU_for_monthly(connect, list_plan)
	
	with open (file_plan,'w') as f:
		json.dump(list_plan, f)
	# print('Add nru====================')




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




