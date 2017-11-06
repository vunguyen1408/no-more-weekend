import cx_Oracle
import json
import os
from datetime import datetime , timedelta, date


def InsertMonthlyDetail(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_PIVOT_DETAIL (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
	START_DATE, END_DATE, CHANNEL, UNIT_COST, AMOUNT_USD, \
	CVALUE, ENGAGEMENT, IMPRESSIONS, REACH, FREQUENCY, \
	CLIKE, CLICKS_ALL, LINK_CLICKS, CVIEWS, C3S_VIDEO_VIEW, \
	INSTALL, NRU, EFORM_TYPE, UNIT_OPTION, OBJECTIVE, \
	EVENT_ID, PRODUCT_ID, CCD_NRU, GG_VIEWS, GG_CONVERSION, \
	GG_INVALID_CLICKS, GG_ENGAGEMENTS, GG_VIDEO_VIEW, GG_CTR, GG_IMPRESSIONS, \
	GG_INTERACTIONS, GG_CLICKS, GG_INTERACTION_TYPE, GG_COST, GG_SPEND, \
	GG_APPSFLYER_INSTALL, GG_STRATEGY_BID_TYPE) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, \
	:21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, \
	:41, :42, :43, :44, :45, :46, :47)'	
		
	cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
		value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
		value['START_DATE'], value['END_DATE'], value['CHANNEL'], value['UNIT_COST'], value['AMOUNT_USD'], \
		value['CVALUE'], value['ENGAGEMENT'], value['IMPRESSIONS'], value['REACH'], value['FREQUENCY'], \
		value['CLIKE'], value['CLICKS_ALL'], value['LINK_CLICKS'], value['CVIEWS'], value['C3S_VIDEO_VIEW'], \
		value['INSTALL'], value['NRU'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['OBJECTIVE'], \
		value['EVENT_ID'], value['PRODUCT_ID'], value['CCD_NRU'], value['GG_VIEWS'], value['GG_CONVERSION'], \
		value['GG_INVALID_CLICKS'], value['GG_ENGAGEMENTS'], value['GG_VIDEO_VIEW'], value['GG_CTR'], value['GG_IMPRESSIONS'], \
		value['GG_INTERACTIONS'], value['GG_CLICKS'], value['GG_INTERACTION_TYPE'], value['GG_COST'], value['GG_SPEND'], \
		value['GG_APPSFLYER_INSTALL'], value['GG_STRATEGY_BID_TYPE']))	
	
	# print("A row inserted!.......")


def UpdateMonthlyDetail(value, cursor):
	#==================== Insert data into database =============================
	statement = 'update DTM_GG_PIVOT_DETAIL \
	set GG_VIEWS = :1, GG_CONVERSION = :2, GG_INVALID_CLICKS = :3, \
	GG_ENGAGEMENTS = :4, GG_VIDEO_VIEW = :5, GG_CTR = :6, \
	GG_IMPRESSIONS = :7, GG_INTERACTIONS = :8, GG_CLICKS = :9,\
	GG_COST = :10, GG_SPEND = :11, GG_APPSFLYER_INSTALL = :12 \
	where PRODUCT = :13 and REASON_CODE_ORACLE = :14 and EFORM_TYPE = :15 \
	and UNIT_OPTION = :16 and SNAPSHOT_DATE = :17'
	
		
	cursor.execute(statement, (value['GG_VIEWS'], value['GG_CONVERSION'], value['GG_INVALID_CLICKS'], \
		value['GG_ENGAGEMENTS'], value['GG_VIDEO_VIEW'], value['GG_CTR'], \
		value['GG_IMPRESSIONS'], value['GG_INTERACTIONS'], value['GG_CLICKS'], \
		value['GG_COST'], value['GG_SPEND'], value['GG_APPSFLYER_INSTALL'], \
		value['PRODUCT'], value['REASON_CODE_ORACLE'], value['EFORM_TYPE'], \
		value['UNIT_OPTION'], value['SNAPSHOT_DATE']))

	# print("A row updated!.......")


def DeleteMonthlyDetail(value, cursor):
	#==================== Remove plan from database =============================
	statement = 'delete from DTM_GG_PIVOT_DETAIL \
	where PRODUCT = :1 and REASON_CODE_ORACLE = :2 and EFORM_TYPE = :3 and UNIT_OPTION = :4'
		
	cursor.execute(statement, (value['PRODUCT'], value['REASON_CODE_ORACLE'], value['FORM_TYPE'], value['UNIT_OPTION']))	

def ConvertJsonMonthlyDetail(index, value):
	# print (value)
	json_ = {}	

	json_['CYEAR'] = '20' + value['CYEAR']
	# if (len(value['CMONTH']) == 1):
	# 	json_['CMONTH'] = '0' + value['CMONTH']
	# else:
	# 	json_['CMONTH'] = value['CMONTH']

	if (len(str(value['MONTHLY'][index]['MONTH'])) == 1):
		json_['SNAPSHOT_DATE'] = json_['CYEAR'] + '-0' + str(value['MONTHLY'][index]['MONTH'])
		json_['CMONTH'] = '-0' + str(value['MONTHLY'][index]['MONTH'])
	else:
		json_['SNAPSHOT_DATE'] = json_['CYEAR'] + '-' + str(value['MONTHLY'][index]['MONTH'])
		json_['CMONTH'] = str(value['MONTHLY'][index]['MONTH'])

	json_['LEGAL'] = value['LEGAL']
	json_['DEPARTMENT'] = value['DEPARTMENT']

	json_['DEPARTMENT_NAME'] = value['DEPARTMENT_NAME'] 
	json_['PRODUCT'] = value['PRODUCT'] 
	json_['PRODUCT_NAME'] = ''
	json_['REASON_CODE_ORACLE'] = value['REASON_CODE_ORACLE'] 
	json_['EFORM_NO'] = value['EFORM_NO'] 

	json_['START_DATE'] = datetime.strptime(value['START_DAY'], '%Y-%m-%d')
	json_['END_DATE'] = datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d')
	json_['CHANNEL'] = value['CHANNEL'] 
	json_['UNIT_COST'] = value['UNIT_COST'] 
	if (value['AMOUNT_USD'] is None):
		json_['AMOUNT_USD'] = value['AMOUNT_USD']
	else:
		json_['AMOUNT_USD'] = float(value['AMOUNT_USD'])

	if (value['CVALUE'] is None):
		json_['CVALUE'] = value['CVALUE']
	else:
		json_['CVALUE'] = float(value['CVALUE'])
	if (value['ENGAGEMENT'] is None):
		json_['ENGAGEMENT'] = value['ENGAGEMENT']
	else:
		json_['ENGAGEMENT'] = float(value['ENGAGEMENT'])
	if (value['IMPRESSIONS'] is None):
		json_['IMPRESSIONS'] = value['IMPRESSIONS']
	else:
		json_['IMPRESSIONS'] = float(value['IMPRESSIONS'])
	json_['REACH'] = None
	json_['FREQUENCY'] = None

	if (value['CLIKE'] is None):
		json_['CLIKE'] = value['CLIKE']
	else:
		json_['CLIKE'] = float(value['CLIKE'])
	json_['CLICKS_ALL'] = None
	json_['LINK_CLICKS'] = None
	if (value['CVIEWS'] is None):
		json_['CVIEWS'] = value['CVIEWS']
	else:
		json_['CVIEWS'] = float(value['CVIEWS'])
	json_['C3S_VIDEO_VIEW'] = None

	if (value['INSTALL'] is None):		
		json_['INSTALL'] = value['INSTALL']
	else:
		json_['INSTALL'] = float(value['INSTALL'])
	if (value['NRU'] is None):
		json_['NRU'] = value['NRU']
	else:
		json_['NRU'] = float(value['NRU'])
	json_['EFORM_TYPE'] = value['FORM_TYPE']
	json_['UNIT_OPTION'] = value['UNIT_OPTION']
	if ('Advertising Channel' in value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']):
		json_['OBJECTIVE'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['Advertising Channel']
	else:
		json_['OBJECTIVE'] = ''

	json_['EVENT_ID'] = value['REASON_CODE_ORACLE']
	json_['PRODUCT_ID'] = value['PRODUCT']
	if 'CCD_NRU' in value['MONTHLY'][index]:
		json_['CCD_NRU'] = value['MONTHLY'][index]['CCD_NRU']
	else:
		json_['CCD_NRU'] = None
	json_['GG_VIEWS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['VIEWS']
	json_['GG_CONVERSION'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['CONVERSIONS']

	json_['GG_INVALID_CLICKS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['INVALID_CLICKS']
	json_['GG_ENGAGEMENTS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['ENGAGEMENTS']
	json_['GG_VIDEO_VIEW'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['VIEWS']
	json_['GG_CTR'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['CTR']
	json_['GG_IMPRESSIONS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['IMPRESSIONS']

	json_['GG_INTERACTIONS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS']
	json_['GG_CLICKS'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['CLICKS']
	json_['GG_INTERACTION_TYPE'] = ''
	json_['GG_COST'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['COST']
	json_['GG_SPEND'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['COST']

	json_['GG_APPSFLYER_INSTALL'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP']
	if ('Bid Strategy Type' in value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']):
		json_['GG_STRATEGY_BID_TYPE'] = value['MONTHLY'][index]['TOTAL_CAMPAIGN_MONTHLY']['Bid Strategy Type']
	else:
		json_['GG_STRATEGY_BID_TYPE'] = ''
	
	return json_

#=================..........=====================
def ConvertJsonMonthlyDetailUnMap_1(value):
	json_ = {}	

	json_['CYEAR'] = '20' + value['CYEAR']
	if (len(value['CMONTH']) == 1):
		json_['CMONTH'] = '0' + value['CMONTH']
	else:
		json_['CMONTH'] = value['CMONTH']

	json_['SNAPSHOT_DATE'] = json_['CYEAR'] + '-' + json_['CMONTH']
	json_['LEGAL'] = value['LEGAL']
	json_['DEPARTMENT'] = value['DEPARTMENT']

	json_['DEPARTMENT_NAME'] = value['DEPARTMENT_NAME'] 
	json_['PRODUCT'] = value['PRODUCT'] 
	json_['PRODUCT_NAME'] = ''
	json_['REASON_CODE_ORACLE'] = value['REASON_CODE_ORACLE'] 
	json_['EFORM_NO'] = value['EFORM_NO'] 

	json_['START_DATE'] = datetime.strptime(value['START_DAY'], '%Y-%m-%d')
	json_['END_DATE'] = datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d')
	json_['CHANNEL'] = value['CHANNEL'] 
	json_['UNIT_COST'] = value['UNIT_COST'] 
	if (value['AMOUNT_USD'] is None):
		json_['AMOUNT_USD'] = value['AMOUNT_USD']
	else:
		json_['AMOUNT_USD'] = float(value['AMOUNT_USD'])

	if (value['CVALUE'] is None):
		json_['CVALUE'] = value['CVALUE']
	else:
		json_['CVALUE'] = float(value['CVALUE'])
	if (value['ENGAGEMENT'] is None):
		json_['ENGAGEMENT'] = value['ENGAGEMENT']
	else:
		json_['ENGAGEMENT'] = float(value['ENGAGEMENT'])
	if (value['IMPRESSIONS'] is None):
		json_['IMPRESSIONS'] = value['IMPRESSIONS']
	else:
		json_['IMPRESSIONS'] = float(value['IMPRESSIONS'])
	json_['REACH'] = None
	json_['FREQUENCY'] = None

	if (value['CLIKE'] is None):
		json_['CLIKE'] = value['CLIKE']
	else:
		json_['CLIKE'] = float(value['CLIKE'])
	json_['CLICKS_ALL'] = None
	json_['LINK_CLICKS'] = None
	if (value['CVIEWS'] is None):
		json_['CVIEWS'] = value['CVIEWS']
	else:
		json_['CVIEWS'] = float(value['CVIEWS'])
	json_['C3S_VIDEO_VIEW'] = None

	if (value['INSTALL'] is None):		
		json_['INSTALL'] = value['INSTALL']
	else:
		json_['INSTALL'] = float(value['INSTALL'])
	if (value['NRU'] is None):
		json_['NRU'] = value['NRU']
	else:
		json_['NRU'] = float(value['NRU'])
	json_['EFORM_TYPE'] = value['FORM_TYPE']
	json_['UNIT_OPTION'] = value['UNIT_OPTION']
	json_['OBJECTIVE'] = ''

	json_['EVENT_ID'] = value['REASON_CODE_ORACLE']
	json_['PRODUCT_ID'] = value['PRODUCT']
	if 'CCD_NRU' in value:
		json_['CCD_NRU'] = value['CCD_NRU']
	else:
		json_['CCD_NRU'] = None
	json_['GG_VIEWS'] = None
	json_['GG_CONVERSION'] = None

	json_['GG_INVALID_CLICKS'] = None
	json_['GG_ENGAGEMENTS'] = None
	json_['GG_VIDEO_VIEW'] = None
	json_['GG_CTR'] = None
	json_['GG_IMPRESSIONS'] = None

	json_['GG_INTERACTIONS'] = None
	json_['GG_CLICKS'] = None
	json_['GG_INTERACTION_TYPE'] = None
	json_['GG_COST'] = None
	json_['GG_SPEND'] = None

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = None

	return json_
#=================..........=====================



#=================..........=====================
def ConvertJsonMonthlyDetailUnMap_2(index, value):
	json_ = {}	

	json_['CYEAR'] = '20' + value['CYEAR']
	# if (len(value['CMONTH']) == 1):
	# 	json_['CMONTH'] = '0' + value['CMONTH']
	# else:
	# 	json_['CMONTH'] = value['CMONTH']

	if (len(str(value['MONTHLY'][index]['MONTH'])) == 1):
		json_['SNAPSHOT_DATE'] = json_['CYEAR'] + '-0' + str(value['MONTHLY'][index]['MONTH'])
		json_['CMONTH'] = '0' + str(value['MONTHLY'][index]['MONTH'])
	else:
		json_['SNAPSHOT_DATE'] = json_['CYEAR'] + '-' + str(value['MONTHLY'][index]['MONTH'])
		json_['CMONTH'] = str(value['MONTHLY'][index]['MONTH'])
	json_['LEGAL'] = value['LEGAL']
	json_['DEPARTMENT'] = value['DEPARTMENT']

	json_['DEPARTMENT_NAME'] = value['DEPARTMENT_NAME'] 
	json_['PRODUCT'] = value['PRODUCT'] 
	json_['PRODUCT_NAME'] = ''
	json_['REASON_CODE_ORACLE'] = value['REASON_CODE_ORACLE'] 
	json_['EFORM_NO'] = value['EFORM_NO'] 

	json_['START_DATE'] = datetime.strptime(value['START_DAY'], '%Y-%m-%d')
	json_['END_DATE'] = datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d')
	json_['CHANNEL'] = value['CHANNEL'] 
	json_['UNIT_COST'] = value['UNIT_COST'] 
	if (value['AMOUNT_USD'] is None):
		json_['AMOUNT_USD'] = value['AMOUNT_USD']
	else:
		json_['AMOUNT_USD'] = float(value['AMOUNT_USD'])

	if (value['CVALUE'] is None):
		json_['CVALUE'] = value['CVALUE']
	else:
		json_['CVALUE'] = float(value['CVALUE'])
	if (value['ENGAGEMENT'] is None):
		json_['ENGAGEMENT'] = value['ENGAGEMENT']
	else:
		json_['ENGAGEMENT'] = float(value['ENGAGEMENT'])
	if (value['IMPRESSIONS'] is None):
		json_['IMPRESSIONS'] = value['IMPRESSIONS']
	else:
		json_['IMPRESSIONS'] = float(value['IMPRESSIONS'])
	json_['REACH'] = None
	json_['FREQUENCY'] = None

	if (value['CLIKE'] is None):
		json_['CLIKE'] = value['CLIKE']
	else:
		json_['CLIKE'] = float(value['CLIKE'])
	json_['CLICKS_ALL'] = None
	json_['LINK_CLICKS'] = None
	if (value['CVIEWS'] is None):
		json_['CVIEWS'] = value['CVIEWS']
	else:
		json_['CVIEWS'] = float(value['CVIEWS'])
	json_['C3S_VIDEO_VIEW'] = None

	if (value['INSTALL'] is None):		
		json_['INSTALL'] = value['INSTALL']
	else:
		json_['INSTALL'] = float(value['INSTALL'])
	if (value['NRU'] is None):
		json_['NRU'] = value['NRU']
	else:
		json_['NRU'] = float(value['NRU'])
	json_['EFORM_TYPE'] = value['FORM_TYPE']
	json_['UNIT_OPTION'] = value['UNIT_OPTION']
	json_['OBJECTIVE'] = ''

	json_['EVENT_ID'] = value['REASON_CODE_ORACLE']
	json_['PRODUCT_ID'] = value['PRODUCT']
	if 'CCD_NRU' in value:
		json_['CCD_NRU'] = value['CCD_NRU']
	else:
		json_['CCD_NRU'] = None
	json_['GG_VIEWS'] = None
	json_['GG_CONVERSION'] = None

	json_['GG_INVALID_CLICKS'] = None
	json_['GG_ENGAGEMENTS'] = None
	json_['GG_VIDEO_VIEW'] = None
	json_['GG_CTR'] = None
	json_['GG_IMPRESSIONS'] = None

	json_['GG_INTERACTIONS'] = None
	json_['GG_CLICKS'] = None
	json_['GG_INTERACTION_TYPE'] = None
	json_['GG_COST'] = None
	json_['GG_SPEND'] = None

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = None

	return json_
#=================..........=====================


def ReportMonthlyDetail(path_data, connect):
	if os.path.exists(path_data):
	 	# ==================== Connect database =======================
		conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
		cursor = conn.cursor()

		# =================== Delete table ==================
		import time
		start = time.time()
		statement = 'delete from DTM_GG_PIVOT_DETAIL'	
		cursor.execute(statement)
		print ("\tTime delete: ", (time.time() - start))

		#=================== Read data from file json ===============================
		with open(path_data, 'r') as fi:
			data = json.load(fi)

		for value in data:
			for i in range(len(value['MONTHLY'])):			
				json_ = ConvertJsonMonthlyDetail(i, value)
				InsertMonthlyDetail(json_, cursor)

		conn.commit()
		cursor.close()


def InsertMonthlyDetailToDatabase(path_data, connect, list_plan_insert, list_plan_update, date):
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	ReportMonthlyDetail(path_data_total_map, connect)