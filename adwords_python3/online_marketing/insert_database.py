import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
	

# def InsertMonthlyDetail(path_data, connect):

#  	# ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()

# 	#===================== Read data from json ==========================
# 	with open(path_data, 'r') as fi:
# 		data = json.load(fi)

# 	for value in data['MONTHLY']:		
# 		for i in value:		 	
# 		 	if value[i] is None:
# 		 		value[i] = 0		 		

# 	#==================== Insert data into database =============================
# 	statement = 'insert into DTM_GG_PIVOT_DETAIL (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
# 	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
# 	START_DATE, END_DATE, CHANNEL, UNIT_COST, AMOUNT_USD, \
# 	CVALUE, ENGAGEMENT, IMPRESSIONS, REACH, FREQUENCY, \
# 	CLIKE, CLICKS_ALL, LINK_CLICKS, CVIEWS, C3S_VIDEO_VIEW, \
# 	INSTALL, NRU, EFORM_TYPE, UNIT_OPTION, OBJECTIVE, \
# 	EVENT_ID, PRODUCT_ID, CCD_NRU, GG_VIEWS, GG_CONVERSION, \
# 	GG_INVALID_CLICKS, GG_ENGAGEMENTS, GG_VIDEO_VIEW, GG_CTR, GG_IMPRESSIONS, \
# 	GG_INTERACTIONS, GG_CLICKS, GG_INTERACTION_TYPE, GG_COST, GG_SPEND, \
# 	GG_APPSFLYER_INSTALL, GG_STRATEGY_BID_TYPE) \
# 	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, \
# 	:21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, \
# 	:41, :42, :43, :44, :45, :46, :47)'
	
	
# 	for value in data['MONTHLY']:
# 		for i in range(len(value['MONTHLY'])):
# 			cursor.execute(statement, (str(value['CYEAR']) + '_' + str(value['MONTHLY'][i]['MONTH']), value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
# 				value['DEPARTMENT_NAME'], value['PRODUCT'], '', value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
# 				datetime.strptime(value['START_DAY'], '%Y-%m-%d'), datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d'), \
# 				value['CHANNEL'], value['UNIT_COST'], float(value['AMOUNT_USD']), \
# 				float(value['CVALUE']), float(value['ENGAGEMENT']), float(value['IMPRESSIONS']), 0, 0, \
# 				float(value['CLIKE']), 0, 0, float(value['CVIEWS']), 0, \
# 				float(value['INSTALL']), float(value['NRU']), value['FORM_TYPE'], value['UNIT_OPTION'], '', \
# 				'', value['PRODUCT'], 0,  float(value['MONTHLY'][i]['DATA_MONTHLY']['VIEWS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CONVERSIONS']), \
# 				float(value['MONTHLY'][i]['DATA_MONTHLY']['INVALID_CLICKS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['ENGAGEMENTS']), \
# 				float(value['MONTHLY'][i]['DATA_MONTHLY']['VIEWS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CTR']), float(value['MONTHLY'][i]['DATA_MONTHLY']['IMPRESSIONS']), \
# 				float(value['MONTHLY'][i]['DATA_MONTHLY']['INTERACTIONS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CLICKS']), \
# 				'', float(value['MONTHLY'][i]['DATA_MONTHLY']['COST']), float(value['MONTHLY'][i]['DATA_MONTHLY']['COST']), \
# 				0, ''))
	
# 	conn.commit()
# 	cursor.close()
# 	print("ok=====================================")



def InsertMonthlySum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_MONTH_SUM (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
	START_DATE, END_DATE, EFORM_TYPE, UNIT_OPTION, NET_BUDGET_VND, \
	NET_BUDGET, UNIT_COST, VOLUMN, EVENT_ID, PRODUCT_ID	, \
	NET_ACTUAL, UNIT_COST_ACTUAL, VOLUMN_ACTUAL, APPSFLYER_INSTALL) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, \
	:13, :14, :15, :16, :17, :18, :19, :20,	:21, :22, :23, :24)'	
		
	cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
		value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
		value['START_DATE'], value['END_DATE'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['NET_BUDGET_VND'], \
		value['NET_BUDGET'], value['UNIT_COST'], value['VOLUMN'], value['EVENT_ID'], value['PRODUCT_ID'], \
		value['NET_ACTUAL'], value['UNIT_COST_ACTUAL'], value['VOLUMN_ACTUAL'], value['APPSFLYER_INSTALL']))	
	
	print("A row inserted!.......")

def ConvertJsonMonthlySum(index, value):
	json_ = {}	
	json_['CYEAR'] = '20' + value['CYEAR']
	if (len(value['CMONTH']) == 1)
		json_['CMONTH'] = '0' + value['CMONTH']
	else:
		json_['CMONTH'] = value['CMONTH']
	json_['SNAPSHOT_DATE'] = '20' + value['CYEAR'] + '-' + value['CMONTH']	
	json_['LEGAL'] = value['LEGAL']
	json_['DEPARTMENT'] = value['DEPARTMENT']

	json_['DEPARTMENT_NAME'] = value['DEPARTMENT_NAME'] 
	json_['PRODUCT'] = value['PRODUCT'] 
	json_['PRODUCT_NAME'] = ''
	json_['REASON_CODE_ORACLE'] = value['REASON_CODE_ORACLE'] 
	json_['EFORM_NO'] = value['EFORM_NO'] 

	json_['START_DATE'] = datetime.strptime(value['START_DAY'], '%Y-%m-%d')
	json_['END_DATE'] = datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d')
	json_['EFORM_TYPE'] = value['FORM_TYPE'] 
	json_['UNIT_OPTION'] = value['UNIT_OPTION'] 
	json_['NET_BUDGET_VND'] = None

	json_['NET_BUDGET'] = float(value['AMOUNT_USD'])
	json_['UNIT_COST'] = str(value['UNIT_COST'])
	json_['VOLUMN'] = value['CVALUE'] 
	json_['EVENT_ID'] = value['REASON_CODE_ORACLE'] 
	json_['PRODUCT_ID'] = value['PRODUCT'] 

	json_['NET_ACTUAL'] = value['MONTHLY'][index]['DATA_MONTHLY']['COST']	 
	json_['VOLUMN_ACTUAL'] = value['MONTHLY'][index]['DATA_MONTHLY']['VOLUME_ACTUAL']
	json_['UNIT_COST_ACTUAL'] = float(json_['NET_ACTUAL']) / json_['VOLUMN_ACTUAL']
	json_['APPSFLYER_INSTALL'] = None

	return json_



def ReportMonthSum(path_data, connect):
 	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#=================== Read data from file json ===============================
	with open(path_data, 'r') as fi:
		data = json.load(fi)

	for value in data['MONTHLY']:
		for i in range(len(value['MONTHLY'])):
			json_ = ConvertJsonMonthlySum(i, value)
			InsertMonthlySum(json_, cursor)

	#==================== Commit and close connect ===============================
	conn.commit()
	print("Committed!.......")
	cursor.close()




def InsertPlanSum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_MONTH_SUM (CYEAR, CMONTH, LEGAL, DEPARTMENT, \
	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
	START_DATE, END_DATE, EFORM_TYPE, UNIT_OPTION, NET_BUDGET_VND, \
	NET_BUDGET, UNIT_COST, VOLUMN, EVENT_ID, PRODUCT_ID	, \
	NET_ACTUAL, UNIT_COST_ACTUAL, VOLUMN_ACTUAL, APPSFLYER_INSTALL) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, \
	:13, :14, :15, :16, :17, :18, :19, :20,	:21, :22, :23)'	
		
	cursor.execute(statement, (value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
		value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
		value['START_DATE'], value['END_DATE'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['NET_BUDGET_VND'], \
		value['NET_BUDGET'], value['UNIT_COST'], value['VOLUMN'], value['EVENT_ID'], value['PRODUCT_ID'], \
		value['NET_ACTUAL'], value['UNIT_COST_ACTUAL'], value['VOLUMN_ACTUAL'], value['APPSFLYER_INSTALL']))	
	
	print("A row inserted!.......")


def ConvertJsonPlanSum(value):
	json_ = {}	
	json_['CYEAR'] = '20' + value['CYEAR']
	json_['CMONTH'] = value['CMONTH']
	json_['LEGAL'] = value['LEGAL']
	json_['DEPARTMENT'] = value['DEPARTMENT']

	json_['DEPARTMENT_NAME'] = value['DEPARTMENT_NAME'] 
	json_['PRODUCT'] = value['PRODUCT'] 
	json_['PRODUCT_NAME'] = ''
	json_['REASON_CODE_ORACLE'] = value['REASON_CODE_ORACLE'] 
	json_['EFORM_NO'] = value['EFORM_NO'] 

	json_['START_DATE'] = datetime.strptime(value['START_DAY'], '%Y-%m-%d')
	json_['END_DATE'] = datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d')
	json_['EFORM_TYPE'] = value['FORM_TYPE'] 
	json_['UNIT_OPTION'] = value['UNIT_OPTION'] 
	json_['NET_BUDGET_VND'] = None

	json_['NET_BUDGET'] = float(value['AMOUNT_USD'])
	json_['UNIT_COST'] = str(value['UNIT_COST'])
	json_['VOLUMN'] = value['CVALUE'] 
	json_['EVENT_ID'] = value['REASON_CODE_ORACLE'] 
	json_['PRODUCT_ID'] = value['PRODUCT'] 

	json_['NET_ACTUAL'] = value['DATA_MONTHLY']['COST']	 
	json_['VOLUMN_ACTUAL'] = value['DATA_MONTHLY']['VOLUME_ACTUAL']
	json_['UNIT_COST_ACTUAL'] = float(json_['NET_ACTUAL']) / json_['VOLUMN_ACTUAL']
	json_['APPSFLYER_INSTALL'] = None

	return json_

def ReportPlanSum(path_data, connect):
 	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#=================== Read data from file json ===============================
	with open(path_data, 'r') as fi:
		data = json.load(fi)

	for value in data['MONTHLY']:		
		json_ = ConvertJsonPlanSum(value)
		InsertPlanSum(json_, cursor)

	#==================== Commit and close connect ===============================
	conn.commit()
	print("Committed!.......")
	cursor.close()


# path_data = 'D:/WorkSpace/Adwords/Finanlly/AdWords/DATA/PLAN/monthly2.json'
# path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/monthly2.json'
# InsertMonthlyDetail(path_data, connect)


path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/monthly3.json'
ConvertJsonMonthlySum(path_data, connect)









