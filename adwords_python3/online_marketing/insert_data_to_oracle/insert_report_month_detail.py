import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
	

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
		value['START_DATE'], value['END_DATE'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['AMOUNT_USD'], \
		value['CVALUE'], value['ENGAGEMENT'], value['IMPRESSIONS'], value['REACH'], value['FREQUENCY'], \
		value['CLIKE'], value['CLICKS_ALL'], value['LINK_CLICKS'], value['CVIEWS'], value['C3S_VIDEO_VIEW'], \
		value['INSTALL'], value['NRU'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['OBJECTIVE'], \
		value['EVENT_ID'], value['PRODUCT_ID'], value['CCD_NRU'], value['GG_VIEWS'], value['GG_CONVERSION'], \
		value['GG_INVALID_CLICKS'], value['GG_ENGAGEMENTS'], value['GG_VIDEO_VIEW'], value['GG_CTR'], value['GG_IMPRESSIONS'], \
		value['GG_INTERACTIONS'], value['GG_CLICKS'], value['GG_INTERACTION_TYPE'], value['GG_COST'], value['GG_SPEND'], \
		value['GG_APPSFLYER_INSTALL'], value['GG_STRATEGY_BID_TYPE']))	
	
	print("A row inserted!.......")


def ConvertJsonMonthlyDetail(index, value):
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
	json_['EFORM_TYPE'] = value['FORM_TYPE'] 
	json_['UNIT_OPTION'] = value['UNIT_OPTION'] 
	json_['AMOUNT_USD'] = float(value['AMOUNT_USD'])

	json_['CLIKE'] = value['CLIKE']
	json_['CLICKS_ALL'] = None
	json_['LINK_CLICKS'] = None
	json_['CVIEWS'] = value['CVIEWS']
	json_['C3S_VIDEO_VIEW'] = None

	json_['INSTALL'] = value['INSTALL']
	json_['NRU'] = value['NRU']
	json_['EFORM_TYPE'] = value['FORM_TYPE']
	json_['UNIT_OPTION'] = value['UNIT_OPTION']
	json_['OBJECTIVE'] = None

	json_['EVENT_ID'] = value['REASON_CODE_ORACLE']
	json_['PRODUCT_ID'] = value['PRODUCT']
	json_['CCD_NRU'] = None
	json_['GG_VIEWS'] = value['MONTHLY'][index]['DATA_MONTHLY']['VIEWS']
	json_['GG_CONVERSION'] = value['MONTHLY'][index]['DATA_MONTHLY']['CONVERSIONS']

	json_['GG_INVALID_CLICKS'] = value['MONTHLY'][index]['DATA_MONTHLY']['INVALID_CLICKS']
	json_['GG_ENGAGEMENTS'] = value['MONTHLY'][index]['DATA_MONTHLY']['ENGAGEMENTS']
	json_['GG_VIDEO_VIEW'] = value['MONTHLY'][index]['DATA_MONTHLY']['VIEWS']
	json_['GG_CTR'] = value['MONTHLY'][index]['DATA_MONTHLY']['CTR']
	json_['GG_IMPRESSIONS'] = value['MONTHLY'][index]['DATA_MONTHLY']['IMPRESSIONS']

	json_['GG_INTERACTIONS'] = value['MONTHLY'][index]['DATA_MONTHLY']['INTERACTIONS']
	json_['GG_CLICKS'] = value['MONTHLY'][index]['DATA_MONTHLY']['CLICKS']
	json_['GG_INTERACTION_TYPE'] = None
	json_['GG_COST'] = value['MONTHLY'][index]['DATA_MONTHLY']['COST']
	json_['GG_SPEND'] = value['MONTHLY'][index]['DATA_MONTHLY']['COST']

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = None

	return json_



def ReportMonthlyDetail(path_data, connect):

 	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#=================== Read data from file json ===============================
	with open(path_data, 'r') as fi:
		data = json.load(fi)

	for value in data['MONTHLY']:
		for i in range(len(value['MONTHLY'])):			
			json_ = ConvertJsonMonthlyDetail(i, value)
			InsertMonthlyDetail(json_, cursor)

	#==================== Commit and close connect ===============================
	conn.commit()
	print("Committed!.......")
	cursor.close()



path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/monthly3.json'
ReportMonthlyDetail(path_data, connect)