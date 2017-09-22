import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
	

def InsertMonthlyDetail(path_data, connect):

 	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#===================== Read data from json ==========================
	with open(path_data, 'r') as fi:
		data = json.load(fi)

	for value in data['MONTHLY']:		
		for i in value:		 	
		 	if value[i] is None:
		 		value[i] = 0		 		

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
	
	
	for value in data['MONTHLY']:
		for i in range(len(value['MONTHLY'])):
			cursor.execute(statement, (str(value['CYEAR']) + '_' + str(value['MONTHLY'][i]['MONTH']), value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
				value['DEPARTMENT_NAME'], value['PRODUCT'], '', value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
				datetime.strptime(value['START_DAY'], '%Y-%m-%d'), datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d'), \
				value['CHANNEL'], value['UNIT_COST'], float(value['AMOUNT_USD']), \
				float(value['CVALUE']), float(value['ENGAGEMENT']), float(value['IMPRESSIONS']), 0, 0, \
				float(value['CLIKE']), 0, 0, float(value['CVIEWS']), 0, \
				float(value['INSTALL']), float(value['NRU']), value['FORM_TYPE'], value['UNIT_OPTION'], '', \
				'', value['PRODUCT'], 0,  float(value['MONTHLY'][i]['DATA_MONTHLY']['VIEWS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CONVERSIONS']), \
				float(value['MONTHLY'][i]['DATA_MONTHLY']['INVALID_CLICKS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['ENGAGEMENTS']), \
				float(value['MONTHLY'][i]['DATA_MONTHLY']['VIEWS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CTR']), float(value['MONTHLY'][i]['DATA_MONTHLY']['IMPRESSIONS']), \
				float(value['MONTHLY'][i]['DATA_MONTHLY']['INTERACTIONS']), float(value['MONTHLY'][i]['DATA_MONTHLY']['CLICKS']), \
				'', float(value['MONTHLY'][i]['DATA_MONTHLY']['COST']), float(value['MONTHLY'][i]['DATA_MONTHLY']['COST']), \
				0, ''))
	
	conn.commit()
	cursor.close()
	print("ok=====================================")



# def InsertMonthlySum(value, connect):

#  	# ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()

# 	#==================== Insert data into database =============================
# 	statement = 'insert into DTM_GG_PIVOT_DETAIL (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
# 	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
# 	START_DATE, END_DATE, EFORM_TYPE, UNIT_OPTION, NET_BUDGET_VND, \
# 	NET_BUDGET, UNIT_COST, VOLUMN, EVENT_ID, PRODUCT_ID	, \
# 	NET_ACTUAL, UNIT_COST_ACTUAL, VOLUMN_ACTUAL, APPSFLYER_INSTALL) \
# 	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, \
# 	:13, :14, :15, :16, :17, :18, :19, :20,	:21, :22, :23, :24)'	
		
# 	cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
# 		value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
# 		datetime.strptime(value['START_DATE'], '%Y-%m-%d'), datetime.strptime(value['END_DATE'], '%Y-%m-%d'), \
# 		value['EFORM_TYPE'], value['UNIT_OPTION'], float(value['NET_BUDGET_VND']), \
# 		value['NET_BUDGET'], str(value['UNIT_COST']), float(value['VOLUMN']), value['EVENT_ID'], value['PRODUCT_ID'], \
# 		float(value['NET_ACTUAL']), float(value['UNIT_COST_ACTUAL']), float(value['VOLUMN_ACTUAL']), float(value['APPSFLYER_INSTALL']))
	
# 	#==================== Commit and close connect ===============================
# 	conn.commit()
# 	cursor.close()
# 	print("A row inserted!.......")

# def 

# def ReportMonthSum(path_data, connect):
# 	#=================== Read data from file json ===============================
# 	with open(path_data, 'r') as fi:
# 		data = json.load(fi)

# 	for value in data['MONTHLY']:


# path_data = 'D:/WorkSpace/Adwords/Finanlly/AdWords/DATA/PLAN/monthly2.json'
path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/monthly2.json'
InsertMonthlyDetail(path_data, connect)









# # cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], \
# # 		value['DEPARTMENT'], value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], \
# # 		value['REASON_CODE_ORACLE'], value['EFORM_NO'], datetime.strptime(value['START_DATE'], '%Y-%m-%d'), \
# # 		datetime.strptime(value['END_DATE'], '%Y-%m-%d'), value['CHANNEL'], value['UNIT_COST'], \
# # 		float(value['AMOUNT_USD']), float(value['CVALUE']), float(value['ENGAGEMENT']), float(value['IMPRESSIONS']),\
# # 		float(value['REACH']), float(value['FREQUENCY']), float(value['CLIKE']), float(value['CLICKS_ALL']), \
# # 		float(value['LINK_CLICKS']), float(value['CVIEWS']), float(value['C3S_VIDEO_VIEW']), float(value['INSTALL']), \
# # 		float(value['NRU']), value['EFORM_TYPE'], value['UNIT_OPTION'], \
# # 		value['OBJECTIVE'], value['EVENT_ID'], value['PRODUCT_ID'], value['CCD_NRU'],\
# # 		float(value['GG_VIEWS']), float(value['GG_CONVERSION']), float(value['GG_INVALID_CLICKS']), \
# # 		float(value['GG_ENGAGEMENTS']), float(value['GG_VIDEO_VIEW']), float(value['GG_CTR']), \
# # 		float(value['GG_IMPRESSIONS']), float(value['GG_INTERACTIONS']), float(value['GG_CLICKS']), \
# # 		value['GG_INTERACTION_TYPE'], float(value['GG_COST']), float(value['GG_SPEND']), \
# # 		float(value['GG_APPSFLYER_INSTALL']), value['GG_STRATEGY_BID_TYPE']))


# conn = cx_Oracle.connect(connect)
# cursor = conn.cursor()

# statement = '''INSERT INTO DTM_GG_PIVOT_DETAIL (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
# 	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, START_DATE, END_DATE, \
# 	CHANNEL) \
# 	VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)'''

# value = {
# 	'SNAPSHOT_DATE': '2017-06', 
# 	'CYEAR': '2017', 	
# }

# cursor.execute(statement, ('', value['CYEAR']))

# conn.commit()
# cursor.close()
# print("ok=====================================")