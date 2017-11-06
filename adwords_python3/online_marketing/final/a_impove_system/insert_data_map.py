import cx_Oracle
import json
import pandas as pd
import os
from datetime import datetime

def InsertDetailUnmap(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_PIVOT_DETAIL_UNMAP ( \
	SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, \
	START_DATE, END_DATE, CHANNEL, UNIT_COST, AMOUNT_USD, \
	CVALUE, ENGAGEMENT, IMPRESSIONS, REACH, FREQUENCY, \
	CLIKE, CLICKS_ALL, LINK_CLICKS, CVIEWS, C3S_VIDEO_VIEW, \
	INSTALL, NRU, EFORM_TYPE, UNIT_OPTION, OBJECTIVE, \
	EVENT_ID, PRODUCT_ID, CCD_NRU, GG_CONVERSION, \
	GG_INVALID_CLICKS, GG_ENGAGEMENTS, GG_VIDEO_VIEW, GG_CTR, GG_IMPRESSIONS, \
	GG_INTERACTIONS, GG_CLICKS, GG_CAMPAIGN_TYPE, GG_SPEND, \
	GG_APPSFLYER_INSTALL, GG_STRATEGY_BID_TYPE, CAMPAIGN_ID, CAMPAIGN_NAME, UPDATE_DATE, \
	GG_MCC_ID, GG_MCC_NAME) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, \
	:21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, \
	:41, :42, :43, :44, :45, :46, :47, :48, :49, :50)'	
		
	cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
		value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], value['REASON_CODE_ORACLE'], value['EFORM_NO'], \
		value['START_DATE'], value['END_DATE'], value['CHANNEL'], value['UNIT_COST'], value['AMOUNT_USD'], \
		value['CVALUE'], value['ENGAGEMENT'], value['IMPRESSIONS'], value['REACH'], value['FREQUENCY'], \
		value['CLIKE'], value['CLICKS_ALL'], value['LINK_CLICKS'], value['CVIEWS'], value['C3S_VIDEO_VIEW'], \
		value['INSTALL'], value['NRU'], value['EFORM_TYPE'], value['UNIT_OPTION'], value['OBJECTIVE'], \
		value['EVENT_ID'], value['PRODUCT_ID'], value['CCD_NRU'], value['GG_CONVERSION'], \
		value['GG_INVALID_CLICKS'], value['GG_ENGAGEMENTS'], value['GG_VIDEO_VIEW'], value['GG_CTR'], value['GG_IMPRESSIONS'], \
		value['GG_INTERACTIONS'], value['GG_CLICKS'], value['GG_CAMPAIGN_TYPE'], value['GG_SPEND'], \
		value['GG_APPSFLYER_INSTALL'], value['GG_STRATEGY_BID_TYPE'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'], value['UPDATE_DATE'], \
		value['GG_MCC_ID'], value['GG_MCC_NAME']))	
	
	# print("A row inserted!.......")


def SelectDetailUnmap(cursor):
	#==================== Insert data into database =============================
	statement = 'Select SNAPSHOT_DATE, CAMPAIGN_ID, \
	PRODUCT, REASON_CODE_ORACLE, EFORM_TYPE, UNIT_OPTION \
	from DTM_GG_PIVOT_DETAIL_UNMAP'
		
	cursor.execute(statement)	
	list_unmap = list(cursor.fetchall())
	
	return list_unmap
	


def ConvertJsonPlan(value):
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
	json_['GG_CONVERSION'] = None
	json_['GG_INVALID_CLICKS'] = None
	json_['GG_ENGAGEMENTS'] = None
	json_['GG_VIDEO_VIEW'] = None
	json_['GG_CTR'] = None
	json_['GG_IMPRESSIONS'] = None

	json_['GG_INTERACTIONS'] = None
	json_['GG_CLICKS'] = None
	json_['GG_CAMPAIGN_TYPE'] = ''	
	json_['GG_SPEND'] = None

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = ''
	json_['CAMPAIGN_ID'] = None
	json_['CAMPAIGN_NAME'] = None
	json_['UPDATE_DATE'] = None
	json_['GG_MCC_ID'] = None
	json_['GG_MCC_NAME'] = None

	return json_

def getProductID(value):
	file_product = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/insert_data_to_oracle/product.xlsx'
	# file_product = 'C:/Users/CPU10912-local/Desktop/product.xlsx'
	product = pd.read_excel(file_product)
	list_pro_code = list(product['Product'])
	list_pro_id = list(product['Product ID'])
	product_code = value['Campaign'].split(' | ')[0]
	for i in range(len(list_pro_code)):
		if (product_code == list_pro_code[i]):
			return list_pro_id[i]
	

def ConvertJsonCamp(value):
	json_ = {}	

	json_['SNAPSHOT_DATE'] = value['Date']
	json_['CYEAR'] = value['Date'][0:4]
	json_['CMONTH'] = value['Date'][5:7]
	
	json_['LEGAL'] = None
	json_['DEPARTMENT'] = None

	json_['DEPARTMENT_NAME'] = None
	json_['PRODUCT'] = None
	json_['PRODUCT_NAME'] = None
	json_['REASON_CODE_ORACLE'] = None
	json_['EFORM_NO'] = None

	json_['START_DATE'] = None
	json_['END_DATE'] = None
	json_['CHANNEL'] = None
	json_['UNIT_COST'] = None
	json_['AMOUNT_USD'] = None

	json_['CVALUE'] = None
	json_['ENGAGEMENT'] = None
	json_['IMPRESSIONS'] = None
	json_['REACH'] = None
	json_['FREQUENCY'] = None

	json_['CLIKE'] = None
	json_['CLICKS_ALL'] = None
	json_['LINK_CLICKS'] = None
	json_['CVIEWS'] = None
	json_['C3S_VIDEO_VIEW'] = None

	json_['INSTALL'] = None
	json_['NRU'] = None
	json_['EFORM_TYPE'] = None
	json_['UNIT_OPTION'] = None
	json_['OBJECTIVE'] = None

	json_['EVENT_ID'] = None
	json_['PRODUCT_ID'] = str(getProductID(value))
	json_['CCD_NRU'] = None	
	json_['GG_CONVERSION'] = value['Conversions']

	json_['GG_INVALID_CLICKS'] = value['Invalid clicks']
	json_['GG_ENGAGEMENTS'] = value['Engagements']
	json_['GG_VIDEO_VIEW'] = value['Views']
	json_['GG_CTR'] = value['CTR']
	json_['GG_IMPRESSIONS'] = value['Impressions']

	json_['GG_INTERACTIONS'] = value['Interactions']
	json_['GG_CLICKS'] = value['Clicks']
	json_['GG_CAMPAIGN_TYPE'] = value['Advertising Channel']	
	json_['GG_SPEND'] = value['Cost']

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = value['Bid Strategy Type']
	json_['CAMPAIGN_ID'] = str(value['Campaign ID'])
	json_['CAMPAIGN_NAME'] = value['Campaign']
	json_['UPDATE_DATE'] = datetime.strptime(value['Date'], '%Y-%m-%d')
	json_['GG_MCC_ID'] = value['Account ID']
	json_['GG_MCC_NAME'] = value['Account Name']

	return json_


def ConvertJsonMap(value):
	json_ = {}	
	# print (value)
	json_['SNAPSHOT_DATE'] = value['Date']
	json_['CYEAR'] = '20' + value['CYEAR']
	# if (len(value['CMONTH']) == 1):
	# 	json_['CMONTH'] = '0' + value['CMONTH']
	# else:
	# 	json_['CMONTH'] = value['CMONTH']	
	json_['CMONTH'] = value['Date'][5:7]
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
	json_['GG_CONVERSION'] = value['Conversions']

	json_['GG_INVALID_CLICKS'] = value['Invalid clicks']
	json_['GG_ENGAGEMENTS'] = value['Engagements']
	json_['GG_VIDEO_VIEW'] = value['Views']
	json_['GG_CTR'] = value['CTR']
	json_['GG_IMPRESSIONS'] = value['Impressions']

	json_['GG_INTERACTIONS'] = value['Interactions']
	json_['GG_CLICKS'] = value['Clicks']
	json_['GG_CAMPAIGN_TYPE'] = value['Advertising Channel']	
	json_['GG_SPEND'] = value['Cost']

	json_['GG_APPSFLYER_INSTALL'] = None
	json_['GG_STRATEGY_BID_TYPE'] = value['Bid Strategy Type']
	json_['CAMPAIGN_ID'] = str(value['Campaign ID'])
	json_['CAMPAIGN_NAME'] = value['Campaign']
	json_['UPDATE_DATE'] = datetime.strptime(value['Date'], '%Y-%m-%d')
	json_['GG_MCC_ID'] = value['Account ID']
	json_['GG_MCC_NAME'] = value['Account Name']

	return json_

def DeletePlan(value, cursor):
	#==================== Remove plan from database =============================
	statement = 'delete from DTM_GG_PIVOT_DETAIL_UNMAP \
	where PRODUCT = :1 and REASON_CODE_ORACLE = :2 and EFORM_TYPE = :3 and UNIT_OPTION = :4'
		
	cursor.execute(statement, (value['PRODUCT'], value['REASON_CODE_ORACLE'], value['FORM_TYPE'], value['UNIT_OPTION']))	
	
	# print("A plan deleted!.......")


def DeleteCamp(value, cursor):
	#==================== Remove campaign from database =============================

	statement = 'delete from DTM_GG_PIVOT_DETAIL_UNMAP \
	where CAMPAIGN_ID = :1 and SNAPSHOT_DATE = :2'
	
	value = ConvertJsonCamp(value)
	cursor.execute(statement, (value['CAMPAIGN_ID'], value['SNAPSHOT_DATE']))	
	
	# print("A campaign deleted!.......")



def DeleteListPlan(list_plan_remove, connect):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	#=================== Read data from file json ==================
	if (len(list_plan_remove) == 0):
		# print("List plan empty...")
		pass
	else:
		for plan in list_plan_remove:
			DeletePlan(plan, cursor)
		# print("Delete", len(list_plan_remove), "plan!.........")

	conn.commit()
	# print("Committed!.......")
	cursor.close()



def DeleteListCamp(list_camp_remove, connect):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	#=================== Read data from file json ==================
	if (len(list_camp_remove) == 0):
		# print("List campaign empty...")
		pass
	else:
		for camp in list_camp_remove:
			DeleteCamp(camp, cursor)
		# print("Delete", len(list_camp_remove), "plan!.........")

	conn.commit()
	# print("Committed!.......")
	cursor.close()


def CreateDataMap(data_total):
	list_map = []
	list_plan_un = []
	list_map_ = []
	for plan in data_total:
		if len(plan['CAMPAIGN']) > 0:
			for camp in plan['CAMPAIGN']:
				z = camp.copy()
				z.update(plan)
				# print (z)
				list_map_.append(z)
				json_ = ConvertJsonMap(z)
				list_map.append(json_)
		else:
			json_ = ConvertJsonPlan(plan)
			list_plan_un.append(json_)

	return (list_map, list_plan_un, list_map_)

def CreateDataUnMap(data_camp):
	list_un_camp = []
	for camp in data_camp:
		json_ = ConvertJsonCamp(camp)
		list_un_camp.append(json_)
	return list_un_camp



def InsertDataMap(path_data_total_map, path_data_un_map, connect):
	if os.path.exists(path_data_total_map):
	 	# ==================== Connect database =======================
		conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
		cursor = conn.cursor()
		# Open file total
		with open(path_data_total_map, 'r') as fi:
			data_total = json.load(fi)	

		# Open file un_map_camp
		with open(path_data_un_map, 'r') as fi:
			data_camp = json.load(fi)	

		list_map, list_plan_un, list_map_ = CreateDataMap(data_total)
		list_un_camp = CreateDataUnMap(data_camp)

		print ('Length data map:', len (list_map))
		print ('Length plan un:', len (list_plan_un))
		print ('Length camp un:', len (list_un_camp))
		

		import time
		start = time.time()
		statement = 'delete from DTM_GG_PIVOT_DETAIL_UNMAP'
		cursor.execute(statement)
		print ("Time delete unmap : ", (time.time() - start))
		start = time.time()
		for value in list_map:
			try:		
				InsertDetailUnmap(value, cursor)
			except:
				pass

		for value in list_plan_un:
			try:		
				InsertDetailUnmap(value, cursor)
			except:
				pass
		
		for value in list_un_camp:
			try:		
				InsertDetailUnmap(value, cursor)
			except:
				pass

		print ("Time insert unmap : ", (time.time() - start))

		conn.commit()
		print("Committed!.......")
		cursor.close()


def InsertDataMapToDatabase(path_data, connect, list_plan_insert, list_plan_update, date):
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
	InsertDataMap(path_data_total_map, path_data_un_map, connect)

# path_data = 'D:/WorkSpace/Adwords/Finanlly/AdWords/DATA/DATA_MAPPING/mapping_final.json'
# path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/insert_data_to_oracle/total_mapping1.json'
# ReportDetailUnmap(path_data, connect)

