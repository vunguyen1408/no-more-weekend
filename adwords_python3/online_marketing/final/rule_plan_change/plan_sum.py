import cx_Oracle
import json
import os
from datetime import datetime , timedelta, date



def InsertPlanSum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_PLAN_SUM (CYEAR, CMONTH, LEGAL, DEPARTMENT, \
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
	
	# print("   A row inserted!.......")


def UpdatePlanPlanSum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'update DTM_GG_PLAN_SUM \
	set CYEAR = :1, CMONTH = :2, LEGAL = :3, DEPARTMENT = :4, \
	DEPARTMENT_NAME = :5, PRODUCT_NAME = :6, EFORM_NO = :7, \
	START_DATE = :8, END_DATE = :9, NET_BUDGET_VND = :10, \
	NET_BUDGET = :11, UNIT_COST = :12, VOLUMN = :13, EVENT_ID = :14, PRODUCT_ID = :15 \
	where PRODUCT = :16 and REASON_CODE_ORACLE = :17 and EFORM_TYPE = :18 \
	and UNIT_OPTION = :19'
	
		
	cursor.execute(statement, (value['CYEAR'], value['CMONTH'], value['LEGAL'], value['DEPARTMENT'], \
		value['DEPARTMENT_NAME'], value['PRODUCT_NAME'], value['EFORM_NO'], \
		value['START_DATE'], value['END_DATE'], value['NET_BUDGET_VND'], \
		value['NET_BUDGET'], value['UNIT_COST'], value['VOLUMN'], value['EVENT_ID'], value['PRODUCT_ID'],\
		value['PRODUCT'], value['REASON_CODE_ORACLE'], value['EFORM_TYPE'], value['UNIT_OPTION']))


def UpdatePlanSum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'update DTM_GG_PLAN_SUM \
	set NET_ACTUAL = :1, UNIT_COST_ACTUAL = :2, \
	VOLUMN_ACTUAL = :3, APPSFLYER_INSTALL = :4 \
	where PRODUCT = :5 and REASON_CODE_ORACLE = :6 and EFORM_TYPE = :7 and UNIT_OPTION = :8'
	
		
	cursor.execute(statement, (value['NET_ACTUAL'], value['UNIT_COST_ACTUAL'], \
		value['VOLUMN_ACTUAL'], value['APPSFLYER_INSTALL'], \
		value['PRODUCT'], value['REASON_CODE_ORACLE'], value['EFORM_TYPE'], value['UNIT_OPTION']))

	# print("   A row updated!.......")

def DeletePlanSum(value, cursor):
	#==================== Remove plan from database =============================
	statement = 'delete from DTM_GG_PLAN_SUM \
	where PRODUCT = :1 and REASON_CODE_ORACLE = :2 and EFORM_TYPE = :3 and UNIT_OPTION = :4'
		
	cursor.execute(statement, (value['PRODUCT'], value['REASON_CODE_ORACLE'], value['FORM_TYPE'], value['UNIT_OPTION']))	
	
	

def MergerPlanSum(value, cursor):
	#==================== Insert data into database =============================
	statement = 'select * from DTM_GG_PLAN_SUM \
	where PRODUCT = :1 and REASON_CODE_ORACLE = :2 and EFORM_TYPE = :3 and UNIT_OPTION = :4'	
		
	cursor.execute(statement, (value['PRODUCT'], value['REASON_CODE_ORACLE'], value['EFORM_TYPE'], value['UNIT_OPTION']))
	res = list(cursor.fetchall())
	
	if (len(res) == 0):
		InsertPlanSum(value, cursor)
	else:
		UpdatePlanSum(value, cursor)
	# print("A row mergered!.......")


def ConvertJsonPlanSum(value):
	json_ = {}	
	json_['CYEAR'] = '20' + value['CYEAR']
	if (len(value['CMONTH']) == 1):
		json_['CMONTH'] = '0' + value['CMONTH']
	else:
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

	if (value['AMOUNT_USD'] is None):
		json_['NET_BUDGET'] =  value['AMOUNT_USD']
	else:
		json_['NET_BUDGET'] =  float(value['AMOUNT_USD'])	

	if (value['UNIT_COST'] is None):
		json_['UNIT_COST'] =   value['UNIT_COST']
	else:
		json_['UNIT_COST'] =  str(value['UNIT_COST'])
	json_['VOLUMN'] = value['CVALUE'] 
	json_['EVENT_ID'] = value['REASON_CODE_ORACLE'] 
	json_['PRODUCT_ID'] = value['PRODUCT'] 

	json_['NET_ACTUAL'] = value['TOTAL_CAMPAIGN']['COST']	 
	json_['VOLUMN_ACTUAL'] = value['TOTAL_CAMPAIGN']['VOLUME_ACTUAL']
	if (json_['VOLUMN_ACTUAL'] == 0):
		json_['UNIT_COST_ACTUAL'] = None
	else:
		json_['UNIT_COST_ACTUAL'] = float(json_['NET_ACTUAL']) / json_['VOLUMN_ACTUAL']
	json_['APPSFLYER_INSTALL'] = value['TOTAL_CAMPAIGN']['INSTALL_CAMP']

	return json_


#=================..........=====================
def ConvertJsonPlanSumUnMap(value):
	json_ = {}	
	json_['CYEAR'] = '20' + value['CYEAR']
	if (len(value['CMONTH']) == 1):
		json_['CMONTH'] = '0' + value['CMONTH']
	else:
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

	if (value['AMOUNT_USD'] is None):
		json_['NET_BUDGET'] =  value['AMOUNT_USD']
	else:
		json_['NET_BUDGET'] =  float(value['AMOUNT_USD'])	

	if (value['UNIT_COST'] is None):
		json_['UNIT_COST'] =   value['UNIT_COST']
	else:
		json_['UNIT_COST'] =  str(value['UNIT_COST'])
	json_['VOLUMN'] = value['CVALUE'] 
	json_['EVENT_ID'] = value['REASON_CODE_ORACLE'] 
	json_['PRODUCT_ID'] = value['PRODUCT'] 

	json_['NET_ACTUAL'] = None
	json_['VOLUMN_ACTUAL'] = None	
	json_['UNIT_COST_ACTUAL'] = None	
	json_['APPSFLYER_INSTALL'] = None

	return json_
#=================..........=====================



def ReportPlanSum(path_data, connect):
	if os.path.exists(path_data):
	 	# ==================== Connect database =======================
		conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
		cursor = conn.cursor()

		#=================== Read data from file json ===============================
		with open(path_data, 'r') as fi:
			data = json.load(fi)

		for value in data['TOTAL']:		
			json_ = ConvertJsonPlanSum(value)
			MergerPlanSum(json_, cursor)


		#=================..........=====================
		for value in data['UN_PLAN']:		
			json_ = ConvertJsonPlanSumUnMap(value)
			MergerPlanSum(json_, cursor)
		#=================..........=====================

		#==================== Commit and close connect ===============================
		conn.commit()
		# print("Committed!.......")
		cursor.close()


def InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date):
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	ReportPlanSum(path_data_total_map, connect)



# path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/insert_data_to_oracle/total_mapping1.json'
# ReportPlanSum(path_data, connect)