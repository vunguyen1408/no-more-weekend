import json
import os
import cx_Oracle 
import time
from datetime import datetime , timedelta, date

import mapping_campaign_plan as mapping
import insert_data_map_to_total as insert_to_total
# import insert_nru_into_data as nru
import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install


def GetListPlanChangeFromTable(connect, final_log):	
	#============ Connect database ==================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()
	
	#============ Read Plan from Table ===============

	query = "select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, \
					PRODUCT, REASON_CODE_ORACLE, EFORM_NO, START_DAY, END_DAY_ESTIMATE, \
					CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, \
					CVALUE, ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, \
					INSTALL, NRU, INSERT_DATE, REAL_START_DATE, REAL_END_DATE, \
          			STATUS, LAST_UPDATED_DATE\
      		from STG_FA_DATA_GG \
      		where LAST_UPDATED_DATE is not null \
      		and LAST_UPDATED_DATE >= to_timestamp('" + final_log + "', 'mm/dd/yyyy hh24:mi:ss')"

	
	cursor.execute(query) 
	final_log = datetime.now().strftime('%m/%d/%Y %H:%M:%S')	
	list_new_plan = cursor.fetchall()
	list_plan_diff = list(list_new_plan)
	cursor.close()

	for i in range(len(list_plan_diff)):
		list_plan_diff[i] = list(list_plan_diff[i])	

	print('list_plan_diff: ', len(list_plan_diff))
	
	return list_plan_diff, final_log


def CheckPlanUpdate(list_plan, plan):
	for _value in list_plan:
		# ========= Change product id =====================
		if _value['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] and \
		_value['PRODUCT'] == plan['PRODUCT'] and \
		_value['FORM_TYPE'] == plan['FORM_TYPE'] and \
		_value['UNIT_OPTION'] == plan['UNIT_OPTION'] and \
		_value['START_DAY'] == plan['START_DAY'] and \
		_value['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE'] and \
		_value['REAL_START_DATE'] == plan['REAL_START_DATE'] and \
		_value['REAL_END_DATE'] == plan['REAL_END_DATE'] :			
			return True
	
	return False


def CheckPlanUpdateRealDate(list_plan, plan):
	for _value in list_plan:
		# ========= Change product id =====================
		if _value['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] and \
		_value['PRODUCT'] == plan['PRODUCT'] and \
		_value['FORM_TYPE'] == plan['FORM_TYPE'] and \
		_value['UNIT_OPTION'] == plan['UNIT_OPTION'] and \
		_value['START_DAY'] == plan['START_DAY'] and \
		_value['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE'] and \
		(_value['REAL_START_DATE'] != plan['REAL_START_DATE'] or \
		_value['REAL_END_DATE'] != plan['REAL_END_DATE'] ):
			return True		

	return False


def GetPlanModified(connect, data_plan):
	#====================== Get old plan in python ==========================	
	list_new_plan = []
	list_plan =  data_plan['plan'].copy()
	
	#======================= Get new plan in database ========================	
	#==================== Connect database ==================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	#============ Read Plan from Table ===============
	query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, \
					PRODUCT, REASON_CODE_ORACLE, EFORM_NO, START_DAY, END_DAY_ESTIMATE, \
					CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, \
					CVALUE, ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, \
					INSTALL, NRU, INSERT_DATE, REAL_START_DATE, REAL_END_DATE, \
          			STATUS, LAST_UPDATED_DATE \
      		from STG_FA_DATA_GG'

	cursor.execute(query)
	new_plan = cursor.fetchall()
	list_modified_plan = list(new_plan)
	cursor.close()
	for i in range(len(list_modified_plan)):		
		list_new_plan.append(ConvertPlan(list(list_modified_plan[i])))
	
	for plan in data['plan']:
		for new_plan in list_new_plan:
			if (new_plan['PRODUCT'] == plan['PRODUCT']) \
				and (new_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE']) \
				and (new_plan['FORM_TYPE'] == plan['FORM_TYPE']) \
				and (new_plan['UNIT_OPTION'] == plan['UNIT_OPTION'])\
				and (new_plan['START_DAY'] == plan['START_DAY']) \
				and (new_plan['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE']):				
				list_plan.remove(plan)
						
	
	return list_plan

def GetListDiff(connect, path_data, date, path_log = None):
	# fi = open(path_log, 'r')
	# final_log = fi.read()
	final_log = '11/06/2017 03:46:00'
	
	list_plan_diff, final_log = GetListPlanChangeFromTable(connect, final_log)

	fi = open(path_log, 'w') 
	fi.writelines(final_log)
	print("Save log ok..........")


	file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')			
	with open(file_plan, 'r') as fi:
		list_plan = json.load(fi)		

	# ============== Classify plan diff ===================
	list_plan_new = []	
	list_plan_change_real_date = []
	list_plan_update = []

	for _plan in list_plan_diff:			
		if CheckPlanUpdate(list_plan['plan'], plan):
			list_plan_update.append(plan)
		else:			
			if CheckPlanUpdateRealDate(list_plan['plan'], plan):
				list_plan_change_real_date.append(plan)
			else:
				list_plan_new.append(plan)

	# ============ List plan modified ================
	list_plan_modified = GetPlanModified(connect, list_plan)

	print('list_diff: ', len(list_plan_diff))
	print('list_plan_new: ', len(list_plan_new))
	print('list_plan_only_update: ', len(list_plan_update))	
	print('list_plan_change_real: ', len(list_plan_change_real_date))
	print('list_plan_modified: ', len(list_plan_modified))

	return list_plan_diff, list_plan_new, list_plan_change_real_date, list_plan_update, list_plan_modified 


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-10-31' 
path_log = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/log_plan_change.txt'

list_plan_diff, list_plan_new, list_plan_change_real_date, \
list_plan_update, list_plan_modified = GetListDiff(connect, path_data, date, path_log)

