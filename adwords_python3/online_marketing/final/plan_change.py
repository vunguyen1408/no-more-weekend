import json
import os
import cx_Oracle 
from datetime import datetime , timedelta, date


def ReadPlanFromTable(connect):
	#============ Connect database ==================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	#============ Read Plan from Table ===============
	query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, PRODUCT, REASON_CODE_ORACLE, EFORM_NO, \
          START_DAY, END_DAY_ESTIMATE, CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, CVALUE, \
          ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, INSTALL, NRU, INSERT_DATE, REAL_START_DATE, REAL_END_DATE \
      from STG_FA_DATA_GG'

	cursor.execute(query)
	list_new_plan = cursor.fetchall()
	list_modified_plan = list(list_new_plan)
	cursor.close()
	for i in range(len(list_modified_plan)):
		list_modified_plan[i] = list(list_modified_plan[i])
	return list_modified_plan



def GetListPlanChange(connect, path_data, date):
	#========== New plan from database =============
	list_modified_plan = ReadPlanFromTable(connect)

	# ========= Finally plan from data ==============
	file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')
	with open(file_plan, 'r') as fi:
		data = json.load(fi)

	list_plan_diff = list_modified_plan.copy()
	list_update = []
	

	#=========== Check list plan change or new plan ================
	print(len(list_plan_diff))
	for plan in list_modified_plan:		
		for value in data['plan']:				
			if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
			(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
			(((plan[8].strftime('%Y-%m-%d') == value['START_DAY']) and (plan[9].strftime('%Y-%m-%d') == value['END_DAY_ESTIMATE'])) or \
			((plan[23].strftime('%Y-%m-%d') == value['REAL_START_DATE']) and (plan[24].strftime('%Y-%m-%d') == value['REAL_END_DATE']))) :
				if (plan in list_plan_diff):
					print("--------------", plan)
					list_plan_diff.remove(plan)	


	#========== Update new plan for file plan ===============	
	
	
	print('list_plan_diff: ', len(list_plan_diff))
	return list_plan_diff





connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
date = '2017-08-31' 
list_plan_diff = GetListPlanChange(connect, path_data, date)



					







