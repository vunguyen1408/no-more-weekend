import json
import os
import cx_Oracle 


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
	return list_modified_plan



def GetListPlanChange(connect, path_data, date):
	#========== New plan from database =============
	list_modified_plan = ReadPlanFromTable(connect)

	# ========= Finally plan from data ==============
	file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')
	with open(file_plan, 'r') as fi:
		data = json.load(fi)

	list_plan_diff = []
	list_change = []
	list_change_date = []
	list_change_plancode = []
	list_update = []
	flag = False

	for plan in list_modified_plan:
		for value in data['plan']:
			#================ Not change plan code ================
			if (plan[6] == value['REASON_CODE_ORACLE']):
				flag = True
				#=========== Change Real start date, Read end date ===========
				if (plan[8] != value['START_DAY']) and (plan[9] != value['END_DAY_ESTIMATE']):
					list_change_date.append(plan)
					list_plan_diff.append(plan)
				elif (plan[6] != value['PRODUCT']) or (plan[11] != value['FORM_TYPE']) or (plan[12] != value['UNIT_OPTION']):
					list_change.append(plan)
					list_plan_diff.append(plan)
				elif (plan[1] != value['CMONTH']) or (plan[3] != value['DEPARTMENT']) or (plan[7] != value['EFORM_NO']) \
					or  (plan[13] != value['UNIT_COST']) or (plan[14] != value['AMOUNT_USD']):
					list_update.append(plan)

		if (flag == False):
			list_change_plancode.append(plan)
			list_plan_diff.append(plan)

	print(len(list_plan_diff))
	for plan in list_plan_diff:
		print(plan)
	return list_plan_diff


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
date = '2017-08-31' 
list_plan_diff = GetListPlanChange(connect, path_data, date)



					






