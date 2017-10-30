import json
import os
import cx_Oracle 
from datetime import datetime , timedelta, date
import manual_mapping_and_remap as manual
import mapping_campaign_plan as mapping
import insert_data_map_to_total as insert_to_total
import time



def ConvertPlan(plan):	
	json_ = {}

	json_['CYEAR'] = plan[0]
	json_['CMONTH'] = plan[1]
	json_['LEGAL'] = plan[2]
	json_['DEPARTMENT'] = plan[3]
	json_['DEPARTMENT_NAME'] = plan[4]

	json_['PRODUCT'] = plan[5]
	json_['REASON_CODE_ORACLE'] = plan[6]
	json_['EFORM_NO'] = plan[7]
	if (plan[8] is None):
		json_['START_DAY'] = plan[8]
	else:
		json_['START_DAY'] = plan[8].strftime('%Y-%m-%d')
	if (plan[9] is None):
		json_['END_DAY_ESTIMATE'] = plan[9]
	else:
		json_['END_DAY_ESTIMATE'] = plan[9].strftime('%Y-%m-%d')	

	json_['CHANNEL'] = plan[10]
	json_['FORM_TYPE'] = plan[11]
	json_['UNIT_OPTION'] = plan[12]
	json_['UNIT_COST'] = plan[13]
	json_['AMOUNT_USD'] = plan[14]

	json_['CVALUE'] = plan[15]
	json_['ENGAGEMENT'] = plan[16]
	json_['IMPRESSIONS'] = plan[17]
	json_['CLIKE'] = plan[18]
	json_['CVIEWS'] = plan[19]

	json_['INSTALL'] = plan[20]
	json_['NRU'] = plan[21]
	if (plan[22] is None):
		json_['INSERT_DATE'] = plan[22]
	else:
		json_['INSERT_DATE'] = plan[22].strftime('%Y-%m-%d')
	
	if (plan[23] is None):
		json_['REAL_START_DATE'] = plan[23]
	else:
		json_['REAL_START_DATE'] = plan[23].strftime('%Y-%m-%d')
	if (plan[24] is None):
		json_['REAL_END_DATE'] = plan[24]
	else:
		json_['REAL_END_DATE'] = plan[24].strftime('%Y-%m-%d')

	json_['STATUS'] = plan[25]
	if (plan[26] is None):
		json_['LAST_UPDATED_DATE'] = plan[26]
	else:
		json_['LAST_UPDATED_DATE'] = plan[26].strftime('%Y-%m-%d')

	return json_


def ConvertListPlan(list_plan):
	list_plan_json = []
	for plan in list_plan:
		json_ = ConvertPlan(plan)
		list_plan_json.append(json_)
	return list_plan_json


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
	
	for plan in list_plan_diff:
		print(plan)

	print(final_log) 
	return list_plan_diff, final_log



def GetFileTotal(path_data, date):	
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')		

	if not os.path.exists(path_data_total_map):
		i = 0
		find = True
		date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
		path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
		while not os.path.exists(path_data_total_map):
			i = i + 1
			date_before = date_before - timedelta(1)
			path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
			if i == 60:
				find = False
				break			
	else:
		find = True

	return path_data_total_map



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

def UpdateOnePlan(plan, updated_plan):
	plan['CYEAR'] = updated_plan['CYEAR']
	plan['CMONTH'] = updated_plan['CMONTH']
	plan['LEGAL'] = updated_plan['LEGAL']
	plan['DEPARTMENT'] = updated_plan['DEPARTMENT']
	plan['DEPARTMENT_NAME'] = updated_plan['DEPARTMENT_NAME']

	plan['PRODUCT'] = updated_plan['PRODUCT']
	plan['REASON_CODE_ORACLE'] = updated_plan['REASON_CODE_ORACLE']
	plan['EFORM_NO'] = updated_plan['EFORM_NO']
	plan['START_DAY'] = updated_plan['START_DAY']
	plan['END_DAY_ESTIMATE'] = updated_plan['END_DAY_ESTIMATE']
	
	plan['CHANNEL'] = updated_plan['CHANNEL']
	plan['FORM_TYPE'] = updated_plan['FORM_TYPE']
	plan['UNIT_OPTION'] = updated_plan['UNIT_OPTION']
	plan['UNIT_COST'] = updated_plan['UNIT_COST']
	plan['AMOUNT_USD'] = updated_plan['AMOUNT_USD']

	plan['CVALUE'] = updated_plan['CVALUE']
	plan['ENGAGEMENT'] = updated_plan['ENGAGEMENT']
	plan['IMPRESSIONS'] = updated_plan['IMPRESSIONS']
	plan['CLIKE'] = updated_plan['CLIKE']
	plan['CVIEWS'] = updated_plan['CVIEWS']

	plan['INSTALL'] = updated_plan['INSTALL']
	plan['NRU'] = updated_plan['NRU']
	plan['INSERT_DATE'] = updated_plan['INSERT_DATE']
	plan['REAL_START_DATE'] = updated_plan['REAL_START_DATE']
	plan['REAL_END_DATE'] = updated_plan['REAL_END_DATE']
	plan['LAST_UPDATED_DATE'] = updated_plan['LAST_UPDATED_DATE']


def UpdatePlan(path_data, list_plan_update):
	
	list_plan = list_plan_update.copy()	
	with open(path_data) as fi:
		data_total = json.load(fi)

	#=========== Update plan in DATA MAPPING ======================	
	for updated_plan in list_plan:
		for plan in data_total['MAP']:
			if updated_plan['PRODUCT'] == plan['PRODUCT'] \
			and updated_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
			and updated_plan['FORM_TYPE'] == plan['FORM_TYPE'] \
			and updated_plan['UNIT_OPTION'] == plan['UNIT_OPTION'] :
				UpdateOnePlan(data_total['MAP'][data_total['MAP'].index(plan)], updated_plan)
				list_plan.remove(updated_plan)


	#=========== Update plan in DATA TOTAL ======================	
	for updated_plan in list_plan:
		for plan in data_total['TOTAL']:
			if updated_plan['PRODUCT'] == plan['PRODUCT'] \
			and updated_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
			and updated_plan['FORM_TYPE'] == plan['FORM_TYPE'] \
			and updated_plan['UNIT_OPTION'] == plan['UNIT_OPTION'] :
				UpdateOnePlan(data_total['TOTAL'][data_total['TOTAL'].index(plan)], updated_plan)
				list_plan.remove(updated_plan)


	#=========== Update plan in UNMAP PLAN ======================	
	for updated_plan in list_plan:
		for plan in data_total['UN_PLAN']:
			if updated_plan['PRODUCT'] == plan['PRODUCT'] \
			and updated_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
			and updated_plan['FORM_TYPE'] == plan['FORM_TYPE'] \
			and updated_plan['UNIT_OPTION'] == plan['UNIT_OPTION'] :
				UpdateOnePlan(data_total['UN_PLAN'][data_total['UN_PLAN'].index(plan)], updated_plan)
				list_plan.remove(updated_plan)


	if (len(list_plan) == 0):
		print("Update complete...........")



# def NewPlan():





def ClassifyPlan(connect, path_data, date, path_log):
	# =============== Get plan change =====================	
	# fi = open(path_log, 'r')
	# final_log = fi.read()
	final_log = '10/27/2017 10:00:00'
	print(final_log)

	list_plan_diff, final_log = GetListPlanChangeFromTable(connect, final_log)

	fi = open(path_log, 'w') 
	fi.writelines(final_log)
	print("Save log ok..........")

	# ============== Classify plan diff ===================
	list_plan_new = []
	list_plan_map = []
	list_plan_update = []

	for plan in list_plan_diff:
		if plan[22] == plan[26]:
			list_plan_new.append(ConvertPlan(plan))
			print('new')
		else:
			# ========= Finally plan from data ==============
			file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')
			with open(file_plan, 'r') as fi:
				list_plan = json.load(fi)
			
			plan = ConvertPlan(plan)
			# print(plan)
			flag = CheckPlanUpdate(list_plan['plan'], plan)

			if flag:
				list_plan_update.append(plan)
			else:
				list_plan_map.append(plan)


	# ============= Process with each case =======================
	path_data_total = GetFileTotal(path_data, date)
	print(path_data_total)

	#======== Case 1: New Plan


	#======== Case 2: Data update can map



	#======== Case 3: Data update not map 
	if (len(list_plan_update) > 0):
		UpdatePlan(path_data_total, list_plan_update)
	print(list_plan_update)

	

	print('list_plan_new: ', len(list_plan_new))
	print('list_plan_map: ', len(list_plan_map))
	print('list_plan_update: ', len(list_plan_update))
	return list_plan_new, list_plan_map, list_plan_update

		








connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
# date = '2017-05-31' 
date = '2017-03-01' 
final_log = '10/27/2017 10:00:00'


# list_plan_diff, final_log = GetListPlanChangeFromTable(connect, final_log)

# path_log = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/log_plan_change.txt'
# fi = open(path_log, 'w') 
# fi.writelines(final_log)
# print("Save log ok..........")

path_log = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/log_plan_change.txt'
ClassifyPlan(connect, path_data, date, path_log)
# # list_plan_diff = GetListPlanChangeFromTable(cursor, final_log)
# list_plan_diff = GetListPlanChange(connect, path_data, date)
# list_data_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update, list_plan_insert = AutoMap(connect, path_data, date)




# ============== Test ham merger ================================
# path1 = 'C:/Users/CPU10912-local/Desktop/test1.json'
# with open(path1, 'r') as fi:
# 	data_map_all = json.load(fi)

# path2 = 'C:/Users/CPU10912-local/Desktop/test2.json'
# with open(path2, 'r') as fi:
# 	data_map_GS5 = json.load(fi)

# path3 = 'C:/Users/CPU10912-local/Desktop/test3.json'
# with open(path3, 'r') as fi:
# 	data_map_WPL = json.load(fi)

# list_plan, list_camp = merger_data_map(data_map_all, data_map_GS5, data_map_WPL)

# result = {}
# result['campaign'] = list_camp
# result['plan'] = list_plan

# path = 'C:/Users/CPU10912-local/Desktop/test4.json'
# with open(path, 'w') as fi:
# 	json.dump(result,fi)

# print('Save ok ................')
# # ================================================================


# path = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'



					







