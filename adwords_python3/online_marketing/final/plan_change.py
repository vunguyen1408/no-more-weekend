import json
import os
import cx_Oracle 
from datetime import datetime , timedelta, date
import manual_mapping_and_remap as manual
import mapping_campaign_plan as mapping


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
			if (plan[23] is not None) and (plan[24] is not None):
				if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
				(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
				(plan[23].strftime('%Y-%m-%d') == value['REAL_START_DATE']) and (plan[24].strftime('%Y-%m-%d') == value['REAL_END_DATE']):
					if (plan in list_plan_diff):						
						list_plan_diff.remove(plan)	

			if (plan[23] is None) and (plan[24] is not None):
				if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
				(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
				(((plan[8].strftime('%Y-%m-%d') == value['START_DAY']) and (plan[9].strftime('%Y-%m-%d') == value['END_DAY_ESTIMATE'])) and \
				((plan[23] == value['REAL_START_DATE']) and (plan[24].strftime('%Y-%m-%d') == value['REAL_END_DATE']))) :
					if (plan in list_plan_diff):						
						list_plan_diff.remove(plan)

			if (plan[23] is not None) and (plan[24] is None):
				if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
				(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
				(((plan[8].strftime('%Y-%m-%d') == value['START_DAY']) and (plan[9].strftime('%Y-%m-%d') == value['END_DAY_ESTIMATE'])) and \
				((plan[23].strftime('%Y-%m-%d') == value['REAL_START_DATE']) and (plan[24] == value['REAL_END_DATE']))) :
					if (plan in list_plan_diff):						
						list_plan_diff.remove(plan)	

			if (plan[23] is None) and (plan[24] is None):
				if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
				(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
				(((plan[8].strftime('%Y-%m-%d') == value['START_DAY']) and (plan[9].strftime('%Y-%m-%d') == value['END_DAY_ESTIMATE'])) and \
				((plan[23] == value['REAL_START_DATE']) and (plan[24] == value['REAL_END_DATE']))) :
					if (plan in list_plan_diff):						
						list_plan_diff.remove(plan)		


	#========== Update new plan for file plan ===============	
	# mapping.ReadPlanFromTable(connect, path_data, str(date))
	# mapping.ReadProductAlias(connect, path_data, str(date))	
	#========================================================
	
	
	return list_plan_diff


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
	json_['START_DAY'] = plan[8]
	json_['END_DAY_ESTIMATE'] = plan[9]
	json_['CHANNEL'] = plan[10]
	json_['EFORM_TYPE'] = plan[11]
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
	json_['INSERT_DATE'] = plan[22]
	json_['REAL_START_DATE'] = plan[23]
	json_['REAL_END_DATE'] = plan[24]

	return json_


def ConvertListPlan(list_plan):
	list_plan_json = []
	for plan in list_plan:
		json_ = ConvertPlan(plan)
		list_plan_json.append(json_)
	return list_plan_json


	
def AutoMap(connect, path_data, date):
	# ------------- Get new plan or change plan ----------------	
	list_plan = GetListPlanChange(connect, path_data, date)
	list_plan = ConvertListPlan(list_plan)
	list_plan = mapping.AddProductCode(path_data, list_plan, date)
	print (len(list_plan))
	print(list_plan)
	# if len(list_plan) > 0:
	# ------------- Get campaign for mapping ----------------	
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	
	list_map_all = []
	list_plan_remove_unmap = []
	list_camp_remove_unmap = []
	list_plan_update = []
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

	if find:
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)

	list_full_camp = data_total['UN_CAMPAIGN']
	list_camp_all = []
	list_camp_GS5 = []
	list_camp_WPL = []
	for camp in list_full_camp:
		if (camp['Dept'] == 'GS5'):
			list_camp_GS5.append(camp)
		elif (camp['Dept'] == 'WPL'):
			list_camp_GS5.append(camp)
		else:
			list_camp_all.append(camp)

	print(len(list_full_camp))
	print(len(list_camp_all))
	print(len(list_camp_GS5))
	print(len(list_camp_WPL))


	#----------------- Mapping with campaign unmap -------------------------
	data_map_all = mapping.MapAccountWithCampaignAll(path_data, list_plan, list_camp_all, date)
	data_map_GS5 = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_GS5, date)
	data_map_WPL = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_WPL, date)

				
		# list_camp_remove_unmap = []
		# list_map_all = []
		# list_plan_remove_unmap = []
		# # print (len(data_total['UN_CAMPAIGN']))
		# for plan in list_plan:
		# 	plan, list_map, list_camp_need_remove = GetCampaignUnMapForPlan(plan, path_data_total_map)
		# 	# print ("--------------- gggg---------------")
		# 	# print (plan)
		# 	# print (list_camp_need_remove)
		# 	# print ("---------------gggg ---------------")
		# 	#------------- Insert data map ------------
		# 	data_total['MAP'].extend(list_map)
		# 	# print (len(list_camp_need_remove))

		# 	#----------- Remove unmap ---------------------
		# 	for camp in list_camp_need_remove:
		# 		for campaign in data_total['UN_CAMPAIGN']:
		# 			if camp['Campaign ID'] == campaign['Campaign ID'] \
		# 				and camp['Date'] == campaign['Date']:
		# 				data_total['UN_CAMPAIGN'].remove(campaign)
		# 				list_camp_remove_unmap.append(campaign)






connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-03-03' 
# list_plan_diff = GetListPlanChange(connect, path_data, date)
AutoMap(connect, path_data, date)



					







