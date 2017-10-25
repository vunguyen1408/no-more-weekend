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
			if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[23] is not None):
				print(plan[23].strftime('%Y-%m-%d'), value['REAL_START_DATE'])			
			if (plan[6] == value['REASON_CODE_ORACLE']) and (plan[5] == value['PRODUCT']) and \
			(plan[11] == value['FORM_TYPE']) and (plan[12] == value['UNIT_OPTION']) and \
			(((plan[8].strftime('%Y-%m-%d') == value['START_DAY']) and (plan[9].strftime('%Y-%m-%d') == value['END_DAY_ESTIMATE'])) or \
			((plan[23].strftime('%Y-%m-%d') == value['REAL_START_DATE']) and (plan[24].strftime('%Y-%m-%d') == value['REAL_END_DATE']))) :
				if (plan in list_plan_diff):
					# print("--------------", plan)
					list_plan_diff.remove(plan)	


	#========== Update new plan for file plan ===============	
	# mapping.ReadPlanFromTable(connect, path_data, str(date))
	# mapping.ReadProductAlias(connect, path_data, str(date))	
	#========================================================
	
	for plan in list_plan_diff:
		print(plan)
	print('list_plan_diff: ', len(list_plan_diff))
	
	return list_plan_diff



# def AutoMap(connect, path_data, date):
# 	# ------------- Get new plan or change plan ----------------	
# 	list_plan = GetListPlanChange(connect, path_data, date)
# 	print (len(list_plan))
# 	# if len(list_plan) > 0:
# 	# ------------- Get campaign for mapping ----------------	
# 	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	
# 	list_map_all = []
# 	list_plan_remove_unmap = []
# 	list_camp_remove_unmap = []
# 	list_plan_update = []
# 	if not os.path.exists(path_data_total_map):
# 		i = 0
# 		find = True
# 		date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
# 		path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
# 		while not os.path.exists(path_data_total_map):
# 			i = i + 1
# 			date_before = date_before - timedelta(1)
# 			path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
# 			if i == 60:
# 				find = False
# 				break			
# 	else:
# 		find = True

# 	if find:
# 		with open (path_data_total_map,'r') as f:
# 			data_total = json.load(f)

# 	list_full_camp = data_total['UN_CAMPAIGN']
# 	list_camp_all = []
# 	list_camp_GS5 = []
# 	list_camp_WPL = []
# 	for camp in list_full_camp:
# 		if (camp['Dept'] == 'GS5'):
# 			list_camp_GS5.append(camp)
# 		elif (camp['Dept'] == 'WPL'):
# 			list_camp_GS5.append(camp)
# 		else:
# 			list_camp_all.append(camp)

# 	print(len(list_full_camp))
# 	print(len(list_camp_all))
# 	print(len(list_camp_GS5))
# 	print(len(list_camp_WPL))

				
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
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
date = '2017-08-31' 
list_plan_diff = GetListPlanChange(connect, path_data, date)



					







