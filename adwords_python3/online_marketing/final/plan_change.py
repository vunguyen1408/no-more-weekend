import json
import os
import cx_Oracle 
from datetime import datetime , timedelta, date
import manual_mapping_and_remap as manual
import mapping_campaign_plan as mapping
import insert_data_map_to_total as insert_to_total



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
						# print(plan)					
						list_plan_diff.remove(plan)		


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


	return json_


def ConvertListPlan(list_plan):
	list_plan_json = []
	for plan in list_plan:
		json_ = ConvertPlan(plan)
		list_plan_json.append(json_)
	return list_plan_json


def merger_data_map(data_map_all, data_map_GS5, data_map_WPL):
	#============= Merger Plan ==================
	list_plan = []
	for value in data_map_all['plan']:		
		if (value not in list_plan):
			list_plan.append(value)
		else:			
			for camp in value['CAMPAIGN']:
				if (camp not in list_plan[list_plan.index(value)]['CAMPAIGN']):				
					list_plan[list_plan.index(value)]['CAMPAIGN'].append(camp)

		
	for value in data_map_GS5['plan']:
		if (value not in list_plan):
			list_plan.append(value)
		else:
			for camp in value['CAMPAIGN']:
				if (camp not in list_plan[list_plan.index(value)]['CAMPAIGN']):				
					list_plan[list_plan.index(value)]['CAMPAIGN'].append(camp)

		
	for value in data_map_WPL['plan']:
		if (value not in list_plan):
			list_plan.append(value)
		else:
			for camp in value['CAMPAIGN']:
				if (camp not in list_plan[list_plan.index(value)]['CAMPAIGN']):				
					list_plan[list_plan.index(value)]['CAMPAIGN'].append(camp)

		
	for plan in list_plan:
		if (len(plan['CAMPAIGN']) > 0):
			print(plan['CAMPAIGN'])


	#============= Merger Campaign ==============
	list_camp = []
	list_camp.extend(data_map_all['campaign'])
	list_camp.extend(data_map_GS5['campaign'])
	list_camp.extend(data_map_WPL['campaign'])
	# print(len(data_map_all['campaign']))
	# print(len(data_map_GS5['campaign']))
	# print(len(data_map_WPL['campaign']))
	# print(len(list_camp))

	return(list_plan, list_camp)


	
def AutoMap(connect, path_data, date):
	# ------------- Get new plan or change plan ----------------	
	list_plan = GetListPlanChange(connect, path_data, date)
	list_plan = ConvertListPlan(list_plan)
	list_plan = mapping.AddProductCode(path_data, list_plan, date)

	
	#========== Update new plan for file plan ===============	
	mapping.ReadPlanFromTable(connect, path_data, str(date))
	mapping.ReadProductAlias(connect, path_data, str(date))	
	#========================================================
	
	
	print (len(list_plan))
	# for plan in list_plan:
	# 	print(plan)

	list_data_map = []
	list_plan_remove_unmap = []
	list_camp_remove_unmap = []
	list_plan_update = []
	list_plan_insert = []

	print("==========================================================")
	if len(list_plan) > 0:
		# ------------- Get campaign for mapping ----------------	
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

		if find:
			with open (path_data_total_map,'r') as f:
				data_total = json.load(f)

			list_full_camp = data_total['UN_CAMPAIGN']
			list_camp_all = []
			list_camp_GS5 = []
			list_camp_WPL = []
			for camp in list_full_camp:	
				if 	camp['Advertising Channel'].find('Search') >= 0:
					print(camp['Campaign ID'])
				if (mapping.CheckIsAccountGS5(path_data, camp['Account ID'])):
				# if (camp['Dept'] == 'GS5'):
					list_camp_GS5.append(camp)
				# elif (camp['Dept'] == 'WPL'):
				if (mapping.CheckIsAccountWPL(path_data, camp['Account ID'])):
					list_camp_GS5.append(camp)
				else:
					list_camp_all.append(camp)

			# print(len(list_full_camp))
			# print(len(list_camp_all))
			# print(len(list_camp_GS5))
			# print(len(list_camp_WPL))


			#----------------- Mapping with campaign unmap -------------------------
			data_map_all = {
				'plan': [],
				'campaign': []
			}

			data_map_GS5 = {
				'plan': [],
				'campaign': []
			}

			data_map_WPL = {
				'plan': [],
				'campaign': []
			}
			if (len(list_camp_all) > 0):
				data_map_all = mapping.MapAccountWithCampaignAll(path_data, list_plan, list_camp_all, date)

			if (len(list_camp_GS5) > 0):
				data_map_GS5 = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_GS5, date)

			if (len(list_camp_WPL) > 0):
				data_map_WPL = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_WPL, date)

			list_plan, list_camp = merger_data_map(data_map_all, data_map_GS5, data_map_WPL)

			list_plan_total, list_data_map = insert_to_total.SumTotalManyPlan(list_plan, list_camp)


			# print(list_plan_total)
			print(list_data_map)

			#---------------- Merger data unmap ---------------------------------------

			print('MAP: ', len(data_total['MAP']))
			print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
			print ('UN_PLAN: ', len(data_total['UN_PLAN']))
			print ('TOTAL: ', len(data_total['TOTAL']))


			#---------- Data map ------------------
			data_total['MAP'].extend(list_data_map)

			# ----------- Update Real date ------------
			for data_map in data_total['MAP']:
				for plan in list_plan:
					if data_map['PRODUCT'] == plan['PRODUCT'] \
						and data_map['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
						and data_map['FORM_TYPE'] == plan['FORM_TYPE'] \
						and data_map['UNIT_OPTION'] == plan['UNIT_OPTION'] :
						data_total['MAP'][data_total['MAP'].index(data_map)]['REAL_START_DATE'] = plan['REAL_START_DATE']
						
						
			#----------- Remove unmap ---------------------
			for camp in list_data_map:		
				for campaign in data_total['UN_CAMPAIGN']:
					if camp['Campaign ID'] == campaign['Campaign ID'] \
						and camp['Date'] == campaign['Date']:
						data_total['UN_CAMPAIGN'].remove(campaign)
						list_camp_remove_unmap.append(campaign)

			#------------- Xoa trong danh sach un map PLAN ------------------
			
			for plan in list_data_map:
				for plan_un in data_total['UN_PLAN']:
					if plan_un['PRODUCT'] == plan['PRODUCT'] \
						and plan_un['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
						and plan_un['FORM_TYPE'] == plan['FORM_TYPE'] \
						and plan_un['UNIT_OPTION'] == plan['UNIT_OPTION'] :
						data_total['UN_PLAN'].remove(plan_un)
						list_plan_remove_unmap.append(plan_un)
						data_total['UN_PLAN'][data_total['UN_PLAN'].index(plan_un)]['REAL_START_DATE'] = plan['REAL_START_DATE']

			#----------- Insert unmap plan new into un_plan -------
			for plan in list_plan:
				flag = True
				for plan_map in list_plan_total:					
					if plan_map['PRODUCT'] == plan['PRODUCT'] \
						and plan_map['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
						and plan_map['FORM_TYPE'] == plan['FORM_TYPE'] \
						and plan_map['UNIT_OPTION'] == plan['UNIT_OPTION'] :
						flag = False
				if flag:
					list_plan_insert.append(plan)
					data_total['UN_PLAN'].append(plan)

						

			#------------- Insert total ------------
			for plan in list_plan_total:
				flag = True
				for plan_total in data_total['TOTAL']:
					if plan_total['PRODUCT'] == plan['PRODUCT'] \
						and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
						and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
						and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:						
						plan_total['TOTAL_CAMPAIGN'] = insert_data.SumTwoTotal(plan_total['TOTAL_CAMPAIGN'], plan['TOTAL_CAMPAIGN'])
						flag = False
						data_total['TOTAL'][data_total['TOTAL'].index(plan_total)]['REAL_START_DATE'] = plan['REAL_START_DATE']
				
				if flag:    #----- Không tìm thấy trong total ------			
					data_total['TOTAL'].append(plan)
					


			# # --------------- Tinh total month cho cac plan --------------
			for plan in data_total['TOTAL']:
				plan['MONTHLY'] = {}
				plan = insert_to_total.CaculatorTotalMonth(plan, date)

				
			for plan in data_total['UN_PLAN']:
				plan['MONTHLY'] = {}
				plan = insert_to_total.CaculatorTotalMonth(plan, date)

						
			for plan in data_total['TOTAL']:
				plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualTotal(plan)
				for m in plan['MONTHLY']:
					m['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualMonthly(plan, m)

				for plan_un in list_plan_total:
					if plan_un['PRODUCT'] == plan['PRODUCT'] \
						and plan_un['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
						and plan_un['FORM_TYPE'] == plan['FORM_TYPE'] \
						and plan_un['UNIT_OPTION'] == plan['UNIT_OPTION']:						
						list_plan_update.append(plan)

			path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping_2' + '.json')
			with open (path_data_total_map,'w') as f:
				json.dump(data_total, f)

			print()
			print('MAP: ', len(data_total['MAP']))
			print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
			print ('UN_PLAN: ', len(data_total['UN_PLAN']))
			print ('TOTAL: ', len(data_total['TOTAL']))


			print()
			print('list_data_map: ', len(list_data_map))
			print ('list_plan_remove_unmap: ', len(list_plan_remove_unmap))
			print ('list_camp_remove_unmap: ', len(list_camp_remove_unmap))		
			print('list_plan_update: ', len(list_plan_update))
			print('list_plan_insert: ', len(list_plan_insert))	


			# print('list_data_map: ', (list_data_map))
			# print ('list_plan_remove_unmap: ', (list_plan_remove_unmap))
			# print ('list_camp_remove_unmap: ', (list_camp_remove_unmap))		
			# print('list_plan_update: ', (list_plan_update))	
		

	return (list_data_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update, list_plan_insert)

				



				

	






connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-05-31' 
list_plan_diff = GetListPlanChange(connect, path_data, date)
list_data_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update, list_plan_insert = AutoMap(connect, path_data, date)




#============== Test ham merger ================================
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
#================================================================


# path = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'



					







