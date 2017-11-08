
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












def ConvertPlanToJson(list_new_plan, list_key):
	
	list_json= []
	for plan in list_new_plan: 
		list_temp = []
		unmap = {}
		for value in plan:
			val = value   
			if isinstance(value, datetime):            
				val = value.strftime('%Y-%m-%d')			
			list_temp.append(val)

		for i in range(len(list_key)):
			unmap[list_key[i]] = list_temp[i]
		list_json.append(unmap)
	return list_json


def CompareTwoPlan(plan_1, plan_2, list_key):

	"""
		Return: 
			1 if only update
			2 if only map
			3 if only change real date
	"""
	check_num = 0
	for i in range(len(list_key)):
		# if (list_key[i] == 'EFORM_NO'):			
		if (str(plan_1[list_key[i]]).find(u'\xa0') >= 0):				
			plan_1[list_key[i]] = plan_1[list_key[i]].replace(u'\xa0', u' ')
		if (str(plan_2[list_key[i]]).find(u'\xa0') >= 0):				
			plan_2[list_key[i]] = plan_2[list_key[i]].replace(u'\xa0', u' ')

			# if (str(plan_1[list_key[i]]).find(' ') >= 0):
			# 	plan_1[list_key[i]] = plan_1[list_key[i]].replace(' ', '')
			# if (str(plan_2[list_key[i]]).find(' ') >= 0):
			# 	plan_2[list_key[i]] = plan_2[list_key[i]].replace(' ', '')

		if (isinstance(plan_1[list_key[i]], float)):
			plan_1[list_key[i]] = round(plan_1[list_key[i]], 0)

		if (isinstance(plan_2[list_key[i]], float)):
			plan_2[list_key[i]] = round(plan_2[list_key[i]], 0)

		if plan_1[list_key[i]] == plan_2[list_key[i]]:
			check_num += 1

	if (check_num == len(list_key)):
		return True

	return False


def GetListDiff(connect, path_data, date):	
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
      		from STG_FA_DATA_GG"

	list_all_key = ['CYEAR', 'CMONTH', 'LEGAL', 'DEPARTMENT', 'DEPARTMENT_NAME', 'PRODUCT', 
		'REASON_CODE_ORACLE', 'EFORM_NO', 'START_DAY', 'END_DAY_ESTIMATE', 'CHANNEL', 
		'FORM_TYPE', 'UNIT_OPTION', 'UNIT_COST', 'AMOUNT_USD', 'CVALUE', 'ENGAGEMENT', 
		'IMPRESSIONS', 'CLIKE', 'CVIEWS', 'INSTALL', 'NRU', 'INSERT_DATE', 
		'REAL_START_DATE', 'REAL_END_DATE']

	cursor.execute(query) 	
	list_new_plan = cursor.fetchall()
	list_new_plan = list(list_new_plan)
	cursor.close()
	list_new_plan = ConvertPlanToJson(list_new_plan, list_all_key)

	#============== Read Plan from file plan.json =============
	path_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')	
	with open(path_plan, 'r') as fi:
		data_plan = json.load(fi)
		
	
	#============ Get list plan diff =================
	list_diff = []
	list_plan_new = []
	list_plan_only_update = []
	list_plan_change_real = []


	for new_plan in list_new_plan:
		flag = False
		for plan in data_plan['plan']:
			if (CompareTwoPlan(plan, new_plan, list_all_key)):
				flag = True

		if flag == False:
			list_diff.append(new_plan)
			

	for plan in list_diff:		
		flag = True
		for _value in data_plan['plan']:			
			if _value['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] and \
			_value['PRODUCT'] == plan['PRODUCT'] and \
			_value['FORM_TYPE'] == plan['FORM_TYPE'] and \
			_value['UNIT_OPTION'] == plan['UNIT_OPTION'] and \
			_value['START_DAY'] == plan['START_DAY'] and \
			_value['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE'] and \
			(_value['REAL_START_DATE'] != plan['REAL_START_DATE'] or \
			_value['REAL_END_DATE'] != plan['REAL_END_DATE'] ):
				list_plan_change_real.append(plan)
				flag = False
				
			if _value['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] and \
			_value['PRODUCT'] == plan['PRODUCT'] and \
			_value['FORM_TYPE'] == plan['FORM_TYPE'] and \
			_value['UNIT_OPTION'] == plan['UNIT_OPTION'] and \
			_value['START_DAY'] == plan['START_DAY'] and \
			_value['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE'] and \
			_value['REAL_START_DATE'] == plan['REAL_START_DATE'] and \
			_value['REAL_END_DATE'] == plan['REAL_END_DATE'] :
				list_plan_only_update.append(plan)
				flag = False

		if flag:
			list_plan_new.append(plan)	


		# ==================== Get list modified ==================
		list_plan_modified = data_plan['plan'].copy()
		for plan in data_plan['plan']:
			for new_plan in list_new_plan:
				if (new_plan['PRODUCT'] == plan['PRODUCT']) \
					and (new_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE']) \
					and (new_plan['FORM_TYPE'] == plan['FORM_TYPE']) \
					and (new_plan['UNIT_OPTION'] == plan['UNIT_OPTION'])\
					and (new_plan['START_DAY'] == plan['START_DAY']) \
					and (new_plan['END_DAY_ESTIMATE'] == plan['END_DAY_ESTIMATE']):
						
					list_plan_modified.remove(plan)			

	return list_diff, list_plan_new, list_plan_change_real, list_plan_only_update, list_plan_modified


def ReleaseCampOfPlanRealDate(list_plan_change, data_total):
	list_camp_insert_unmap = []	
	list_remove_manual = []

	print()	
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))	
	print ('TOTAL: ', len(data_total['TOTAL']))


	# ---------- Remove camp from TOTAL --------------------
	list_camp = []
	for plan in list_plan_change:
		plan_temp = {
					'PLAN' : plan,
					'CAMPAIGN_MANUAL_MAP' : []
		}
		
		for plan_total in data_total['TOTAL']:
			if plan_total['PRODUCT'] == plan['PRODUCT'] and \
				plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] and \
				plan_total['FORM_TYPE'] == plan['FORM_TYPE'] and \
				plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:

				start, end = mapping.ChooseTime(plan)

				if (len(plan_total['CAMPAIGN']) > 0):
					for camp in plan_total['CAMPAIGN']:
						if (camp['Date'] <= start) or (camp['Date'] >= end):
							#----------- Remove from TOTAL -----------------
							if ('CAMPAIGN_MANUAL_MAP' in plan_total) and (camp in plan_total['CAMPAIGN_MANUAL_MAP']):
								plan_temp['CAMPAIGN_MANUAL_MAP'].append(camp)
							
							plan_total['CAMPAIGN'].remove(camp)		
							list_camp_insert_unmap.append(camp)

				if ('CAMPAIGN_MANUAL_MAP' in plan_total) and (len(plan_temp['CAMPAIGN_MANUAL_MAP']) > 0):
					list_remove_manual.append(plan_temp)

			
	# -------------- Insert camp unmap into UN_CAMPAIGN ---------------------
	data_total['UN_CAMPAIGN'].extend(list_camp_insert_unmap)

	

	print()	
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))	
	print ('TOTAL: ', len(data_total['TOTAL']))

	print()
	print('list_camp_insert_unmap: ', len(list_camp_insert_unmap))	
	print('list_remove_manual: ', len(list_remove_manual))

	return data_total, list_camp_insert_unmap, list_remove_manual


def ReleaseModifiedPlan(list_plan_modified, data_total):
		
	list_camp_insert_unmap = []
	list_plan_remove_total = []		
	list_remove_manual = []

	# ----------------- Merger into database ------------------------
	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))	
	print ('TOTAL: ', len(data_total['TOTAL']))
	
	# =================== Remove in data TOTAL and Re-Insert camp in UN_CAMPAIGN ========================
	for plan in list_plan_modified:		
		for plan_total in data_total['TOTAL']:
			if plan_total['PRODUCT'] == plan['PRODUCT'] \
				and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
				and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
				and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:
				
				if ('CAMPAIGN_MANUAL_MAP' in plan_total) and len(plan_total['CAMPAIGN_MANUAL_MAP']) > 0 :
					plan_temp = {
						'PLAN': plan,
						'CAMPAIGN_MANUAL_MAP': plan_total['CAMPAIGN_MANUAL_MAP']
					}	
					list_remove_manual.append(plan_temp)	

				for camp in plan_total['CAMPAIGN']:
					data_total['UN_CAMPAIGN'].append(camp)
					list_camp_insert_unmap.append(camp)

				data_total['TOTAL'].remove(plan_total)
				list_plan_remove_total.append(plan)		
	
	
	print()	
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))	
	print ('TOTAL: ', len(data_total['TOTAL']))	

	
	return data_total, list_camp_insert_unmap, list_plan_remove_total, list_remove_manual


def AddToTotal (data_total, data_date):

	list_plan_insert = []
	list_plan_remove = []
	# Merge plan cua ngay voi total
	for plan_date in data_date:
		flag = False
		for plan in data_total:
			# print (plan)
			# print (plan_date)
			if plan['PRODUCT_CODE'] == plan_date['PRODUCT_CODE'] \
				and plan['REASON_CODE_ORACLE'] == plan_date['REASON_CODE_ORACLE'] \
				and plan['FORM_TYPE'] == plan_date['FORM_TYPE'] \
				and plan['UNIT_OPTION'] == plan_date['UNIT_OPTION'] \
				and plan['START_DAY'] == plan_date['START_DAY'] \
				and plan['END_DAY_ESTIMATE'] == plan_date['END_DAY_ESTIMATE']:

				if len(plan_date['CAMPAIGN']) > 0 and len(plan['CAMPAIGN']) == 0:					
					list_plan_remove.append(plan_date)

				# Cap nhat real date
				plan['REAL_START_DATE'] = plan_date['REAL_START_DATE']
				plan['REAL_END_DATE'] = plan_date['REAL_END_DATE']

				# Chuyen campaign maping duoc cua plan
				temp_date = plan_date['CAMPAIGN']
				temp = plan['CAMPAIGN']
				temp.extend(temp_date)
				plan['CAMPAIGN'] = temp
				flag = True

		# Plan moi
		if flag == False:
			data_total.append(plan_date)
			# Plan nay, neu unmap (list campaign == 0) se insert vao trong plan un, con neu map se insert vao total.
			list_plan_insert.append(plan_date)

	# print (len(list_plan_remove))
	return (data_total, list_plan_insert, list_plan_remove)


def merger_data_map(data_map_all, data_map_GS5, data_map_WPL):
	#============= Merger Plan ==================	
	list_plan = data_map_all['PLAN'].copy()
	list_plan, list_plan_insert, list_plan_remove = AddToTotal (list_plan, data_map_GS5['PLAN'])
	list_plan, list_plan_insert, list_plan_remove = AddToTotal (list_plan, data_map_WPL['PLAN'])


	#============= Merger Campaign ==============
	list_un_camp = []
	list_un_camp.extend(data_map_all['UN_CAMP'])
	list_un_camp.extend(data_map_GS5['UN_CAMP'])
	list_un_camp.extend(data_map_WPL['UN_CAMP'])
	
	return(list_plan, list_un_camp)


def Mapping_Auto(path_data, date, list_plan, list_full_uncamp):

	list_camp_all = []
	list_camp_GS5 = []
	list_camp_WPL = []

	for camp in list_full_uncamp:					
		# if (str(camp['Campaign ID']) == '702245469'):
		# 	list_full_camp[list_full_camp.index(camp)]['Campaign'] = 'ROW|239|1705131|AND|IN|SEM_Competitor global vn'	
				
		if (camp['Dept'] == 'GS5'):			
			list_camp_GS5.append(camp)
		elif (camp['Dept'] == 'WPL'):		
			list_camp_GS5.append(camp)
		else:
			list_camp_all.append(camp)	

	#----------------- Mapping with campaign unmap -------------------------
	data_map_all = {
		'PLAN': [],
		'UN_CAMP': []
	}

	data_map_GS5 = {
		'PLAN': [],
		'UN_CAMP': []
	}

	data_map_WPL = {
		'PLAN': [],
		'UN_CAMP': []
	}
	
	auto_mapping  = time.time()
	if (len(list_camp_all) > 0):
		data_map_all = mapping.MapAccountWithCampaignAll(path_data, list_plan, list_camp_all, date)

	if (len(list_camp_GS5) > 0):
		data_map_GS5 = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_GS5, date)

	if (len(list_camp_WPL) > 0):
		data_map_WPL = mapping.MapAccountWithCampaignGS5(path_data, list_plan, list_camp_WPL, date)



	list_plan, list_un_camp = merger_data_map(data_map_all, data_map_GS5, data_map_WPL)

	return list_plan, list_un_camp


def NewPlan(path_data, date, list_plan, data_total):

	list_camp_remove_unmap = []
	list_plan_insert_total = []
		
	list_plan_total, list_un_camp = Mapping_Auto(path_data, date, list_plan, data_total['UN_CAMPAIGN'])

	
	#---------------- Merger data unmap ---------------------------------------
	print ()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))	

	#===================== CASE MAP ===================
	#----------- Remove campaign unmap ---------------------
	if (len(list_un_camp) > 0):
		for campaign in data_total['UN_CAMPAIGN']:
			flag = True    # True if just map
			for camp in list_un_camp:				  
				if camp['Campaign ID'] == campaign['Campaign ID'] \
				and camp['Date'] == campaign['Date']:
					flag = False   # False if un_map
			if flag:
				list_camp_remove_unmap.append(campaign)
				data_total['UN_CAMPAIGN'].remove(campaign)

	#===================== CASE MAP AND UN_MAP===================
	#---------- Insert data total ------------------
	data_total['TOTAL'], list_plan_insert, list_plan_remove = AddToTotal (data_total['TOTAL'], list_plan_total)
	list_plan_insert_total.extend(list_plan_total)   
			
	
	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	print()
	print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
	print ('list_plan_insert_total: ', len(list_plan_insert_total))
	
	
	return data_total, list_camp_remove_unmap, list_plan_insert_total


def ClassifyPlan(connect, path_data, date):

	list_camp_remove_unmap = []
	list_camp_insert_unmap = []

	list_plan_insert_total = []
	list_plan_update_total = []
	list_plan_remove_total = [] 

	list_data_insert_map = []
	list_data_remove_map = []
	list_plan_update_map = []
	list_plan_remove_map = []

	list_plan_insert_unmap = []
	list_plan_remove_unmap = []

	list_remove_manual = []
		
	

	# =============== Get plan change =====================	
	list_plan_diff, list_plan_new, list_plan_change_real_date, \
	list_plan_update, list_plan_modified = GetListDiff(connect, path_data, date)

	print('list_diff: ', len(list_plan_diff))
	print('list_plan_new: ', len(list_plan_new))
	print('list_plan_only_update: ', len(list_plan_update))	
	print('list_plan_change_real: ', len(list_plan_change_real_date))
	print('list_plan_modified: ', len(list_plan_modified))


	# ============= Process with each case =======================	
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')

	if not os.path.exists(path_data_total_map):
		i = 0
		find = True
		date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
		path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
		path_data_un_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'un_map_camp' + '.json')
		while not os.path.exists(path_data_total_map):
			i = i + 1
			date_before = date_before - timedelta(1)
			path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'total_mapping' + '.json')
			path_data_un_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'un_map_camp' + '.json')
			if i == 60:
				find = False
				break
		# ---- Neu tim thay file total truoc do -----
	else:
		find = True

	if find:
		data_total = {}
		data_total['TOTAL'] = []
		data_total['UN_CAMPAIGN'] = []

		with open (path_data_total_map,'r') as f:
			data_total['TOTAL'] = json.load(f)			

		with open (path_data_un_map,'r') as f:
			data_total['UN_CAMPAIGN'] = json.load(f)

		print('TOTAL: ', len(data_total['TOTAL']))	
		print('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))



	# ============ Case 0: Release camp in list change real date ===============
		if (len(list_plan_change_real_date) > 0):
			print("=========== Case 0: Release camp in list change real date ==========")					
			data_total, camp_insert_unmap, remove_manual = ReleaseCampOfPlanRealDate(list_plan_change_real_date, data_total)

			list_camp_insert_unmap.extend(camp_insert_unmap)			
			list_remove_manual.extend(remove_manual)


	# ============ Case 1: Release list plan modified ===============
		if (len(list_plan_modified) > 0):
			print("=========== Case 1: Release list plan modified ==========")					
			data_total, camp_insert_unmap, plan_remove_total, \
			remove_manual = ReleaseModifiedPlan(list_plan_modified, data_total)

			list_camp_insert_unmap.extend(camp_insert_unmap)	
			list_plan_remove_total.extend(plan_remove_total)		
			list_remove_manual.extend(remove_manual)


	#================== Case 3: New Plan	==================
		if (len(list_plan_new) > 0):
			print("=========== Case 3: New Plan	 ================")
			list_plan_new = mapping.AddProductCode(path_data, list_plan_new, date)			 			
			data_total, camp_remove_unmap, plan_insert_total = NewPlan(path_data, date, list_plan_new, data_total)

			list_camp_remove_unmap.extend(camp_remove_unmap)
			list_plan_insert_total.extend(plan_insert_total)
			




	# 	#======== Case 1: Data update can map
	# 	if (len(list_plan_map) > 0):
	# 		print("=========== Case 1: Data update can map (not change real date) ==========")
	# 		# for plan in list_plan_map:
	# 		# 	print(plan)
	# 		list_plan_map = mapping.AddProductCode(path_data, list_plan_map, date)		

	# 		list_plan_modified = GetPlanModified(connect, path_data)
	# 		data_total, camp_remove_unmap, camp_insert_unmap, plan_remove_total, \
	# 		plan_remove_map, plan_remove_unmap, plan_insert_unmap, \
	# 		data_insert_map, plan_insert_total, remove_manual = ModifiedPlanToMap(path_data, date, list_plan_map, list_plan_modified, data_total)

	# 		list_camp_remove_unmap.extend(camp_remove_unmap)
	# 		list_camp_insert_unmap.extend(camp_insert_unmap)
	# 		list_plan_remove_total.extend(plan_remove_total)
	# 		list_plan_remove_map.extend(plan_remove_map)
	# 		list_plan_remove_unmap.extend(plan_remove_unmap)
	# 		list_plan_insert_unmap.extend(plan_insert_unmap)
	# 		list_data_insert_map.extend(data_insert_map)
	# 		list_plan_insert_total.extend(plan_insert_total)
	# 		list_remove_manual.extend(remove_manual)

	# 	# #======== Case 2: Data update can map
	# 	if (len(list_plan_change_real_date) > 0):
	# 		print("=========== Case 2: Data update can map (change real date) ==========")
	# 		# for plan in list_plan_change_real_date:
	# 		# 	print(plan)
	# 		list_plan_change_real_date = mapping.AddProductCode(path_data, list_plan_change_real_date, date)		
	# 		# list_plan_change_real_date = nru.Add_NRU_into_list(connect, list_plan_change_real_date, date)  
			
			
	# 		data_total, camp_remove_unmap, data_insert_map, \
	# 		plan_update_map, plan_remove_unmap, \
	# 		plan_insert_total, plan_update_total  = ChangeRealDatePlanToMap(path_data, date, list_plan_change_real_date, data_total)

	# 		list_camp_remove_unmap.extend(camp_remove_unmap)
	# 		list_data_insert_map.extend(data_insert_map)
	# 		list_plan_update_map.extend(plan_update_map)
	# 		list_plan_remove_unmap.extend(plan_remove_unmap)
	# 		list_plan_insert_total.extend(plan_insert_total)
	# 		list_plan_update_total.extend(plan_update_total)
		

	



	# 	# #============== Case 4: Data update not map ===================
	# 	if (len(list_plan_update) > 0):		
	# 		print("=========== Case 4: Data update not map	 ======================")
	# 		# for plan in list_plan_update:
	# 		# 	print(plan)
	# 		list_plan_update = mapping.AddProductCode(path_data, list_plan_update, date)		
	# 		# list_plan_update = nru.Add_NRU_into_list(connect, list_plan_update, date) 

	# 		data_total, plan_update_total = UpdatePlan(data_total, list_plan_update)
	# 		list_plan_update_total.extend(plan_update_total)
			



	# 	# # =============== COMPUTE MONTHLY FOR EACH TOTAL PLAN ===================		
	# 	start = time.time()
	# 	data_total['TOTAL'] = insert_to_total.CaculatorForPlan(data_total['TOTAL'])
		
	# 	data_total['TOTAL'] = insert_install.InsertInstallToPlan(data_total['TOTAL'], connect, date)
	# 	data_total['TOTAL'] = insert_install_brandingGPS.AddBrandingGPSToPlan(data_total['TOTAL'], connect, date)
	# 	print('Compute MONTHLY time: ', time.time() - start)
	# 	# # with open (path_data_total,'w') as f:
	# 	# # 	json.dump(data_total, f)


	# 	# # ============== Write plan new verson into file plan.json ==========================
	# 	# # ReadPlanFromTable(connect, path_data, date)
	# 	# # nru.Add_NRU_into_plan(connect, path_data, date)

	# print('list_plan_new: ', len(list_plan_new))
	# print('list_plan_map: ', len(list_plan_map))
	# print('list_plan_change_real_date: ', len(list_plan_change_real_date))
	# print('list_plan_update: ', len(list_plan_update))
	# print()
	# print()

		print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
		print('list_camp_insert_unmap: ', len(list_camp_insert_unmap))
		print('list_plan_insert_total: ', len(list_plan_insert_total))
		print('list_plan_update_total: ', len(list_plan_update_total))
		print('list_plan_remove_total: ', len(list_plan_remove_total))
		print('list_data_insert_map: ', len(list_data_insert_map))
		print('list_data_remove_map: ', len(list_data_remove_map))
		print('list_plan_update_map: ', len(list_plan_update_map))
		print('list_plan_remove_map: ', len(list_plan_remove_map))
		print('list_plan_insert_unmap: ', len(list_plan_insert_unmap))
		print('list_plan_remove_unmap: ', len(list_plan_remove_unmap))
		print()
	
	return list_camp_remove_unmap, list_camp_insert_unmap, list_plan_insert_total, \
	list_plan_update_total, list_plan_remove_total, list_data_insert_map, \
	list_data_remove_map, list_plan_update_map, list_plan_remove_map, \
	list_plan_insert_unmap, list_plan_remove_unmap, list_remove_manual


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-10-31' 

list_camp_remove_unmap, list_camp_insert_unmap, list_plan_insert_total, \
	list_plan_update_total, list_plan_remove_total, list_data_insert_map, \
	list_data_remove_map, list_plan_update_map, list_plan_remove_map, \
	list_plan_insert_unmap, list_plan_remove_unmap, list_remove_manual = ClassifyPlan(connect, path_data, date)