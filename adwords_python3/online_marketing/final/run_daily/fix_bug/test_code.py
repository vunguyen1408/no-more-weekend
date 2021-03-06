import json
import os
import cx_Oracle 
from datetime import datetime , timedelta, date
# import manual_mapping_and_remap as manual
import mapping_campaign_plan as mapping
import insert_data_map_to_total as insert_to_total
# import insert_nru_into_data as nru
import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install
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


def ReadPlanFromTable(connect, path_folder, date):
	import datetime

	folder = os.path.join(path_folder, str(date) + '/PLAN')
	if not os.path.exists(folder):
		os.makedirs(folder)
	
	file_plan = os.path.join(folder, 'plan.json')
	

	#============================== Connect database =============================	
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#======================= Get data from database ==============================
	query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, PRODUCT, REASON_CODE_ORACLE, EFORM_NO, \
	      START_DAY, END_DAY_ESTIMATE, CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, CVALUE, \
	      ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, INSTALL, NRU, INSERT_DATE, REAL_START_DATE, REAL_END_DATE, \
	      STATUS, LAST_UPDATED_DATE \
	  from STG_FA_DATA_GG'

	cursor.execute(query)
	row = cursor.fetchall()
	temp = list(row)
	cursor.close()



	#===================== Convert data into json =================================

	list_key = ['CYEAR', 'CMONTH', 'LEGAL', 'DEPARTMENT', 'DEPARTMENT_NAME', 'PRODUCT', 
		'REASON_CODE_ORACLE', 'EFORM_NO', 'START_DAY', 'END_DAY_ESTIMATE', 'CHANNEL', 
		'FORM_TYPE', 'UNIT_OPTION', 'UNIT_COST', 'AMOUNT_USD', 'CVALUE', 'ENGAGEMENT', 
		'IMPRESSIONS', 'CLIKE', 'CVIEWS', 'INSTALL', 'NRU', 'INSERT_DATE', 
		'REAL_START_DATE', 'REAL_END_DATE', 'STATUS', 'LAST_UPDATED_DATE']

	list_json= []
	for plan in temp: 
		list_temp = []
		unmap = {}
		for value in plan:
			val = value   
			if isinstance(value, datetime.datetime):            
				val = value.strftime('%Y-%m-%d')			
			list_temp.append(val)

		for i in range(len(list_key)):
			unmap[list_key[i]] = list_temp[i]
	list_json.append(unmap)
	plan_ = {}
	plan_['plan'] = list_json

	#================ Add product id to plan =================
	ReadProductAlias(connect, path_folder, date)	
	plan_['plan'] = AddProductCode(path_folder, plan_['plan'], date)
	

	with open (file_plan, 'w') as f:
		json.dump(plan_, f)


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
	# print(final_log) 
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


def UpdatePlan(data_total, list_plan_update):

	list_plan_update_total = []	
	
	#=========== Update plan in DATA TOTAL ======================	
	for updated_plan in list_plan_update:
		for plan in data_total['TOTAL']:
			if updated_plan['PRODUCT'] == plan['PRODUCT'] \
			and updated_plan['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
			and updated_plan['FORM_TYPE'] == plan['FORM_TYPE'] \
			and updated_plan['UNIT_OPTION'] == plan['UNIT_OPTION'] :
				UpdateOnePlan(data_total['TOTAL'][data_total['TOTAL'].index(plan)], updated_plan)				
				list_plan_update_total.append(updated_plan)

	print('list_plan_update_total: ', len(list_plan_update_total))
	
	return data_total, list_plan_update_total


def AddToTotal (data_total, data_date, date):

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
					# print (len(plan_date['CAMPAIGN']))
					# print (len(plan['CAMPAIGN']))
					# print ("add reomve")
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


def merger_data_map(data_map_all, data_map_GS5, data_map_WPL, date):
	#============= Merger Plan ==================	
	list_plan = data_map_all['PLAN'].copy()
	list_plan, list_plan_insert, list_plan_remove = AddToTotal (list_plan, data_map_GS5['PLAN'], date)
	list_plan, list_plan_insert, list_plan_remove = AddToTotal (list_plan, data_map_WPL['PLAN'], date)


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



	list_plan, list_un_camp = merger_data_map(data_map_all, data_map_GS5, data_map_WPL, date)

	return list_plan, list_un_camp


def NewPlan(path_data, date, list_plan, data_total):

	list_camp_remove_unmap = []
	list_plan_insert_total = []
	list_data_insert_map = []
	list_plan_insert_unmap = []

	get_camp = time.time()

	
	list_plan_total, list_un_camp = Mapping_Auto(path_data, date, list_plan, data_total['UN_CAMPAIGN'])

	
	#---------------- Merger data unmap ---------------------------------------

	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	insert_file  = time.time()

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

	
	#---------- Insert data total ------------------
	data_total['TOTAL'], list_plan_insert, list_plan_remove = AddToTotal (data_total['TOTAL'], list_plan_total, date)
	list_plan_insert_total.extend(list_plan_total)   # Can tinh them MONTHLY

	
	#==================== CASE UNMAP ==========================		
	#----------- Insert unmap plan new into un_plan -------
	
	end_insert_file  = time.time()
	print('Time insert file: ', end_insert_file - insert_file)

	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	print()
	print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
	print ('list_plan_insert_total: ', len(list_plan_insert_total))
	print ('list_data_insert_map: ', len(list_data_insert_map))		
	print('list_plan_insert_unmap: ', len(list_plan_insert_unmap))
	
	# print()
	# print('list_camp_remove_unmap: ', list_camp_remove_unmap)
	# print ('list_plan_insert_total: ', list_plan_insert_total)
	# print ('list_data_insert_map: ', list_data_insert_map)	
	# print('list_plan_insert_unmap: ', list_plan_insert_unmap)

	total_time = time.time()
	print("TOTAL TIME: ", total_time - get_camp)

	return data_total, list_camp_remove_unmap, list_plan_insert_total, list_data_insert_map, list_plan_insert_unmap


def GetPlanModified(connect, path_data):
	#====================== Get old plan in python ==========================
	path_plan = os.path.join(path_data + '/' + str(date) + '/PLAN/plan.json')
	# path_plan = '/u01/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA/2017-09-30/PLAN/plan.json'
	with open(path_plan, 'r') as fi:
		data = json.load(fi)
	list_plan =  data['plan'].copy()
	
	#======================= Get new plan in database ========================
	list_new_plan = []
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
				# and (new_plan['REAL_START_DATE'] == plan['REAL_START_DATE']) \
				# and (new_plan['REAL_END_DATE'] == plan['REAL_END_DATE']) :	
				list_plan.remove(plan)
						
	
	return list_plan


def ModifiedPlanToMap(path_data, date, list_plan_map, list_plan_modified, data_total):
	print('list_plan_modified: ', len(list_plan_modified))
	# for i in list_plan_modified:
		# print(i)
	list_camp_remove_unmap = []	
	list_camp_insert_unmap = []
	list_plan_remove_total = []	
	list_plan_remove_unmap = []
	list_plan_remove_map = []
	list_plan_insert_unmap = []
	list_data_insert_map = []
	list_plan_insert_total = []
	list_remove_manual = []


	get_camp = time.time()

	# ----------------- Merger into database ------------------------

	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))	
	print ('TOTAL: ', len(data_total['TOTAL']))

	insert_file  = time.time()

	
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

	list_plan_total, list_un_camp = Mapping_Auto(path_data, date, list_plan_map, data_total['UN_CAMPAIGN'])
	
	
	#===================== CASE MAP ===================
	#----------- Remove campaign unmap ---------------------	
	if (len(list_un_camp) > 0):
		for campaign in data_total['UN_CAMPAIGN']:
			flag = False    # False if just map
			for camp in list_un_camp:				  
				if camp['Campaign ID'] == campaign['Campaign ID'] \
				and camp['Date'] == campaign['Date']:
					flag = True   # True if un_map
			if flag == False:
				list_camp_remove_unmap.append(campaign)
				data_total['UN_CAMPAIGN'].remove(campaign)

	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	#---------- Insert data total ------------------
	data_total['TOTAL'], list_plan_insert, list_plan_remove = AddToTotal (data_total['TOTAL'], list_plan_total, date)
	
	#==================== CASE UNMAP ==========================		
	#----------- Insert unmap plan new into total -------
	for plan in list_plan_total:
		flag = True   # True if plan un map
		for plan_total in data_total['TOTAL']:					
			if plan_total['PRODUCT'] == plan['PRODUCT'] \
				and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
				and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
				and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION'] :
				flag = False
		if flag:			
			data_total['TOTAL'].append(plan)	
			list_plan_insert_total.append(plan)	
	
	end_insert_file  = time.time()
	print('Time insert file: ', end_insert_file - insert_file)

	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	print()
	print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
	print ('list_camp_insert_unmap: ', len(list_camp_insert_unmap))
	print ('list_plan_remove_total: ', len(list_plan_remove_total))		
	# print('list_plan_remove_map: ', len(list_plan_remove_map))

	# print('list_plan_remove_unmap: ', len(list_plan_remove_unmap))
	# print ('list_plan_insert_unmap: ', len(list_plan_insert_unmap))
	# print ('list_data_insert_map: ', len(list_data_insert_map))		
	print('list_plan_insert_total: ', len(list_plan_insert_total))
	print('list_remove_manual: ', len(list_remove_manual))
	
	total_time = time.time()
	print("TOTAL TIME: ", total_time - get_camp)

	
	return data_total, list_camp_remove_unmap, list_camp_insert_unmap, list_plan_remove_total, list_plan_remove_map, \
	list_plan_remove_unmap, list_plan_insert_unmap, list_data_insert_map, list_plan_insert_total, list_remove_manual


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


def GetCampFromDataMAP(data_map):
	camp = {}

	camp['Campaign ID'] = data_map['Campaign ID']
	camp['Campaign'] = data_map['Campaign']
	camp['Account ID'] = data_map['Account ID']
	camp['Account Name'] = data_map['Account Name']
	camp['Dept'] = data_map['Dept']

	camp['Campaign state'] = data_map['Campaign state']
	camp['Campaign serving status'] = data_map['Campaign serving status']
	camp['Advertising Channel'] = data_map['Advertising Channel']
	camp['Advertising Sub Channel'] = data_map['Advertising Sub Channel']
	camp['Bid Strategy Type'] = data_map['Bid Strategy Type']

	camp['Start date'] = data_map['Start date']
	camp['End date'] = data_map['End date']
	camp['Invalid clicks'] = data_map['Invalid clicks']
	camp['Conversions'] = data_map['Conversions']
	camp['Engagements'] = data_map['Engagements']

	camp['Impressions'] = data_map['Impressions']
	camp['Unique cookies'] = data_map['Unique cookies']
	camp['Clicks'] = data_map['Clicks']
	camp['Interactions'] = data_map['Interactions']
	camp['Interaction Rate'] = data_map['Interaction Rate']

	camp['Interaction Types'] = data_map['Interaction Types']
	camp['Cost'] = data_map['Cost']
	camp['Views'] = data_map['Views']
	camp['CTR'] = data_map['CTR']
	camp['Avg. position'] = data_map['Avg. position']

	camp['View rate'] = data_map['View rate']
	camp['Video played to 25%'] = data_map['Video played to 25%']
	camp['Video played to 50%'] = data_map['Video played to 50%']
	camp['Video played to 75%'] = data_map['Video played to 75%']
	camp['Video played to 100%'] = 	data_map['Video played to 100%']

	camp['Avg. CPE'] = 	data_map['Avg. CPE']
	camp['Avg. CPV'] = data_map['Avg. CPV']
	camp['Avg. CPC'] = data_map['Avg. CPC']
	camp['Avg. Cost'] = data_map['Avg. Cost']
	camp['Avg. CPM'] = data_map['Avg. CPM']

	camp['INSTALL_CAMP'] = data_map['INSTALL_CAMP']
	camp['Mapping'] = data_map['Mapping']
	camp['Date'] = data_map['Date']
	camp['Plan'] = data_map['Plan']
	camp['STATUS'] =data_map['STATUS']

	return camp
	

def ChangeRealDatePlanToMap(path_data, date, list_plan_change, data_total):	
	list_camp_remove_unmap = []	
	list_data_insert_map = []
	list_plan_update_map = []	
	list_plan_remove_unmap = []	
	list_plan_insert_total = []
	list_plan_update_total = []
	

	list_plan_total, list_un_camp = Mapping_Auto(path_data, date, list_plan_change, data_total['UN_CAMPAIGN'])

	
	# ----------------- Merger into database ------------------------		
	print()
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

	

	#------------- Insert total ------------
	data_total['TOTAL'], list_plan_insert, list_plan_remove = AddToTotal (data_total['TOTAL'], list_plan_total, date)

	for plan in list_plan_total:		
		for plan_total in data_total['TOTAL']:
			if plan_total['PRODUCT'] == plan['PRODUCT'] \
				and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
				and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
				and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:						
								
				plan_total['REAL_START_DATE'] = plan['REAL_START_DATE']
				plan_total['REAL_END_DATE'] = plan['REAL_END_DATE']
				list_plan_update_total.append(plan)
		
					
				
		
	#==================== CASE UNMAP ==========================			
	# ----------- Update Real date in UN_PLAN ------------
	
	
	print()
	print ('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))
	print ('TOTAL: ', len(data_total['TOTAL']))

	print()
	print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
	print ('list_data_insert_map: ', len(list_data_insert_map))
	print ('list_plan_update_map: ', len(list_plan_update_map))		
	print('list_plan_remove_unmap: ', len(list_plan_remove_unmap))
	print ('list_plan_insert_total: ', len(list_plan_insert_total))		
	print('list_plan_update_total: ', len(list_plan_update_total))
	
	return data_total, list_camp_remove_unmap, list_data_insert_map, list_plan_update_map, list_plan_remove_unmap, list_plan_insert_total, list_plan_update_total
	

def RecomputeTotalPlan(plan, list_campaign):

	"""
		Hàm tính lại total cho một plan (trừ đi các campaign được nhả)
	"""
	sum_plan = plan['TOTAL_CAMPAIGN'].copy()
	for campaign_in_plan in plan['CAMPAIGN']:
		for campaign in list_campaign:
			
			if (str(campaign_in_plan['CAMPAIGN_ID']) == str(campaign['Campaign ID'])) \
			and (campaign_in_plan['Date'] == campaign['Date']):
				# --------------- Tính total ------------------
				sum_plan['CLICKS'] -= float(campaign['Clicks'])
				sum_plan['IMPRESSIONS'] -= float(campaign['Impressions'])
				sum_plan['CTR'] -= float(campaign['CTR'])
				sum_plan['AVG_CPC'] -= float(campaign['Avg. CPC'])
				sum_plan['AVG_CPM'] -= float(campaign['Avg. CPM'])
				sum_plan['COST'] -= float(campaign['Cost'])
				sum_plan['CONVERSIONS'] -= float(campaign['Conversions'])
				sum_plan['INVALID_CLICKS'] -= float(campaign['Invalid clicks'])
				sum_plan['AVG_POSITION'] -= float(campaign['Avg. position'])
				sum_plan['ENGAGEMENTS'] -= float(campaign['Engagements'])
				sum_plan['AVG_CPE'] -= float(campaign['Avg. CPE'])
				sum_plan['AVG_CPV'] -= float(campaign['Avg. CPV'])
				sum_plan['INTERACTIONS'] -= float(campaign['Interactions'])
				sum_plan['VIEWS'] -= float(campaign['Views'])
				if 'INSTALL_CAMP' not in campaign:
					campaign['INSTALL_CAMP'] = 0
				sum_plan['INSTALL_CAMP'] -= float(campaign['INSTALL_CAMP'])
	print (plan['TOTAL_CAMPAIGN'])
	print()		
	plan['TOTAL_CAMPAIGN'] = sum_plan.copy()
	print (plan['TOTAL_CAMPAIGN'])
	return plan


def ReleaseCampOfPlanRealDate(path_data, date, list_plan_change, data_total):
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
							if camp in plan_total['CAMPAIGN_MANUAL_MAP']:
								plan_temp['CAMPAIGN_MANUAL_MAP'].append(camp)
							
							plan_total['CAMPAIGN'].remove(camp)		
							list_camp_insert_unmap.append(camp)

				if len(plan_temp['CAMPAIGN_MANUAL_MAP']) > 0:
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
			



def ClassifyPlan(connect, path_data, date, path_log):

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
	# fi = open(path_log, 'r')
	# final_log = fi.read()
	final_log = '11/06/2017 03:46:00'
	print(final_log)

	list_plan_diff, final_log = GetListPlanChangeFromTable(connect, final_log)

	fi = open(path_log, 'w') 
	fi.writelines(final_log)
	print("Save log ok..........")

	# ============== Classify plan diff ===================
	list_plan_new = []
	list_plan_map = []
	list_plan_change_real_date = []
	list_plan_update = []

	for _plan in list_plan_diff:

		plan = ConvertPlan(_plan)
		if _plan[22] == _plan[26]:			
			list_plan_new.append(plan)
			# print('new')
		else:
			# ========= Finally plan from data ==============
			file_plan = os.path.join(path_data, str(date) + '/PLAN/plan.json')
			# file_plan = '/u01/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA/2017-09-30/PLAN/plan.json'
			with open(file_plan, 'r') as fi:
				list_plan = json.load(fi)
		
			flag = CheckPlanUpdate(list_plan['plan'], plan)

			if flag:
				list_plan_update.append(plan)
			else:
				check_real_date = CheckPlanUpdateRealDate(list_plan['plan'], plan)
				if check_real_date:
					list_plan_change_real_date.append(plan)
				else:
					list_plan_map.append(plan)


	# ============= Process with each case =======================
	# path_data_total = GetFileTotal(path_data, date)
	# print(path_data_total)
	# with open (path_data_total,'r') as f:
	# 	data_total = json.load(f)
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
		print('TOTAL: ', len(data_total['TOTAL']))		

		with open (path_data_un_map,'r') as f:
			data_total['UN_CAMPAIGN'] = json.load(f)
		print('UN_CAMPAIGN: ', len(data_total['UN_CAMPAIGN']))



	#============ Case 0: Release camp in list change real date ===============
		if (len(list_plan_change_real_date) > 0):
			print("=========== Case 0: Release camp in list change real date ==========")
			list_plan_change_real_date = mapping.AddProductCode(path_data, list_plan_change_real_date, date)		
			data_total, camp_insert_unmap, remove_manual = ReleaseCampOfPlanRealDate(path_data, date, list_plan_change_real_date, data_total)

			# insert_install.InsertInstallToPlan(path_data, connect, date)
			# insert_install_brandingGPS.AddBrandingGPSToPlan(path_data, connect, date)

			list_camp_insert_unmap.extend(camp_insert_unmap)			
			list_remove_manual.extend(remove_manual)




		#======== Case 1: Data update can map
		if (len(list_plan_map) > 0):
			print("=========== Case 1: Data update can map (not change real date) ==========")
			# for plan in list_plan_map:
			# 	print(plan)
			list_plan_map = mapping.AddProductCode(path_data, list_plan_map, date)		

			list_plan_modified = GetPlanModified(connect, path_data)
			data_total, camp_remove_unmap, camp_insert_unmap, plan_remove_total, \
			plan_remove_map, plan_remove_unmap, plan_insert_unmap, \
			data_insert_map, plan_insert_total, remove_manual = ModifiedPlanToMap(path_data, date, list_plan_map, list_plan_modified, data_total)

			list_camp_remove_unmap.extend(camp_remove_unmap)
			list_camp_insert_unmap.extend(camp_insert_unmap)
			list_plan_remove_total.extend(plan_remove_total)
			list_plan_remove_map.extend(plan_remove_map)
			list_plan_remove_unmap.extend(plan_remove_unmap)
			list_plan_insert_unmap.extend(plan_insert_unmap)
			list_data_insert_map.extend(data_insert_map)
			list_plan_insert_total.extend(plan_insert_total)
			list_remove_manual.extend(remove_manual)

		# #======== Case 2: Data update can map
		if (len(list_plan_change_real_date) > 0):
			print("=========== Case 2: Data update can map (change real date) ==========")
			# for plan in list_plan_change_real_date:
			# 	print(plan)
			list_plan_change_real_date = mapping.AddProductCode(path_data, list_plan_change_real_date, date)		
			# list_plan_change_real_date = nru.Add_NRU_into_list(connect, list_plan_change_real_date, date)  
			
			
			data_total, camp_remove_unmap, data_insert_map, \
			plan_update_map, plan_remove_unmap, \
			plan_insert_total, plan_update_total  = ChangeRealDatePlanToMap(path_data, date, list_plan_change_real_date, data_total)

			list_camp_remove_unmap.extend(camp_remove_unmap)
			list_data_insert_map.extend(data_insert_map)
			list_plan_update_map.extend(plan_update_map)
			list_plan_remove_unmap.extend(plan_remove_unmap)
			list_plan_insert_total.extend(plan_insert_total)
			list_plan_update_total.extend(plan_update_total)
		

		# #======== Case 3: New Plan	
		if (len(list_plan_new) > 0):
			print("=========== Case 3: New Plan	 ================")
			list_plan_new = mapping.AddProductCode(path_data, list_plan_new, date)		
			# list_plan_new = nru.Add_NRU_into_list(connect, list_plan_new, date)  			
			data_total, camp_remove_unmap, plan_insert_total, data_insert_map, plan_insert_unmap = NewPlan(path_data, date, list_plan_new, data_total)

			list_camp_remove_unmap.extend(camp_remove_unmap)
			list_plan_insert_total.extend(plan_insert_total)
			list_data_insert_map.extend(data_insert_map)
			list_plan_insert_unmap.extend(plan_insert_unmap)



		# #============== Case 4: Data update not map ===================
		if (len(list_plan_update) > 0):		
			print("=========== Case 4: Data update not map	 ======================")
			# for plan in list_plan_update:
			# 	print(plan)
			list_plan_update = mapping.AddProductCode(path_data, list_plan_update, date)		
			# list_plan_update = nru.Add_NRU_into_list(connect, list_plan_update, date) 

			data_total, plan_update_total = UpdatePlan(data_total, list_plan_update)
			list_plan_update_total.extend(plan_update_total)
			



		# # =============== COMPUTE MONTHLY FOR EACH TOTAL PLAN ===================		
		start = time.time()
		data_total['TOTAL'] = insert_to_total.CaculatorForPlan(data_total['TOTAL'])
		
		data_total['TOTAL'] = insert_install.InsertInstallToPlan(data_total['TOTAL'], connect, date)
		data_total['TOTAL'] = insert_install_brandingGPS.AddBrandingGPSToPlan(data_total['TOTAL'], connect, date)
		print('Compute MONTHLY time: ', time.time() - start)
		# # with open (path_data_total,'w') as f:
		# # 	json.dump(data_total, f)


		# # ============== Write plan new verson into file plan.json ==========================
		# # ReadPlanFromTable(connect, path_data, date)
		# # nru.Add_NRU_into_plan(connect, path_data, date)

	print('list_plan_new: ', len(list_plan_new))
	print('list_plan_map: ', len(list_plan_map))
	print('list_plan_change_real_date: ', len(list_plan_change_real_date))
	print('list_plan_update: ', len(list_plan_update))
	print()
	print()

		# print('list_camp_remove_unmap: ', len(list_camp_remove_unmap))
		# print('list_camp_insert_unmap: ', len(list_camp_insert_unmap))
		# print('list_plan_insert_total: ', len(list_plan_insert_total))
		# print('list_plan_update_total: ', len(list_plan_update_total))
		# print('list_plan_remove_total: ', len(list_plan_remove_total))
		# print('list_data_insert_map: ', len(list_data_insert_map))
		# print('list_data_remove_map: ', len(list_data_remove_map))
		# print('list_plan_update_map: ', len(list_plan_update_map))
		# print('list_plan_remove_map: ', len(list_plan_remove_map))
		# print('list_plan_insert_unmap: ', len(list_plan_insert_unmap))
		# print('list_plan_remove_unmap: ', len(list_plan_remove_unmap))
		# print()
	
	return list_camp_remove_unmap, list_camp_insert_unmap, list_plan_insert_total, \
	list_plan_update_total, list_plan_remove_total, list_data_insert_map, \
	list_data_remove_map, list_plan_update_map, list_plan_remove_map, \
	list_plan_insert_unmap, list_plan_remove_unmap, list_remove_manual


connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-10-31' 
# date = '2017-03-01' 
path_log = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/log_plan_change.txt'


list_camp_remove_unmap, list_camp_insert_unmap, list_plan_insert_total, \
	list_plan_update_total, list_plan_remove_total, list_data_insert_map, \
	list_data_remove_map, list_plan_update_map, list_plan_remove_map, \
	list_plan_insert_unmap, list_plan_remove_unmap, list_remove_manual = ClassifyPlan(connect, path_data, date, path_log)












# import insert_report_monthly_detail as monthly_detail
# import insert_report_monthly_sum as monthly_sum
# import insert_report_plan_sum as plan_sum
# import insert_report_detail_map as detail_map

# def merger_data_plan_change(connect, list_camp_remove_unmap, list_camp_insert_unmap, \
# 	list_plan_insert_total, list_plan_update_total, list_plan_remove_total, \
# 	list_data_insert_map, list_data_remove_map, list_plan_update_map, \
# 	list_plan_remove_map, list_plan_insert_unmap, list_plan_remove_unmap):

# 	# ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
# 	cursor = conn.cursor()


# 	import time
# 	start = time.time()
# 	#=================== List campaign remove from table detail_unmap =====================
# 	if (len(list_camp_remove_unmap) > 0):
# 		for camp in list_camp_remove_unmap:
# 			detail_map.DeleteCamp(camp, cursor)
# 	print ("Time for list_camp_remove_unmap: ", (time.time() - start))


# 	start = time.time()
# 	#=================== List campaign insert into detail_unmap =====================
# 	if (len(list_camp_insert_unmap) > 0):
# 		for camp in list_camp_insert_unmap:
# 			value = detail_map.ConvertJsonCamp(camp)
# 			detail_map.InsertDetailUnmap(value, cursor)
# 	print ("Time for list_camp_insert_unmap : ", (time.time() - start))


# 	start = time.time()
# 	# =============== List plan insert into total ============================
# 	if (len(list_plan_insert_total) > 0):
# 		for plan in list_plan_insert_total:
# 			json_ = plan_sum.ConvertJsonPlanSum(plan)
# 			plan_sum.InsertPlanSum(json_, cursor)

# 			if ('MONTHLY' in plan):
# 				for i in range(len(plan['MONTHLY'])):
# 					json_ = monthly_sum.ConvertJsonMonthlySum(i, plan)
# 					monthly_sum.InsertMonthlySum(json_, cursor)

# 					json_ = monthly_detail.ConvertJsonMonthlyDetail(i, plan)
# 					monthly_detail.InsertMonthlyDetail(json_, cursor)
# 	print ("Time insert PLAN (map) into 3 report : ", (time.time() - start))


# 	start = time.time()
# 	# ============ List plan update into total =====================
# 	if (len(list_plan_update_total) > 0):
# 		for plan in list_plan_update_total:
# 			json_ = plan_sum.ConvertJsonPlanSum(plan)
# 			plan_sum.UpdatePlanPlanSum(json_, cursor)

# 			if ('MONTHLY' in plan):
# 				for i in range(len(plan['MONTHLY'])):
# 					json_ = monthly_sum.ConvertJsonMonthlySum(i, plan)
# 					monthly_sum.UpdatePlanMonthlySum(json_, cursor)

# 					json_ = monthly_detail.ConvertJsonMonthlyDetail(i, plan)
# 					monthly_detail.UpdatePlanMonthlyDetail(json_, cursor)
# 	print ("Time update PLAN into 3 report : ", (time.time() - start))



# 	start = time.time()
# 	# ============ List plan remove from total =====================
# 	if (len(list_plan_remove_total) > 0):
# 		for plan in list_plan_remove_total:			
# 			plan_sum.DeletePlanSum(plan, cursor)			
# 			monthly_sum.DeleteMonthlySum(plan, cursor)			
# 			monthly_detail.DeleteMonthlyDetail(plan, cursor)
# 	print ("Time remove PLAN into 3 report : ", (time.time() - start))


# 	start = time.time()
# 	#=================== List data insert into detail_unmap =====================
# 	if (len(list_data_insert_map) > 0):
# 		for plan in list_data_insert_map:
# 			value = detail_map.ConvertJsonMap(plan)
# 			detail_map.InsertDetailUnmap(value, cursor)
# 	print ("Time insert for list_data_insert_map : ", (time.time() - start))


# 	start = time.time()
# 	#=================== List data remove from detail_unmap =====================
# 	if (len(list_data_remove_map) > 0):
# 		for data in list_data_remove_map:
# 			detail_map.DeleteCamp(data, cursor)
# 	print ("Time remove for list_data_remove_map : ", (time.time() - start))



# 	start = time.time()
# 	#=================== List data update into detail_unmap =====================
# 	if (len(list_plan_update_map) > 0):
# 		for plan in list_plan_update_map:
# 			# _json = ConvertJsonMap(plan)
# 			detail_map.UpdatePlanDetail(_json, cursor)
# 	print ("Time update DETAIL MAP : ", (time.time() - start))


# 	start = time.time()
# 	#=================== List data remove from detail_unmap =====================
# 	if (len(list_plan_remove_map) > 0):
# 		for plan in list_plan_remove_map:
# 			detail_map.DeleteCamp(data, cursor)
# 	print ("Time remove DATA MAP : ", (time.time() - start))


# 	start = time.time()
# 	#=================== List data insert into detail_unmap =====================
# 	if (len(list_plan_insert_unmap) > 0):
# 		for plan in list_plan_insert_unmap:
# 			value = detail_map.ConvertJsonPlan(plan)
# 			detail_map.InsertDetailUnmap(value, cursor)
# 	print ("Time insert UN_PLAN : ", (time.time() - start))


# 	start = time.time()
# 	#=================== List data remove from detail_unmap =====================
# 	if (len(list_plan_remove_unmap) > 0):
# 		for plan in list_plan_remove_unmap:
# 			detail_map.DeletePlan(value)
# 	print ("Time remove UN_PLAN : ", (time.time() - start))
	

# 	#=================== Commit and close connect =================
# 	conn.commit()
# 	print("Committed!.......")
# 	cursor.close()


































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


#============  Get file =========================================

# path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA/2017-10-25/PLAN/plan.json'
# with open(path, 'r') as fi:
# 	data = json.load(fi)


# for plan in data['plan']:
# 	if plan['PRODUCT'] == '221' and plan['REASON_CODE_ORACLE'] == '1703048' :
# 		print(plan)

# 	if plan['REASON_CODE_ORACLE'] == '1702073':
# 		print(plan)

# 	if plan['REASON_CODE_ORACLE'] == '1709089':
# 		print(plan)

# 	if plan['REASON_CODE_ORACLE'] == '1709140':
# 		print(plan)






# print(len(data['TOTAL'])
# print(len(data['MAP'])
# print(len(data['UN_PLAN'])
# print(len(data['UN_CAMPAIGN'])


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
	print(path_plan)
	with open(path_plan, 'r') as fi:
		data_plan = json.load(fi)


	print('list_old_plan: ',len(data_plan['plan']))
	print('list_new_plan: ', len(list_new_plan))	
	
	
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
			
				



	print('list_diff: ', len(list_diff))
	print('list_plan_new: ', len(list_plan_new))
	print('list_plan_only_update: ', len(list_plan_only_update))	
	print('list_plan_change_real: ', len(list_plan_change_real))
	

		




connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
date = '2017-10-31' 

import time
start = time.time()
GetListDiff(connect, path_data, date)
print('Time GetListDiff:',  time.time() - start)

# s = 'FA-PA170525004\xa0'
# print(s)
# print(s.find('\xa0'))
# print(s.replace('\xa0', ''))


					







