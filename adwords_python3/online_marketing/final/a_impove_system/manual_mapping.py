import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

#-------------- import file ---------------
import insert_data_map_to_total as insert_data
import mapping_campaign_plan as mapping

import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install




def ParseFormatDate(data):
	# print (data)
	if (data is None):
		return None
	temp = data.split('-')
	d = temp[2] + '-' + temp[0] + '-' + temp[1] 
	d = str(datetime.strptime(d, '%Y-%m-%d').date())
	return d

def InsertPlanToDataBase(connect, plan):
	return 0



def ParseLogManualToJson(log):
	temp = {
		'PRODUCT' : log[0],
		'REASON_CODE_ORACLE' : log[1],
		'FORM_TYPE' : log[2],
		'UNIT_OPTION' : log[3],
		'USER_NAME' : log[4],
		'ACCOUNT_ID' : log[5],
		'CAMPAIGN_ID' : log[6],
		'START_DATE' : log[7],
		'END_DATE' : log[8]
	}
	return temp

def ReadTableManualMap(connect, path_data, date):
	path_folder = os.path.join(path_data, str(date) + '/LOG_MANUAL')
	path_data_total_map = os.path.join(path_folder, 'log_manual.json')
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)

	if not os.path.exists(path_data_total_map):
		data_manual_map = {}
		data_manual_map['MANUAL_MAP'] = []
		with open (path_data_total_map,'w') as f:
			json.dump(data_manual_map, f)

	with open (path_data_total_map,'r') as f:
		data_manual_map = json.load(f)
	if 'LOG' not in data_manual_map:
		data_manual_map['LOG'] = []
	manual_map = data_manual_map['LOG']
	 # ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()
	
	statement = "select PRODUCT, REASON_CODE_ORACLE, \
					EFORM_TYPE, UNIT_OPTION, \
					USER_NAME, ACCOUNT_ID, CAMPAIGN_ID, \
					TO_CHAR(START_DATE, 'YYYY-MM-DD'), TO_CHAR(END_DATE, 'YYYY-MM-DD') from ODS_CAMP_FA_MAPPING_GG"
	cursor.execute(statement)
	log_manual = cursor.fetchall()

	list_diff = []
	list_plan_diff = []
	list_out = []
	#------------- Check manual map change --------------------
	# print (log_manual)
	print (len(log_manual))
	for data in log_manual:
		if data[0] != None and data[1] != None \
		and data[2] != None and data[3] != None \
		and data[6] != None and data[7] != None and data[8] != None:
			# print (data)
			list_out.append(ParseLogManualToJson(data))
			flag = True
			# print (data[6])
			# print (type(data[6]))
			for data_local in manual_map:
				if data[0] == data_local['PRODUCT'] \
				and data[1] == data_local['REASON_CODE_ORACLE'] \
				and data[2] == data_local['FORM_TYPE'] \
				and data[3] == data_local['UNIT_OPTION'] \
				and data[6] == data_local['CAMPAIGN_ID'] \
				and data[7] == data_local['START_DATE'] \
				and data[8] == data_local['END_DATE']:
					# print ("---------------- Trung log")
					flag = False
			if flag:
				temp = ParseLogManualToJson(data)
				# print (temp)
				list_diff.append(temp)
				print ("--------------- Da add them ---------------")
	# print (list_diff)

	#--------------- Write file manual log -------------------
	data_manual_map['LOG'] = list_out
	with open (path_data_total_map,'w') as f:
		json.dump(data_manual_map, f)


	# ------------ Cần đọc thông tin plan mới nhất --------------------
	# mapping.ReadPlanFromTable(connect, path_data, str(date))
	# mapping.ReadProductAlias(connect, path_data, str(date))
	# nru.Add_NRU_into_plan(connect, path_data, date)  
	list_plan = mapping.ReadPlan(path_data, str(date))


	# mapping.ReadPlanFromTable(connect, path_data, str(date))
	# mapping.ReadProductAlias(connect, path_data, str(date))
	# # nru.Add_NRU_into_plan(connect, path_data, date)  
	# list_plan = mapping.ReadPlan(path_data, str(date))
	# list_plan['plan'] = mapping.AddProductCode(path_data, list_plan['plan'], date)

	# print (data_manual_map)
	# print (list_diff)
	# --------------- Get info plan ------------
	list_plan_diff = []
	plan_temp = None
	list_plan_new = []
	# print (list_diff)
	for plan in list_diff:
		# ----------- Create data campaign ----------------
		campaign = {}
		campaign['CAMPAIGN_ID'] = plan['CAMPAIGN_ID']
		campaign['START_DATE_MANUAL_MAP'] = plan['START_DATE']
		campaign['END_DATE_MANUAL_MAP'] = plan['END_DATE']
		campaign['USER_MAP'] = plan['USER_NAME']
		campaign['STATUS'] = 'USER'
		flag = True
		for plan_info in list_plan['plan']:

			if int(plan['PRODUCT']) == int(plan_info['PRODUCT']) \
				and plan['REASON_CODE_ORACLE'] == plan_info['REASON_CODE_ORACLE']:
				plan_temp = plan_info
				if plan['UNIT_OPTION'] == plan_info['UNIT_OPTION'] and plan['FORM_TYPE'] == plan_info['FORM_TYPE']:
					temp = plan_temp.copy()

					temp['CAMPAIGN_MANUAL_MAP'] = []
					temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
					list_plan_diff.append(temp)
					flag = False
		# ----------- Plan moi duoc tao -----------------
		if flag:
			temp = plan_temp.copy()
			temp['UNIT_OPTION'] = plan['UNIT_OPTION']
			temp['FORM_TYPE'] = plan['FORM_TYPE']
			temp['CAMPAIGN_MANUAL_MAP'] = []
			temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
			temp['USER_MAP'] = plan['USER_NAME']
			temp['STATUS'] = 'USER'
			list_plan_diff.append(temp)
			list_plan_new.append(temp)
	print (len(list_plan_diff))
	return (list_plan_diff)



def ChooseTimeManualMap(plan):
	if plan['REAL_START_DATE'] is not None:
		start_plan = datetime.strptime(plan['REAL_START_DATE'], '%Y-%m-%d').date()
	else:
		start_plan = datetime.strptime(plan['START_DAY'], '%Y-%m-%d').date()
		
	if plan['REAL_END_DATE'] is not None:
		end_plan = datetime.strptime(plan['REAL_END_DATE'], '%Y-%m-%d').date()
	else:
		end_plan = datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d').date()

	#------------ Lay time start và end  ----------------------
	# start_plan = datetime.strptime(plan['START_DAY'], '%Y-%m-%d').date()
	# end_plan = datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d').date()

	start_camp = datetime.strptime(plan['CAMPAIGN_MANUAL_MAP'][0]['START_DATE_MANUAL_MAP'], '%Y-%m-%d').date()
	end_camp = datetime.strptime(plan['CAMPAIGN_MANUAL_MAP'][0]['END_DATE_MANUAL_MAP'], '%Y-%m-%d').date()

	if start_plan < start_camp:
		start = start_camp
	else:
		start = start_plan

	if end_plan > end_camp:
		end = end_camp
	else:
		end = end_plan

	return (start, end)

#-------- Vào data unmap sum các camp cho một plan ----------
def GetCampaignUnMapForPlan(plan, data_total):
	import time
	# start_time = time.time()
	# # with open (path_data_total_map,'r') as f:
	# # 	data_total = json.load(f)
	# print ("Time load file : ", (time.time() - start_time))

	list_campaign = data_total['UN_CAMP']
	start, end = ChooseTimeManualMap(plan)
	# print (start)
	# print (end)
	list_map = []
	plan['CAMPAIGN'] = []
	list_camp_need_remove = []
	# print (len(list_campaign))
	for camp in list_campaign:
		d = datetime.strptime(camp['Date'], '%Y-%m-%d').date()
		if str(plan['CAMPAIGN_MANUAL_MAP'][0]['CAMPAIGN_ID']) == str(camp['Campaign ID']) \
		and d >= start and d <= end:
			# --------- Data map ----------
			z = camp.copy()
			z.update(plan)
			list_map.append(z)
			list_map.append(camp)

			campaign = camp.copy()
			plan['CAMPAIGN'].append(campaign)
	return (plan, list_map)


def ManualMap(connect, path_data, date):
	# # ------------- Get manual map from table log ----------------
	# list_diff = ReadTableManualMap(connect, path_data, date)
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
	
	list_map_all = []
	list_plan_remove_unmap = []
	list_plan_update = []
	list_camp_remove_unmap = []
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
		data_total['UN_CAMP'] = []
		with open (path_data_total_map,'r') as f:
			data_total['TOTAL'] = json.load(f)
		with open (path_data_un_map,'r') as f:
			data_total['UN_CAMP'] = json.load(f)

		list_plan = ReadTableManualMap(connect, path_data, date)
		print (len(list_plan))
		if len(list_plan) > 0:

			list_map_all = []
			list_plan_remove_unmap = []
			# print (len(data_total['UN_CAMPAIGN']))

			# for plan_total in list_plan:
			# 	if str(plan_total['REASON_CODE_ORACLE']) == '1708007':
			# 		print (plan_total)


			import time
			start_time = time.time()
			for plan in list_plan:
				plan, list_map = GetCampaignUnMapForPlan(plan, data_total)

				list_map_all.extend(list_map)
				# print (len(list_map))

			#----------- Remove unmap ---------------------
			for camp in list_map_all:
				for campaign in data_total['UN_CAMP']:
					if camp['Campaign ID'] == campaign['Campaign ID'] \
						and camp['Date'] == campaign['Date']:
						data_total['UN_CAMP'].remove(campaign)


			print ("Time get in manual 1 : ", (time.time() - start_time))

			data_date = {}
			data_date['PLAN'] = list_plan

			data_total, list_plan_insert, list_plan_remove = insert_data.AddToTotal (data_total, data_date, date)

			data_total['TOTAL'] = insert_data.CaculatorForPlan(data_total['TOTAL'])

			# for plan_total in data_total['TOTAL']:
			# 	if str(plan_total['REASON_CODE_ORACLE']) == '1708007':
			# 		print (plan_total)

			list_plan_remove_unmap = list_plan_remove
			list_camp_remove_unmap = list_map_all
			list_plan_update = data_total['TOTAL']

			# path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
			# with open (path_data_total_map,'w') as f:
			# 	json.dump(data_total['TOTAL'], f)

			# path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
			# with open (path_data_un_map,'w') as f:
			# 	json.dump(data_total['UN_CAMP'], f)

			print (len(data_total['UN_CAMP']))
			print (len(list_map_all))
			print (len(list_plan_remove_unmap))
			print (len(list_camp_remove_unmap))
			print (list_map_all[0]['CYEAR'])

	return (list_map_all, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update)

