import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

#-------------- import file ---------------
import insert_data_map_to_total as insert_data
import mapping_campaign_plan as mapping
import insert_nru_to_data as nru

# list_plan = [{'CYEAR': '17', 'CMONTH': '6', 'LEGAL': 'VNG', 'DEPARTMENT': '0902', 'DEPARTMENT_NAME': 'PG1',\
#  'PRODUCT': '221', 'REASON_CODE_ORACLE': '1706008', 'EFORM_NO': 'FA-PA170529003', 'START_DAY': '2017-06-01', \
#  'END_DAY_ESTIMATE': '2017-06-30', 'CHANNEL': 'GG', 'FORM_TYPE': 'SEARCH', 'UNIT_OPTION': 'CPI', 'UNIT_COST': '1.3', \
#  'AMOUNT_USD': 39000, 'CVALUE': 30000, 'ENGAGEMENT': None, 'IMPRESSIONS': None, 'CLIKE': None, 'CVIEWS': None, \
#  'INSTALL': 30000, 'NRU': None, 'INSERT_DATE': '2017-09-13', \
#  'CAMPAIGN_MANUAL_MAP': [{'CAMPAIGN_ID': '682545537', 'UPDATE_DATE': '2017-09-27'}], \
#  'USER_MAP': 'HIENNTV'}]


def ParseFormatDate(data):
	print (data)
	if (data is None):
		return None
	temp = data.split('-')
	d = temp[2] + '-' + temp[0] + '-' + temp[1] 
	d = str(datetime.strptime(d, '%Y-%m-%d').date())
	return d

def InsertPlanToDataBase(connect, plan):
	return 0
#-------------- Read database , lấy các data thay đổi, thay đổi bảng plan  ----------
# def ReadTableManualMap(connect, path_data, date):
# 	path_folder = os.path.join(path_data, str(date) + '/LOG_MANUAL')
# 	path_data_total_map = os.path.join(path_folder, 'log_manual.json')
# 	if not os.path.exists(path_folder):
# 		os.makedirs(path_folder)

# 	if not os.path.exists(path_data_total_map):
# 		data_manual_map = {}
# 		data_manual_map['MANUAL_MAP'] = []
# 		with open (path_data_total_map,'w') as f:
# 			json.dump(data_manual_map, f)

# 	with open (path_data_total_map,'r') as f:
# 		data_manual_map = json.load(f)

# 	 # ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()
	
# 	statement = "select PRODUCT, REASON_CODE_ORACLE, \
# 					EFORM_TYPE, UNIT_OPTION, \
# 					USER_NAME, USER_CAMPAIGN_ID, \
# 					TO_CHAR(UPDATE_DATE, 'YYYY-MM-DD'), START_DATE, END_DATE from CAMPAIGN_MAP_HIS_GG"

# 	cursor.execute(statement)
# 	log_manual = cursor.fetchall()

# 	list_diff = []
# 	#------------- Check manual map change ---------------------
# 	if (len(log_manual) != len(data_manual_map) or (data_manual_map['MANUAL_MAP'] == [])):
# 		for data in log_manual:
# 			flag = True
# 			for data_local in data_manual_map:
# 				if data[0] == data_local[0] \
# 				and data[1] == data_local[1] \
# 				and data[2] == data_local[2] \
# 				and data[3] == data_local[3] \
# 				and data[4] == data_local[4] \
# 				and data[5] == data_local[5] \
# 				and ParseFormatDate(data[7]) == data_local[7] \
# 				and ParseFormatDate(data[8]) == data_local[8]:
# 					print ("---------------- Trung log")
# 					flag = False
# 			if flag:
# 				temp = list(data)
# 				temp[7] = ParseFormatDate(data[7])
# 				temp[8] = ParseFormatDate(data[8])
# 				list_diff.append(list(temp))
# 				print ("--------------- Da add them")

# 	list_plan = mapping.ReadPlan(path_data, str(date))
# 	# --------------- Get info plan ------------
# 	list_plan_diff = []
# 	plan_temp = None
# 	list_plan_new = []
# 	for plan in list_diff:
# 		# ----------- Create data campaign ----------------
# 		campaign = {}
# 		campaign['CAMPAIGN_ID'] = plan[5]
# 		campaign['UPDATE_DATE'] = str(plan[6])
# 		flag = True
# 		for plan_info in list_plan['plan']:
# 			# print (plan_info['PRODUCT'])
# 			# print (plan[0])
# 			# print (plan[1])
# 			# print (plan_info['REASON_CODE_ORACLE'])
# 			if int(plan[0]) == int(plan_info['PRODUCT']) \
# 				and plan[1] == plan_info['REASON_CODE_ORACLE']:
# 				plan_temp = plan_info
# 				if plan[3] == plan_info['UNIT_OPTION'] and plan[2] == plan_info['FORM_TYPE']:
# 					temp = plan_info

# 					temp['CAMPAIGN_MANUAL_MAP'] = []
# 					temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
# 					temp['USER_MAP'] = plan[4]
# 					list_plan_diff.append(temp)
# 					flag = False
# 		# ----------- Plan moi duoc tao -----------------
# 		if flag:
# 			temp = plan_temp
# 			temp['UNIT_OPTION'] = plan[3]
# 			temp['FORM_TYPE'] = plan[2]
# 			temp['CAMPAIGN_MANUAL_MAP'] = []
# 			temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
# 			temp['USER_MAP'] = plan[4]
# 			temp['STATUS'] = 'USER'
# 			list_plan_diff.append(temp)
# 			list_plan_new.append(temp)

# 	#------------------------ Insert database ------------------------------
# 	connect = ''
# 	for plan in list_plan_new:
# 		InsertPlanToDataBase(connect, plan)
# 	print (list_plan_diff)
# 	return (list_plan_diff)


def ParseLogManualToJson(log):
	temp = {
		'PRODUCT' : log[0],
		'REASON_CODE_ORACLE' : log[1],
		'EFORM_TYPE' : log[2],
		'UNIT_OPTION' : log[3],
		'USER_NAME' : log[4],
		'ACCOUNT_ID' : log[5],
		'CAMPAIGN_ID' : log[6],
		'START_DATE' : log[7],
		'END_DATE' : log[8]
	}
	# temp['PRODUCT'] = log[0]
	# temp['REASON_CODE_ORACLE'] = log[1]
	# temp['EFORM_TYPE'] = log[2]
	# temp['UNIT_OPTION'] = log[3]
	# temp['USER_NAME'] = log[4]
	# temp['ACCOUNT_ID'] = log[5]
	# temp['CAMPAIGN_ID'] = log[6]
	# temp['START_DATE'] = log[7]
	# temp['END_DATE'] = log[8]
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
	for data in log_manual:
		# print (data)
		list_out.append(ParseLogManualToJson(data))
		flag = True
		# print (data[6])
		# print (type(data[6]))
		for data_local in manual_map:
			if data[0] == data_local['PRODUCT'] \
			and data[1] == data_local['REASON_CODE_ORACLE'] \
			and data[2] == data_local['EFORM_TYPE'] \
			and data[3] == data_local['UNIT_OPTION'] \
			and data[6] == data_local['CAMPAIGN_ID'] \
			and data[7] == data_local['START_DATE'] \
			and data[8] == data_local['END_DATE']:
				print ("---------------- Trung log")
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

	list_plan = mapping.ReadPlan(path_data, str(date))
	# print (list_diff)
	# --------------- Get info plan ------------
	list_plan_diff = []
	plan_temp = None
	list_plan_new = []
	for plan in list_diff:
		# ----------- Create data campaign ----------------
		campaign = {}
		campaign['CAMPAIGN_ID'] = plan['CAMPAIGN_ID']
		campaign['START_DATE_MANUAL_MAP'] = plan['START_DATE']
		campaign['END_DATE_MANUAL_MAP'] = plan['END_DATE']
		campaign['USER_MAP'] = plan['USER_NAME']
		campaign['STATUS'] = 'USER'
		# campaign['UPDATE_DATE'] = str(plan[6])
		flag = True
		for plan_info in list_plan['plan']:
			# print (plan_info['PRODUCT'])
			# print (plan[0])
			# print (plan[1])
			# print (plan_info['REASON_CODE_ORACLE'])
			if int(plan['PRODUCT']) == int(plan_info['PRODUCT']) \
				and plan['REASON_CODE_ORACLE'] == plan_info['REASON_CODE_ORACLE']:
				plan_temp = plan_info
				if plan['UNIT_OPTION'] == plan_info['UNIT_OPTION'] and plan['EFORM_TYPE'] == plan_info['FORM_TYPE']:
					temp = plan_temp.copy()
					print (temp)
					print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

					temp['CAMPAIGN_MANUAL_MAP'] = []
					temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
					list_plan_diff.append(temp)
					flag = False
		# ----------- Plan moi duoc tao -----------------
		if flag:
			temp = plan_temp.copy()
			temp['UNIT_OPTION'] = plan[3]
			temp['FORM_TYPE'] = plan[2]
			temp['CAMPAIGN_MANUAL_MAP'] = []
			temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
			temp['USER_MAP'] = plan[4]
			temp['STATUS'] = 'USER'
			list_plan_diff.append(temp)
			list_plan_new.append(temp)

	#------------------------ Insert database plan new------------------------------
	# connect = ''
	# # list_plan_new chua duy nhat can check lai
	# for plan1 in list_plan_new:
	# 	for plan2 in list_plan_new:
	# 		if plan1['PRODUCT'] == plan2['PRODUCT'] \
	# 			and plan1['REASON_CODE_ORACLE'] == plan2['REASON_CODE_ORACLE'] \
	# 			and plan1['FORM_TYPE'] == plan2['FORM_TYPE']:

	# print (list_plan_diff)
	# for plan in list_plan_new:
	# 	InsertPlanToDataBase(connect, plan)
	# print (list_plan_diff)
	print (list_plan_diff)
	return (list_plan_diff)



def ChooseTimeManualMap(plan):
	#------------ Lay time start và end  ----------------------
	start_plan = datetime.strptime(plan['START_DAY'], '%Y-%m-%d').date()
	end_plan = datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d').date()

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
def GetCampaignUnMapForPlan(plan, path_data_total_map):

	with open (path_data_total_map,'r') as f:
		data_total = json.load(f)

	list_campaign = data_total['UN_CAMPAIGN']
	start, end = ChooseTimeManualMap(plan)
	# print (start)
	# print (end)
	list_map = []
	list_camp = []
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
			list_camp.append(camp)

			campaign = {}
			campaign['CAMPAIGN_ID'] = camp['Campaign ID']
			campaign['Date'] = camp['Date']
			plan['CAMPAIGN'].append(campaign)
			list_camp_need_remove.append(camp)

	list_map_temp = []
	plan_sum = []

	plan_sum, list_map_temp = insert_data.SumTotalPlan(plan, list_camp)
	print (" TEST")
	print (list_map_temp)
	print (list_camp)
	# print (list_campaign)
	return (plan_sum, list_map_temp, list_camp_need_remove)


def GetCampaignUnMapForManualMap(connect, path_data, date):
	# # ------------- Get manual map from table log ----------------
	# list_diff = ReadTableManualMap(connect, path_data, date)
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')

	# #--------------- Tim file total hien tai or ngay gan nhat -------------------------
	# if not os.path.exists(path_data_total_map):
	# 	data_total = {}
	# 	data_total['TOTAL'] = []
	# 	data_total['MAP'] = []
	# 	data_total['UN_PLAN'] = []
	# 	data_total['UN_CAMPAIGN'] = []
	# 	with open (path_data_total_map,'w') as f:
	# 		json.dump(data_total, f)
	# #-----------------------------------------------------------------------------------

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
		# ---- Neu tim thay file total truoc do -----
	else:
		find = True

	if find:
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)
		list_plan = ReadTableManualMap(connect, path_data, date)
		# print (list_plan)
		# print (len(list_plan))
		list_camp_remove_unmap = []
		list_map_all = []
		list_plan_remove_unmap = []
		# print (len(data_total['UN_CAMPAIGN']))
		for plan in list_plan:
			plan, list_map, list_camp_need_remove = GetCampaignUnMapForPlan(plan, path_data_total_map)
			# print ("--------------- gggg---------------")
			# print (plan)
			# print (list_camp_need_remove)
			# print ("---------------gggg ---------------")
			#------------- Insert data map ------------
			data_total['MAP'].extend(list_map)
			# print (len(list_camp_need_remove))

			#----------- Remove unmap ---------------------
			for camp in list_camp_need_remove:
				for campaign in data_total['UN_CAMPAIGN']:
					if camp['Campaign ID'] == campaign['Campaign ID'] \
						and camp['Date'] == campaign['Date']:
						data_total['UN_CAMPAIGN'].remove(campaign)
						list_camp_remove_unmap.append(campaign)


		# print (list_plan)

		list_plan = mapping.AddProductCode(path_data, list_plan, date)
		# list_plan = mapping.AddProductCode(path_data, list_plan, date)

		# list_plan = nru.AddNRU(path_data, list_plan, date)
		# list_plan = nru.AddNRU(path_data, list_plan, date)

		# print (len(data_total['UN_CAMPAIGN']))

		# print (list_plan)

		#------------- Insert total ------------
		for plan in list_plan:
			flag = True
			for plan_total in data_total['TOTAL']:
				if plan_total['PRODUCT'] == plan['PRODUCT'] \
					and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
					and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
					and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:
					plan_total['TOTAL_CAMPAIGN'] = insert_data.SumTwoTotal(plan_total['TOTAL_CAMPAIGN'], plan['TOTAL_CAMPAIGN'])
					flag = False

			#----- Không tìm thấy trong total ------
			if flag:
				# --------------- Tạo các thông tin month cho plan trước khi add --------------
				data_total['TOTAL'].append(plan)

			#------------- Xoa trong danh sach un map PLAN ------------------
			for plan_un in data_total['UN_PLAN']:
				if plan_un['PRODUCT'] == plan['PRODUCT'] \
					and plan_un['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
					and plan_un['FORM_TYPE'] == plan['FORM_TYPE'] \
					and plan_un['UNIT_OPTION'] == plan['UNIT_OPTION'] :
					data_total['UN_PLAN'].remove(plan_un)
					list_plan_remove_unmap.append(plan_un)

		# print (len(data_total['UN_CAMPAIGN']))

		# --------------- Tinh total month cho cac plan --------------
		print ("---------------------------------------------------")
		for plan in data_total['TOTAL']:
			plan['MONTHLY'] = {}
			plan = insert_data.CaculatorTotalMonth(plan, date)
			# print (plan)
		print ("---------------------------------------------------")

		for plan in data_total['UN_PLAN']:
			plan['MONTHLY'] = {}
			plan = insert_data.CaculatorTotalMonth(plan, date)


		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		with open (path_data_total_map,'w') as f:
			json.dump(data_total, f)

		insert_data.CreateListPlanMonthly(path_data, date)

		# print (len(data_total['UN_CAMPAIGN']))
		# print (len(list_plan_remove_unmap))
		# print (len(list_camp_remove_unmap))
		# # print (list_camp_remove_unmap)
		# print (list_plan_remove_unmap)
		return (list_map_all, list_plan_remove_unmap, list_camp_remove_unmap)


# connect = ''
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA'
# date = '2017-08-31'
# GetCampaignUnMapForManualMap(connect, path_data, date)