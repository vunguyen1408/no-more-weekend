import os
import pandas as pd
import numpy as np
import json
# import cx_Oracle
from datetime import datetime , timedelta, date

#-------------- import file ---------------
import insert_data_map_to_total as insert_data
import mapping_campaign_plan as mapping

list_plan = [{'CYEAR': '17', 'CMONTH': '6', 'LEGAL': 'VNG', 'DEPARTMENT': '0902', 'DEPARTMENT_NAME': 'PG1',\
 'PRODUCT': '221', 'REASON_CODE_ORACLE': '1706008', 'EFORM_NO': 'FA-PA170529003', 'START_DAY': '2017-06-01', \
 'END_DAY_ESTIMATE': '2017-06-30', 'CHANNEL': 'GG', 'FORM_TYPE': 'SEARCH', 'UNIT_OPTION': 'CPI', 'UNIT_COST': '1.3', \
 'AMOUNT_USD': 39000, 'CVALUE': 30000, 'ENGAGEMENT': None, 'IMPRESSIONS': None, 'CLIKE': None, 'CVIEWS': None, \
 'INSTALL': 30000, 'NRU': None, 'INSERT_DATE': '2017-09-13', \
 'CAMPAIGN_MANUAL_MAP': [{'CAMPAIGN_ID': '682545537', 'UPDATE_DATE': '2017-09-27'}], \
 'USER_MAP': 'HIENNTV'}]


def ParseFormatDate(date):
	temp = date.split('/')
	d = temp[2] + '-' + temp[0] + '-' + temp[1] 
	d = str(datetime.strptime(d, '%Y-%m-%d').date())
	return d

def InsertPlanToDataBase(connect, plan):
	return 0
#-------------- Read database , lấy các data thay đổi, thay đổi bảng plan  ----------
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

	print (path_data_total_map)
	with open (path_data_total_map,'r') as f:
		data_manual_map = json.load(f)

	 # ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()
	
	statement = "select PRODUCT, REASON_CODE_ORACLE, \
					EFORM_TYPE, UNIT_OPTION, \
					USER_NAME, USER_CAMPAIGN_ID, \
					TO_CHAR(UPDATE_DATE, 'YYYY-MM-DD'), START_DATE, END_DATE from CAMPAIGN_MAP_HIS_GG"

	cursor.execute(statement)
	log_manual = cursor.fetchall()

	print (log_manual)
	list_diff = []
	#------------- Check manual map change ---------------------
	if (len(log_manual) != len(data_manual_map) or (data_manual_map['MANUAL_MAP'] == [])):
		for data in log_manual:
			flag = True
			for data_local in data_manual_map:
				if data[0] == data_local[0] \
				and data[1] == data_local[1] \
				and data[2] == data_local[2] \
				and data[3] == data_local[3] \
				and data[4] == data_local[4] \
				and data[5] == data_local[5] \
				and ParseFormatDate(data[7]) == data_local[7] \
				and ParseFormatDate(data[8]) == data_local[8]:
					print ("---------------- Trung log")
					flag = False
			if flag:
				temp = list(data)
				temp[7] = ParseFormatDate(data[7])
				temp[8] = ParseFormatDate(data[8])
				list_diff.append(list(temp))
				print ("--------------- Da add them")

	list_plan = mapping.ReadPlan(path_data)
	# --------------- Get info plan ------------
	list_plan_diff = []
	plan_temp = None
	list_plan_new = []
	for plan in list_diff:
		# ----------- Create data campaign ----------------
		campaign = {}
		campaign['CAMPAIGN_ID'] = plan[5]
		campaign['UPDATE_DATE'] = str(plan[6])
		flag = True
		for plan_info in list_plan['plan']:
			# print (plan_info['PRODUCT'])
			# print (plan[0])
			# print (plan[1])
			# print (plan_info['REASON_CODE_ORACLE'])
			if int(plan[0]) == int(plan_info['PRODUCT']) \
				and plan[1] == plan_info['REASON_CODE_ORACLE']:
				plan_temp = plan_info
				if plan[3] == plan_info['UNIT_OPTION'] and plan[2] == plan_info['FORM_TYPE']:
					temp = plan_info

					temp['CAMPAIGN_MANUAL_MAP'] = []
					temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
					temp['USER_MAP'] = plan[4]
					list_plan_diff.append(temp)
					flag = False
		# ----------- Plan moi duoc tao -----------------
		if flag:
			temp = plan_temp
			temp['UNIT_OPTION'] = plan[3]
			temp['FORM_TYPE'] = plan[2]
			temp['CAMPAIGN_MANUAL_MAP'] = []
			temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
			temp['USER_MAP'] = plan[4]
			temp['STATUS'] = 'USER'
			list_plan_diff.append(temp)
			list_plan_new.append(temp)
	print (list_plan_diff)

	#------------------------ Insert database ------------------------------
	connect = ''
	for plan in list_plan_new:
		InsertPlanToDataBase(connect, plan)
	print (list_plan_diff)
	return (list_plan_diff)

#--- Vào data unmap sum các camp cho một plan ----------
def GetCampaignUnMapForPlan(path_data, plan):
	path_data_total_map = os.path.join(path_data + '/DATA_MAPPING', 'total_mapping' + '.json')
	print (plan)

	if not os.path.exists(path_data_total_map):
		data_total = {}
		data_total['TOTAL'] = []
		data_total['MAP'] = []
		data_total['UN_PLAN'] = []
		data_total['UN_CAMPAIGN'] = []
		with open (path_data_total_map,'w') as f:
			json.dump(data_total, f)

	with open (path_data_total_map,'r') as f:
		data_total = json.load(f)

	list_campaign = data_total['UN_CAMPAIGN']
	start = datetime.strptime(plan['START_DAY'], '%Y-%m-%d').date()
	end = datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d').date()

	list_map = []
	list_camp = []
	plan['CAMPAIGN'] = []
	list_camp_need_remove = []
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

	for camp in list_camp_need_remove:
		list_campaign.remove(camp)
	plan_sum, list_map_temp = insert_data.SumTotalPlan(plan, list_camp)
	print (list_campaign)
	return (plan_sum, list_map, list_camp_need_remove)


def GetCampaignUnMapForManualMap(connect, path_data, date):
	# ------------- Get manual map from table log ----------------
	list_diff = ReadTableManualMap(connect, path_data, date)

	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
	with open (path_data_total_map,'r') as f:
		data_total = json.load(f)

	list_camp_remove = []
	for plan in list_plan:
		plan, list_map, list_camp_need_remove = GetCampaignUnMapForPlan(path_data, plan)
		#------------- Insert data map ------------
		data_total['MAP'].extend(list_map)

		#----------- Remove unmap ---------------------
		list_camp_remove.extend(list_camp_need_remove)
		for camp in list_camp_need_remove:
			for campaign in data_total['UN_CAMPAIGN']:
				if camp['Campaign ID'] == campaign['Campaign ID'] \
					and camp['Date'] == campaign['Date']:
					data_total['UN_CAMPAIGN'].remove(campaign)


	#------------- Insert total ------------
	for plan in list_diff:
		flag = True
		for plan_total in data_total['TOTAL']:
			print (plan)
			print (plan_total)
			if plan_total['PRODUCT'] == plan['PRODUCT'] \
				and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
				and plan_total['FORM_TYPE'] == plan['FORM_TYPE']:
				plan_total['TOTAL_CAMPAIGN'] = insert_data.SumTwoTotal(plan_total['TOTAL_CAMPAIGN'], plan['TOTAL_CAMPAIGN'])
				flag = False

		#----- Không tìm thấy trong total ------
		if flag:
			# --------------- Tạo các thông tin month cho plan trước khi add --------------
			data_total['TOTAL'].append(plan)

	# --------------- Tinh total month cho cac plan --------------
	for plan in data_total['TOTAL']:
		plan = insert_data.CaculatorTotalMonth(plan, date)

	with open (path_data_total_map,'w') as f:
		json.dump(data_total, f)

	list_map_remove = []
	list_plan_remove = []
	return (list_plan_remove, list_plan_remove, list_camp_remove)


# connect = ''
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA'
# date = '2017-06-30'
# GetCampaignUnMapForManualMap(connect, path_data, date)