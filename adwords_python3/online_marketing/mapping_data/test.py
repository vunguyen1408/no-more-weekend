
import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
import json
from datetime import datetime , timedelta, date


# ==================== Connect database =======================

def ReadPlan(path_folder):
  # =============== List plan code ================  
  file_plan = os.path.join(path_folder, 'plan.json')
  list_plan = {}
  with open (file_plan, 'r') as f:
    list_plan = json.load(f)
    return list_plan


def ReadTableManualMap(connect, path_data):
	path_folder = os.path.join(path_data, 'DATA_MAPPING/LOG_MANUAL')
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
	
	statement = 'select PRODUCT, REASON_CODE_ORACLE, \
					EFORM_TYPE, UNIT_OPTION, \
					USER_NAME, USER_CAMPAIGN_ID, \
					UPDATE_DATE, START_DATE, END_DATE from CAMPAIGN_MAP_HIS_GG'

	cursor.execute(statement)
	log_manual = cursor.fetchall()

	print (log_manual)
	list_diff = []
	#------------- Check manual map change ---------------------
	if len(log_manual) != len(data_manual_map):
		for data in log_manual:
			flag = True
			for data_local in data_manual_map:
				if data[0] == data_local[0] \
				and data[1] == data_local[1] \
				and data[2] == data_local[2] \
				and data[3] == data_local[3] \
				and data[4] == data_local[4] \
				and data[5] == data_local[5] \
				and ParseFormatDate(data[6]) == ParseFormatDate(data_local[6]) \
				and ParseFormatDate(data[7]) == ParseFormatDate(data_local[7]) \
				and ParseFormatDate(data[8]) == ParseFormatDate(data_local[8]):
					flag = False
			if flag:
				list_diff.append(list(data))

	list_plan = ReadPlan(path_data)
	print (list_diff)
	# --------------- Get info plan ------------
	list_plan_diff = []
	plan_temp = None
	for plan in list_diff:
		# ----------- Create data campaign ----------------
		campaign = {}
		campaign['CAMPAIGN_ID'] = plan[6]
		campaign['UPDATE_DATE'] = plan[7]
		flag = True
		for plan_info in list_plan:
			if int(plan[0]) == int(plan_info['PRODUCT']) \
				and plan[1] == plan_info['REASON_CODE_ORACLE']:
				plan_temp = plan_info

				if plan[3] == plan_info['UNIT_OPTION'] and plan[2] == plan_info['EFORM_TYPE']:
					temp = plan_info

					temp['CAMPAIGN_MANUAL_MAP'] = []
					temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
					temp['USER_MAP'] = plan[4]
					list_plan_diff.append(temp)
					flag = False
		# ----------- Plan moi duoc tao -----------------
		if flag:
			temp = plan_temp
			temp['UNIT_OPTION'] = plan_info['UNIT_OPTION']
			temp['EFORM_TYPE'] = plan_info['EFORM_TYPE']
			temp['CAMPAIGN_MANUAL_MAP'] = []
			temp['CAMPAIGN_MANUAL_MAP'].append(campaign)
			temp['USER_MAP'] = plan[4]
			list_plan_diff.append(temp)

	print (list_plan_diff)
	return (list_plan_diff)


path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/mapping_data'
connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
ReadTableManualMap(connect, path_data)