import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

#-------------- import file ---------------
import insert_data_map_to_total as insert_data
import mapping_campaign_plan as mapping
import insert_nru_into_data as nru
# import insert_nru_to_data as nru


# ------------- Remap -----------------
#-------- Vào data unmap sum các camp cho một plan ----------
def GetCampaignUnMapForPlan(plan, path_data_total_map):

	with open (path_data_total_map,'r') as f:
		data_total = json.load(f)

	list_campaign = data_total['MAP']
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
	# print (list_map_temp)
	# print (list_camp)
	# print (list_campaign)
	return (plan_sum, list_map_temp, list_camp_need_remove)

def Remap(connect, path_data, date):
	# # ------------- Get manual map from table log ----------------
	# list_diff = ReadTableManualMap(connect, path_data, date)
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
		# ---- Neu tim thay file total truoc do -----
	else:
		find = True

	if find:
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)
		list_plan = ReadTableManualMap(connect, path_data, date)
		print (len(list_plan))
		if len(list_plan) > 0:
			# print (list_plan)
			# print (len(list_plan))
			list_camp_remove_unmap = []
			list_map_all = []
			list_plan_remove_unmap = []
			# print (len(data_total['UN_CAMPAIGN']))
			for plan in list_plan:
				plan, list_map, list_camp_need_remove = GetCampaignMapForPlan(plan, path_data_total_map)
				list_map_all.extend(list_map)

# connect = ''
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA'
# date = '2017-08-31'
# GetCampaignUnMapForManualMap(connect, path_data, date)