import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime , timedelta, date

import mapping_campaign_plan as mapping_data


def CreateSum():
	sum_plan = {}
	sum_plan['CLICKS'] = 0
	sum_plan['IMPRESSIONS'] = 0
	# sum_plan['UNIQUE_COOKIES'] = 0
	sum_plan['CTR'] = 0
	# sum_plan['AVG_CPC'] = 0
	# sum_plan['AVG_CPM'] = 0
	sum_plan['COST'] = 0
	sum_plan['CONVERSIONS'] = 0
	sum_plan['INVALID_CLICKS'] = 0
	# sum_plan['AVG_POSITION'] = 0
	sum_plan['ENGAGEMENTS'] = 0
	# sum_plan['AVG_CPE'] = 0
	# sum_plan['AVG_CPV'] = 0
	sum_plan['INTERACTIONS'] = 0
	sum_plan['VIEWS'] = 0
	return sum_plan

def SumTotalPlan(plan, list_campaign):

	"""
		Hàm tính total cho một plan (tổng các campaign)
	"""
	list_map = []
	sum_plan = CreateSum()
	for campaign_in_plan in plan['CAMPAIGN']:
		for campaign in list_campaign:
			# if str(campaign['Campaign ID']) == '794232395' and plan['REASON_CODE_ORACLE'] == '1708062' \
			# 	and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN' and campaign_in_plan['CAMPAIGN_ID'] == '794232395':
			# 	print (campaign)
			# 	print (plan)
			# 	print ("SUM======================================\n\n\n")
			if (str(campaign_in_plan['CAMPAIGN_ID']) == str(campaign['Campaign ID'])) \
			and (campaign_in_plan['Date'] == campaign['Date']):
				# --------------- Tính total ------------------
				sum_plan['CLICKS'] += float(campaign['Clicks'])
				sum_plan['IMPRESSIONS'] += float(campaign['Impressions'])
				sum_plan['CTR'] += float(campaign['CTR'])
				sum_plan['AVG_CPC'] += float(campaign['Avg. CPC'])
				sum_plan['AVG_CPM'] += float(campaign['Avg. CPM'])
				sum_plan['COST'] += float(campaign['Cost'])
				sum_plan['CONVERSIONS'] += float(campaign['Conversions'])
				sum_plan['INVALID_CLICKS'] += float(campaign['Invalid clicks'])
				sum_plan['AVG_POSITION'] += float(campaign['Avg. position'])
				sum_plan['ENGAGEMENTS'] += float(campaign['Engagements'])
				sum_plan['AVG_CPE'] += float(campaign['Avg. CPE'])
				sum_plan['AVG_CPV'] += float(campaign['Avg. CPV'])
				sum_plan['INTERACTIONS'] += float(campaign['Interactions'])
				sum_plan['VIEWS'] += float(campaign['Views'])
				if 'INSTALL_CAMP' not in campaign:
					campaign['INSTALL_CAMP'] = 0
				sum_plan['INSTALL_CAMP'] += float(campaign['INSTALL_CAMP'])

				#---------------- Add data map ------------------
				z = campaign.copy()
				z.update(plan)
				list_map.append(z)
	plan['TOTAL_CAMPAIGN'] = sum_plan
	# print (len(list_map))
	return (plan, list_map)

#------------ Cộng hai plan ----------------------
def SumTotalManyPlan(list_plan, list_campaign):
	"""
		Hàm tính total list plan (tổng các campaign). Đồng thời tạo data mp and un map
	"""
	list_plan_total = []
	list_data_map = []
	for plan in list_plan:
		# if plan['REASON_CODE_ORACLE'] == '1708062' and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN':
		# 	print (plan)
		# ------------- Lấy các plan mapping được ---------------
		if 'CAMPAIGN' in plan:
			plan, list_map = SumTotalPlan(plan, list_campaign)
			if len(plan['CAMPAIGN']) > 0:
				list_plan_total.append(plan)
				list_data_map.extend(list_map)
				# print (len(list_data_map))
	return (list_plan_total, list_data_map)

#------------ Cộng hai plan ----------------------
def SumTwoTotal(sum_plan1, sum_plan2):
	sum_plan = CreateSum()
	sum_plan['CLICKS'] = sum_plan1['CLICKS'] + sum_plan2['CLICKS']
	sum_plan['IMPRESSIONS'] = sum_plan1['IMPRESSIONS'] + sum_plan2['IMPRESSIONS']
	sum_plan['UNIQUE_COOKIES'] = sum_plan1['UNIQUE_COOKIES'] + sum_plan2['UNIQUE_COOKIES']
	sum_plan['CTR'] = sum_plan1['CTR'] + sum_plan2['CTR']
	sum_plan['AVG_CPC'] = sum_plan1['AVG_CPC'] + sum_plan2['AVG_CPC']
	sum_plan['AVG_CPM'] = sum_plan1['AVG_CPM'] + sum_plan2['AVG_CPM']
	sum_plan['COST'] = sum_plan1['COST'] + sum_plan2['COST']
	sum_plan['CONVERSIONS'] = sum_plan1['CONVERSIONS'] + sum_plan2['CONVERSIONS']
	sum_plan['INVALID_CLICKS'] = sum_plan1['INVALID_CLICKS'] + sum_plan2['INVALID_CLICKS']
	sum_plan['AVG_POSITION'] = sum_plan1['AVG_POSITION'] + sum_plan2['AVG_POSITION']
	sum_plan['ENGAGEMENTS'] = sum_plan1['ENGAGEMENTS'] + sum_plan2['ENGAGEMENTS']
	sum_plan['AVG_CPE'] = sum_plan1['AVG_CPE'] + sum_plan2['AVG_CPE']
	sum_plan['AVG_CPV'] = sum_plan1['AVG_CPV'] + sum_plan2['AVG_CPV']
	sum_plan['INTERACTIONS'] = sum_plan1['INTERACTIONS'] + sum_plan2['INTERACTIONS']
	sum_plan['VIEWS'] = sum_plan1['VIEWS'] + sum_plan2['VIEWS']
	sum_plan['INSTALL_CAMP'] = sum_plan1['INSTALL_CAMP'] + sum_plan2['INSTALL_CAMP']
	return sum_plan

#====================================== Total plan ===========================================
def CaculatorNumberDate(start_date, end_date):
	startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
	endDate = datetime.strptime(end_date, '%Y-%m-%d').date()  
	date = endDate - startDate
	return date.days + 1

def CaculatorListMonth(start_date, end_date):
	# ====================  Ham can cai tien de tinh dc 2 nam ======================
	"""
		Hàm chia ngày của các tháng của plan
		input: 
			+ startDate = '2017-06-01'
			+ endDate = '2017-07-05'
		Output:
			[{'MONTH': 6, 'DAY': 30, 'DATA': False}, {'MONTH': 7, 'DAY': 5, 'DATA': False}]
	"""
	year = start_date[:-6]

	start_m = int(start_date[5:-3])
	end_m = int(end_date[5:-3])
	month = []

	if (end_m - start_m) > 0:
	    # Plan chạy nhiều tháng
	    # Tinh thang dau tien co bao nhieu ngay
	    start = start_date
	    month_end = int(start_date[5:-3]) + 1
	    end = year + '-' + str(month_end)
	    s = datetime.strptime(start, '%Y-%m-%d').date()
	    e = datetime.strptime(end, '%Y-%m').date()
	    date = (e - s).days
	    json_ = {
	        'MONTH' : month_end - 1,
	        'DAY' : date,
	        'DATA' : False
	    }
	    month.append(json_)
	    
	    while(month_end != end_m):
	        start = end
	        month_end = int(start[5:]) + 1
	        end = year + '-' + str(month_end)
	        s = datetime.strptime(start, '%Y-%m').date()
	        e = datetime.strptime(end, '%Y-%m').date()
	        date = (e - s).days
	        json_ = {
	            'MONTH' : month_end - 1,
	            'DAY' : date,
	            'DATA' : False
	        }
	        month.append(json_)
	    start = end
	    s = datetime.strptime(start, '%Y-%m').date()
	    e = datetime.strptime(end_date, '%Y-%m-%d').date()
	    date = (e - s).days + 1
	    json_ = {
	        'MONTH' : month_end,
	        'DAY' : date,
	        'DATA' : False
	    }
	    month.append(json_)
	else:
		month_end = int(start_date[5:-3]) + 1
		s = datetime.strptime(start_date, '%Y-%m-%d').date()
		e = datetime.strptime(end_date, '%Y-%m-%d').date()
		date = (e - s).days + 1
		json_ = {
			'MONTH' : month_end - 1,
			'DAY' : date,
			'DATA' : False
		}
		month.append(json_)
	return month


def CaculatorStartEndDate(plan, start, end):
	from datetime import datetime , timedelta, date
	# Get start end
	month_start = int(start[5:-3])
	month_end = int(end[5:-3])
	year_end = end[:4]
	for month in plan['MONTHLY']:
		if (int(month['MONTH']) == month_start):
			start_date = datetime.strptime(start, '%Y-%m-%d').date()
			end_date = start_date + timedelta(int(month['DAY']) - 1)
			start_date = start_date.strftime('%Y-%m-%d')
			end_date = end_date.strftime('%Y-%m-%d')
		else:
			start_date = year_end + '-' + str(month['MONTH']) + '-01'
			start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
			end_date = start_date + timedelta(int(month['DAY']) - 1)
			start_date = start_date.strftime('%Y-%m-%d')
			end_date = end_date.strftime('%Y-%m-%d')
		month['START_DATE'] = start_date
		month['END_DATE'] = end_date
	return plan

# ----------- Tính total từng month -------------
def CaculatorTotalMonth(plan, date):
	# print ("vao ham")
	# if plan['REASON_CODE_ORACLE'] == '1708007':
	# 		print (plan)
	# ---------------- Choose time real ----------------------
	start_plan, end_plan = mapping_data.ChooseTime(plan)

	plan['NUMBER_DATE'] = CaculatorNumberDate(start_plan, end_plan)

	if datetime.strptime(start_plan, '%Y-%m-%d').date() <= datetime.strptime(date, '%Y-%m-%d').date():
		# Thang hien tai dang mapping
		month = int(date[5:-3])
		end_date = datetime.strptime(end_plan, '%Y-%m-%d').date()
		now = datetime.strptime(date, '%Y-%m-%d').date()

		if now > end_date:
			plan['MONTHLY'] = CaculatorListMonth(start_plan, end_plan)
			number_date = plan['NUMBER_DATE']
		else:
			# So ngay tu start_day den hien tai (co the tren lech 1 ngay)
			number_date = CaculatorNumberDate(start_plan, date)
			plan['MONTHLY'] = CaculatorListMonth(start_plan, date)
		for m in plan['MONTHLY']:
			if m['MONTH'] <= month:
				# Da co data
				m['DATA'] = True
				sum_plan = {}
				sum_plan['CLICKS'] = (float(plan['TOTAL_CAMPAIGN']['CLICKS']) / number_date) * m['DAY']
				sum_plan['IMPRESSIONS'] = (float(plan['TOTAL_CAMPAIGN']['IMPRESSIONS']) / number_date) * m['DAY']
				sum_plan['UNIQUE_COOKIES'] = (float(plan['TOTAL_CAMPAIGN']['UNIQUE_COOKIES']) / number_date) * m['DAY']
				sum_plan['CTR'] = (float(plan['TOTAL_CAMPAIGN']['CTR']) / number_date) * m['DAY']
				sum_plan['AVG_CPC'] = (float(plan['TOTAL_CAMPAIGN']['AVG_CPC']) / number_date) * m['DAY']
				sum_plan['AVG_CPM'] = (float(plan['TOTAL_CAMPAIGN']['AVG_CPM']) / number_date) * m['DAY']
				sum_plan['COST'] = (float(plan['TOTAL_CAMPAIGN']['COST']) / number_date) * m['DAY']
				sum_plan['CONVERSIONS'] = (float(plan['TOTAL_CAMPAIGN']['CONVERSIONS']) / number_date) * m['DAY']
				sum_plan['INVALID_CLICKS'] = (float(plan['TOTAL_CAMPAIGN']['INVALID_CLICKS']) / number_date) * m['DAY']
				sum_plan['AVG_POSITION'] = (float(plan['TOTAL_CAMPAIGN']['AVG_POSITION']) / number_date) * m['DAY']
				sum_plan['ENGAGEMENTS'] = (float(plan['TOTAL_CAMPAIGN']['ENGAGEMENTS']) / number_date) * m['DAY']
				sum_plan['AVG_CPE'] = (float(plan['TOTAL_CAMPAIGN']['AVG_CPE']) / number_date) * m['DAY']
				sum_plan['AVG_CPV'] = (float(plan['TOTAL_CAMPAIGN']['AVG_CPV']) / number_date) * m['DAY']
				sum_plan['INTERACTIONS'] = (float(plan['TOTAL_CAMPAIGN']['INTERACTIONS']) / number_date) * m['DAY']
				sum_plan['VIEWS'] = (float(plan['TOTAL_CAMPAIGN']['VIEWS']) / number_date) * m['DAY']
				sum_plan['INSTALL_CAMP'] = (float(plan['TOTAL_CAMPAIGN']['INSTALL_CAMP']) / number_date) * m['DAY']
				m['TOTAL_CAMPAIGN_MONTHLY'] = sum_plan
		# if plan['REASON_CODE_ORACLE'] == '1708007':
		# 	print (plan)
	return plan


#--------------- Insert Volume actual --------------------
def GetVolumeActualMonthly(plan, month):
	unit_option = plan['UNIT_OPTION']

	if unit_option == 'CPC':
		return month['TOTAL_CAMPAIGN_MONTHLY']['CLICKS']
	if unit_option == 'CPM':
		return month['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS']
	if unit_option == 'CPA':
		if 'CCD_NRU' in month['TOTAL_CAMPAIGN_MONTHLY']:
			return month['TOTAL_CAMPAIGN_MONTHLY']['CCD_NRU'] #CCD_NRU
		else:
			return month['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS']
	if unit_option == 'CPI':
		return month['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP']  # Install
	if unit_option == 'CPV':
		return month['TOTAL_CAMPAIGN_MONTHLY']['VIEWS']
	return ''

def GetVolumeActualTotal(plan):
	unit_option = plan['UNIT_OPTION']

	if unit_option == 'CPC':
		return plan['TOTAL_CAMPAIGN']['CLICKS']
	if unit_option == 'CPM':
		return plan['TOTAL_CAMPAIGN']['INTERACTIONS']
	if unit_option == 'CPA':
		if 'CCD_NRU' in plan['TOTAL_CAMPAIGN']:
			return plan['TOTAL_CAMPAIGN']['CCD_NRU'] #CCD_NRU
		else:
			return plan['TOTAL_CAMPAIGN']['INTERACTIONS']
	if unit_option == 'CPI':
		return plan['TOTAL_CAMPAIGN']['INSTALL_CAMP']  # Install
	if unit_option == 'CPV':
		return plan['TOTAL_CAMPAIGN']['VIEWS']
	return ''


def SetVolunmActual(data_map, date):
	for plan in data_map:
		plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = GetVolumeActualTotal(plan)
		for m in plan['MONTHLY']:
			m['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = GetVolumeActualMonthly(plan, m)
	return data_map

# Hai ham de set volum Actual
def SetVolunmActualFile(path_data, date):
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
		# ---- Neu tim thay file total truoc do -----
	else:
		find = True
	if find:
		with open (path_data_total_map,'r') as f:
			data_map = json.load(f)
		data_map = SetVolunmActual(data_map, date)
		with open (path_data_total_map,'w') as f:
			json.dump(data_map, f)	


def SumMonthly(plan, list_campaign):
	start_plan, end_plan = mapping_data.ChooseTime(plan)

	plan['NUMBER_DATE'] = CaculatorNumberDate(start_plan, end_plan)
	plan['MONTHLY'] = CaculatorListMonth(start_plan, end_plan)
	plan = CaculatorStartEndDate(plan, start_plan, end_plan)


	return plan

def SumTotalPlan(plan, list_campaign):
	"""
		Hàm tính total cho một plan (tổng các campaign)
	"""
	list_map = []
	sum_plan = CreateSum()
	for campaign in list_campaign:
		# --------------- Tính total ------------------
		sum_plan['CLICKS'] += float(campaign['Clicks'])
		sum_plan['IMPRESSIONS'] += float(campaign['Impressions'])
		sum_plan['CTR'] += float(campaign['CTR'])
		sum_plan['COST'] += float(campaign['Cost'])
		# sum_plan['CONVERSIONS'] = 0
		sum_plan['CONVERSIONS'] += float(campaign['Conversions'])
		sum_plan['INVALID_CLICKS'] += float(campaign['Invalid clicks'])
		sum_plan['ENGAGEMENTS'] += float(campaign['Engagements'])
		sum_plan['INTERACTIONS'] += float(campaign['Interactions'])
		sum_plan['VIEWS'] += float(campaign['Views'])
		#---------------- Add data map ------------------
		z = campaign.copy()
		z.update(plan)
		list_map.append(z)
	plan['TOTAL_CAMPAIGN'] = sum_plan
	# print (len(list_map))
	return (plan, list_map)

def SumMonthlyPlan(plan, list_campaign):
	"""
		Hàm tính total monthly cho một plan (tổng các campaign)
	"""
	
	for month in plan['MONTHLY']:
		start = datetime.strptime(month['START_DATE'], '%Y-%m-%d').date()
		end = datetime.strptime(month['END_DATE'], '%Y-%m-%d').date()
		sum_plan = CreateSum()
		for campaign in list_campaign:
			date = datetime.strptime(campaign['Date'], '%Y-%m-%d').date()
			if date >= start and date <= end:
				# if plan['REASON_CODE_ORACLE'] == '1703061':
				# 	print (month)
				# 	print (campaign)
				# --------------- Tính total ------------------
				sum_plan['CLICKS'] += float(campaign['Clicks'])
				sum_plan['IMPRESSIONS'] += float(campaign['Impressions'])
				sum_plan['CTR'] += float(campaign['CTR'])
				sum_plan['COST'] += float(campaign['Cost'])
				# sum_plan['CONVERSIONS'] = 0
				sum_plan['CONVERSIONS'] += float(campaign['Conversions'])
				sum_plan['INVALID_CLICKS'] += float(campaign['Invalid clicks'])
				sum_plan['ENGAGEMENTS'] += float(campaign['Engagements'])
				sum_plan['INTERACTIONS'] += float(campaign['Interactions'])
				sum_plan['VIEWS'] += float(campaign['Views'])
				#---------------- Add data map ------------------
				# z = campaign.copy()
				# z.update(plan)
				# list_map.append(z)
		month['TOTAL_CAMPAIGN_MONTHLY'] = sum_plan
	# print (len(list_map))
	# if plan['REASON_CODE_ORACLE'] == '1703061':
	# 	print (plan)
	return plan

def CaculatorForPlan(list_plan):
	for plan in list_plan:
		plan, list_map = SumTotalPlan(plan, plan['CAMPAIGN'])
		plan = SumMonthly(plan, plan['CAMPAIGN'])
		plan = SumMonthlyPlan(plan, plan['CAMPAIGN'])

		# print (plan)
	return list_plan


def AddToTotal (data_total, data_date, date):

	list_plan_insert = []
	list_plan_remove = []
	# Merge plan cua ngay voi total
	for plan_date in data_date:
		flag = False
		for plan in data_total:
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

def GetListMapOnDate(data_date):
	list_map = []
	for plan in data_date['PLAN']:
		for camp in plan['CAMPAIGN']:
			z = camp.copy()
			z.update(plan)
			list_map.append(z)
	return list_map

def MergeDataToTotal(path_data, date):

	path_folder = path_data + '/' + str(date) +  '/DATA_MAPPING'
	list_data_map = []
	list_plan_remove = []
	list_plan_update = []
	list_plan_insert = []
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)

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


	path_data_map = os.path.join(path_folder, 'mapping_' + str(date) + '.json')
	if not find:
		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		data_total = {}
		data_total['TOTAL'] = []
		data_total['UN_CAMP'] = []
		with open (path_data_total_map,'w') as f:
			json.dump(data_total['TOTAL'], f)

		# ======== Un map camp ===============
		path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
		with open (path_data_un_map,'w') as f:
			json.dump(data_total['UN_CAMP'], f)


	if os.path.exists(path_data_map):
		with open (path_data_map,'r') as f:
			data_date = json.load(f)

		data_total = {}
		with open (path_data_total_map,'r') as f:
			data_total['TOTAL'] = json.load(f)
		with open (path_data_un_map,'r') as f:
			data_total['UN_CAMP'] = json.load(f)


		list_data_map = GetListMapOnDate(data_date)
		list_plan_update = list(data_total['TOTAL'])
		# print (data_total)
		# print (data_date['PLAN'])
		# print (len(data_date['PLAN']))

		# data_total, list_plan_insert, list_plan_remove = AddToTotal (data_total, data_date, date)
		data_total['TOTAL'], list_plan_insert, list_plan_remove = AddToTotal (data_total['TOTAL'], data_date['PLAN'], date)
		# print (len(data_total['TOTAL']))

		data_total['TOTAL'] = CaculatorForPlan(data_total['TOTAL'])

		# for plan_total in data_total['TOTAL']:
		# 	if plan_total['REASON_CODE_ORACLE'] == '1708007':
		# 		print (plan_total)
		data_total['UN_CAMP'].extend(data_date['UN_CAMP'])
		


		# print ("=================== LUU FILE ===========================")

		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		# print (path_data_total_map)
		#-------------------------- Write total lần 1------------------
		with open (path_data_total_map,'w') as f:
			json.dump(data_total['TOTAL'], f)

		path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
		with open (path_data_un_map,'w') as f:
			json.dump(data_total['UN_CAMP'], f)


	return (list_data_map, list_plan_insert, list_plan_remove, list_plan_update)


def InsertDateToTotal(path_data, date):
	list_data_map, list_plan_insert, list_plan_remove, list_plan_update = MergeDataToTotal(path_data, str(date))

	return (list_data_map, list_plan_insert, list_plan_remove, list_plan_update)