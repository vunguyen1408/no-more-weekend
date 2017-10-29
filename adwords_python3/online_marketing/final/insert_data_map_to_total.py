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
	sum_plan['UNIQUE_COOKIES'] = 0
	sum_plan['CTR'] = 0
	sum_plan['AVG_CPC'] = 0
	sum_plan['AVG_CPM'] = 0
	sum_plan['COST'] = 0
	sum_plan['CONVERSIONS'] = 0
	sum_plan['INVALID_CLICKS'] = 0
	sum_plan['AVG_POSITION'] = 0
	sum_plan['ENGAGEMENTS'] = 0
	sum_plan['AVG_CPE'] = 0
	sum_plan['AVG_CPV'] = 0
	sum_plan['INTERACTIONS'] = 0
	sum_plan['VIEWS'] = 0
	sum_plan['INSTALL_CAMP'] = 0
	return sum_plan

def SumTotalPlan(plan, list_campaign):

	"""
		Hàm tính total cho một plan (tổng các campaign)
	"""
	list_map = []
	sum_plan = CreateSum()
	for campaign_in_plan in plan['CAMPAIGN']:
		for campaign in list_campaign:
			if str(campaign['Campaign ID']) == '794232395' and plan['REASON_CODE_ORACLE'] == '1708062' \
				and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN' and campaign_in_plan['CAMPAIGN_ID'] == '794232395':
				print (campaign)
				print (plan)
				print ("SUM======================================\n\n\n")
			if (campaign_in_plan['CAMPAIGN_ID'] == campaign['Campaign ID']) \
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
	return (plan, list_map)

#------------ Cộng hai plan ----------------------
def SumTotalManyPlan(list_plan, list_campaign):
	"""
		Hàm tính total list plan (tổng các campaign). Đồng thời tạo data mp and un map
	"""
	list_plan_total = []
	list_data_map = []
	for plan in list_plan:
		if plan['REASON_CODE_ORACLE'] == '1708062' and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN':
			print (plan)
		# ------------- Lấy các plan mapping được ---------------
		plan, list_map = SumTotalPlan(plan, list_campaign)
		if len(plan['CAMPAIGN']) > 0:
			list_plan_total.append(plan)
			list_data_map.extend(list_map)
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
		# if plan['REASON_CODE_ORACLE'] == '1708007':
		# 	print (plan)
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

def AddToTotal (data_total, data_date, date):
	# -------------------- Tính total cho các plan mapping được của ngày -------------------
	list_plan_total_date, list_data_map = SumTotalManyPlan(data_date['plan'], data_date['campaign'])
	# for i in list_plan_total_date:
		# print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
		# print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
		# print (i)
		# print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
		# print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	for plan_date in list_plan_total_date:
		flag = True
		for plan_total in data_total['TOTAL']:
			if plan_total['PRODUCT'] == plan_date['PRODUCT'] \
				and plan_total['REASON_CODE_ORACLE'] == plan_date['REASON_CODE_ORACLE'] \
				and plan_total['FORM_TYPE'] == plan_date['FORM_TYPE']:
				plan_total['TOTAL_CAMPAIGN'] = SumTwoTotal(plan_total['TOTAL_CAMPAIGN'], plan_date['TOTAL_CAMPAIGN'])
				plan_total['CAMPAIGN'].extend(plan_date['CAMPAIGN'])
				flag = False

		#----- Không tìm thấy trong total ------
		if flag:
			# --------------- Tạo các thông tin month cho plan trước khi add --------------
			data_total['TOTAL'].append(plan_date)

	# --------------- Tinh total month cho cac plan --------------
	for plan in data_total['TOTAL']:
		plan['MONTHLY'] = {}
		plan = CaculatorTotalMonth(plan, date)


	# --------------- Insert data map -------------------
	data_total['MAP'].extend(list_data_map)

	#---------------- Insert data un map -------------------
	#------- campaign --------------
	list_campaign_un_map = []
	for camp in data_date['campaign']:
		if camp['Plan'] == None:
			list_campaign_un_map.append(camp)

	temp = data_total['UN_CAMPAIGN']
	temp.extend(list_campaign_un_map)
	data_total['UN_CAMPAIGN'] = temp

	#--------- plan -----------
	list_plan_un_map = []
	for plan in data_date['plan']:
		if plan['CAMPAIGN'] == []:
			list_plan_un_map.append(plan)

	# print (len(list_plan_un_map))
	temp_un = []
	list_plan_remove = []
	if len(data_total['UN_PLAN']) == 0:
		data_total['UN_PLAN'] = list_plan_un_map
	else:
		for plan_un in list_plan_un_map:
			flag = True
			# print (data_total['UN_PLAN'])
			for plan in data_total['UN_PLAN']:
				if plan_un['PRODUCT'] == plan['PRODUCT'] \
					and plan_un['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
					and plan_un['FORM_TYPE'] == plan['FORM_TYPE'] :
					temp_un.append(plan_un)
					flag = False
			if flag:
				list_plan_remove.append(plan_un)

		data_total['UN_PLAN'] = temp_un
	# --------------- Tinh total month cho cac plan --------------
	# print (data_total['UN_PLAN'])
	# print ("=======================================")
	for plan in data_total['UN_PLAN']:
		plan['MONTHLY'] = {}
		s, e = mapping_data.ChooseTime(plan)
		plan = CaculatorTotalMonth(plan, e)
	list_plan_update = list_plan_total_date
	return (data_total, list_data_map, list_plan_remove, list_plan_update)

def MergeDataToTotal(path_data, date):

	path_folder = path_data + '/' + str(date) +  '/DATA_MAPPING'
	list_data_map = []
	list_plan_remove = []
	list_plan_update = []
	if not os.path.exists(path_folder):
		os.makedirs(path_folder)

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


	path_data_map = os.path.join(path_folder, 'mapping_' + str(date) + '.json')
	if not find:
		print ("=========================================================")
		print ("=========================================================")
		print ("=========================================================")
		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		data_total = {}
		data_total['TOTAL'] = []
		data_total['MAP'] = []
		data_total['UN_PLAN'] = []
		data_total['UN_CAMPAIGN'] = []
		with open (path_data_total_map,'w') as f:
			json.dump(data_total, f)
	if os.path.exists(path_data_map):
		with open (path_data_map,'r') as f:
			data_date = json.load(f)
		# print (path_data_total_map)
		with open (path_data_total_map,'r') as f:
			data_total = json.load(f)
		# print (len(data_total['TOTAL']))

		data_total, list_data_map, list_plan_remove, list_plan_update = AddToTotal (data_total, data_date, date)


		# 	# print (plan)
		# print (len(data_total['TOTAL']))
		# print (len(data_total['UN_PLAN']))
		# for camp in data_total['MAP']:
		# 	loop = 0
		# 	for c in data_total['MAP']:
		# 		if camp['PRODUCT'] == c['PRODUCT'] \
		# 			and camp['REASON_CODE_ORACLE'] == c['REASON_CODE_ORACLE'] \
		# 			and camp['FORM_TYPE'] == c['FORM_TYPE'] \
		# 			and camp['Campaign ID'] == c['Campaign ID'] \
		# 			and camp['Date'] == c['Date']:
		# 			loop += 1
		# 	if loop > 1:
		# 		print ("//////////////////////////////////////////////////////////////////")
		# 		print ("//////////////////////////////////////////////////////////////////")
		# 		print ("//////////////////////////////////////////////////////////////////")
		# 		print (camp)



		print ("=================== LUU FILE ===========================")

		path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
		print (path_data_total_map)
		#-------------------------- Write total lần 1------------------
		with open (path_data_total_map,'w') as f:
			json.dump(data_total, f)
	return (list_data_map, list_plan_remove, list_plan_update)


#--------------- Insert Volume actual --------------------
def GetVolumeActualMonthly(plan, month):
	unit_option = plan['UNIT_OPTION']

	if unit_option == 'CPC':
		return month['TOTAL_CAMPAIGN_MONTHLY']['CLICKS']
	if unit_option == 'CPM':
		return month['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS']
	if unit_option == 'CPA':
		return month['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS'] #CCD_NRU
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
		return plan['TOTAL_CAMPAIGN']['INTERACTIONS'] #CCD_NRU
	if unit_option == 'CPI':
		return plan['TOTAL_CAMPAIGN']['INSTALL_CAMP']  # Install
	if unit_option == 'CPV':
		return plan['TOTAL_CAMPAIGN']['VIEWS']
	return ''

def CreateListPlanMonthly(path_data, date, list_plan_update):
	path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')

	list_temp = []
	if os.path.exists(path_data_total_map):
		with open (path_data_total_map,'r') as f:
			data_map = json.load(f)
		for plan in data_map['TOTAL']:
			# if plan['REASON_CODE_ORACLE'] == '1708007':
			# 	print ("======================================================================")
			# 	print (len(plan['CAMPAIGN']))
			# 	print ("======================================================================")
			plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = GetVolumeActualTotal(plan)
			for m in plan['MONTHLY']:
				m['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = GetVolumeActualMonthly(plan, m)


			# --------- List plan update ---------------
			for p in list_plan_update:
				if p['PRODUCT'] == plan['PRODUCT'] \
					and p['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
					and p['FORM_TYPE'] == plan['FORM_TYPE'] :
					list_temp.append(plan)

		sum_ = 0
		for camp in data_map['UN_CAMPAIGN']:
			# print (camp)
			sum_ += camp['Cost']
		# print ("=================================== SUM COST : " , sum_)
		# print ("=================================== MAP ", len(data_map['MAP']))
		# print ("=================================== UM MAP ", len(data_map['UN_CAMPAIGN']))
		with open (path_data_total_map,'w') as f:
			json.dump(data_map, f)	

	return list_temp


def InsertDateToTotal(path_data, date):
	list_data_map, list_plan_remove, list_plan_update = MergeDataToTotal(path_data, str(date))
	list_plan_update = CreateListPlanMonthly(path_data, str(date), list_plan_update)

	return (list_data_map, list_plan_remove, list_plan_update)
	
def InsertManyDate(path_data, start_date, end_date):
	startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
	endDate = datetime.strptime(end_date, '%Y-%m-%d').date()   

	date = startDate
	while(date <= endDate):
		MergeDataToTotal(path_data, str(date))
		CreateListPlanMonthly(path_data, str(date))
		date = date + timedelta(1)
		# print (date)
		# print ("------------------------")
	



# startDate = '2017-06-01'
# endDate = '2017-06-01'
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/END'
# InsertManyDate(path_data, startDate, endDate)