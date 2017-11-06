# # # import sys
# # # import os
# # # import pandas as pd
# # # import numpy as np
# # # import json
# # # import cx_Oracle
# # # import logging
# # # from datetime import datetime , timedelta, date
# # # from googleads import adwords

# # # logging.basicConfig(level=logging.INFO)
# # # logging.getLogger('suds.transport').setLevel(logging.DEBUG)


# # # PAGE_SIZE = 100


# # # def GetCampaign(client, acccount_id):
# # #   # Initialize appropriate service.
# # #   client.SetClientCustomerId(str(acccount_id))
# # #   print ("================================================================")
# # #   campaign_service = client.GetService('CampaignService', version='v201708')
# # #   print ("================================================================")

# # #   # Construct selector and get all campaigns.
# # #   offset = 0
# # #   # selector = {
# # #   #     'fields': ['Id', 'Name', 'Status'],
# # #   #     'paging': {
# # #   #         'startIndex': str(offset),
# # #   #         'numberResults': str(PAGE_SIZE)
# # #   #     }
# # #   # }



# # #   selector = {
# # #       'fields': ['Id', 'Name', 'Status','Amount'
# # #       ,'BaseCampaignId','BiddingStrategyName','BiddingStrategyType'
# # #       ,'BudgetId','BudgetName','BudgetStatus'
# # #       ,'StartDate','EndDate'
# # #     #  ,'frequencyCap','advertisingChannelType','advertisingChannelSubType'
# # #     # ,'labels'
# # #     ],
# # #       'paging': {
# # #           'startIndex': str(offset),
# # #           'numberResults': str(PAGE_SIZE)
# # #       }
# # #   }

# # #   from datetime import datetime , timedelta, date
# # #   more_pages = True
# # #   list_camp = []
# # #   while more_pages:
# # #     page = campaign_service.get(selector)
# # #     print(page['totalNumEntries'])

# # #     # Display results.
    
# # #     if 'entries' in page:
# # #       for campaign in page['entries']:
# # #         camp = {
# # #           'CAMPAIGN_NAME' : campaign['name'],
# # #           'CAMPAIGN_ID' : campaign['id'],
# # #           'ACCOUNT_ID' : acccount_id
# # #         }
# # #         list_camp.append(camp)
# # #         print(camp)
# # #     else:
# # #       print('No campaigns were found.')
# # #     offset += PAGE_SIZE
# # #     selector['paging']['startIndex'] = str(offset)
# # #     more_pages = offset < int(page['totalNumEntries'])
# # #     time.sleep(1)

# # #   return list_camp


# # # acccount_id = '6493618146'
# # # adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
# # # print (adwords_client)
# # # print ("================================================================")
# # # list_camp = GetCampaign(adwords_client, acccount_id)

# # # for camp in list_camp:
# # # 	print (camp)


# # # from googleads import adwords
# # # adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
# # # campaign_service = adwords_client.GetService('CampaignService', version='v201708')


# # # import sys
# # # import os
# # # import pandas as pd
# # # import numpy as np
# # # import json
# # # import cx_Oracle
# # # from datetime import datetime , timedelta, date

# # # def Read_NRU_for_month(cursor, year, month, product):
# # # 	#==================== Get NRU =============================
# # # 	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
# # # 	and extract (Year from SNAPSHOT_DATE) = :1 and extract (Month from SNAPSHOT_DATE) = :2 "
# # # 	cursor.execute(statement, (year, month))
# # # 	list_NRU = list(cursor.fetchall())  
# # # 	# print(list_NRU)


# # # 	#==================== Get product ID ===================
# # # 	statement = "Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT where PRODUCT_ID = '" + product +  "'"
# # # 	cursor.execute(statement)
# # # 	list_product = list(cursor.fetchall())

# # # 	ccd_nru = 0  
# # # 	list_nru = []
# # # 	for i in range(len(list_NRU)):
# # # 		list_NRU[i] = list(list_NRU[i])    
# # # 		for pro in list_product:
# # # 			if (list_NRU[i][1] == pro[1]):
# # # 				data = [list_NRU[i][0], list_NRU[i][1], list_NRU[i][2], pro[0], pro[1]]
# # # 				if data not in list_nru:					
# # # 					list_nru.append(data)
# # # 					ccd_nru += list_NRU[i][2] 
	
# # # 	return ccd_nru 
        



# # # def add_NRU_monthly_for_plan(connect, path_folder, list_plan):
# # # # ==================== Connect database =======================
# # # 	conn = cx_Oracle.connect(connect)
# # # 	cursor = conn.cursor()

# # # 	for plan in list_plan:
# # # 		if ('MONTHLY' in value):
# # # 			for i in range(len(value['MONTHLY'])):
# # # 				plan['MONTHLY']['CCD_NRU'] = Read_NRU_for_month(cursor, value['CYEAR'], str(value['MONTHLY'][i]['MONTH']), value['PRODUCT'])

# # # 	cursor.close()
# # # 	return list_plan



# # # def AddNRU(connect, path_data, date):
	

# # # connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# # # conn = cx_Oracle.connect(connect)
# # # cursor = conn.cursor()
# # # nru = Read_NRU_for_month(cursor, '8', '219')
# # # print(nru)




import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date
import time


# # # def Insert(name, cursor):
# # # 	#==================== Insert data into database =============================
# # # 	statement = 'insert into DTM_GG_RUN_FLAG (FLAG_RUNNING, FINAL_RUNTIME) \
# # # 	values (:1, :2) '
		
# # # 	cursor.execute(statement, (name, None))
	
# # # 	print("A row inserted!.......")
# # # 	conn.commit()
# # # 	print("Committed!.......")



# # # connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# # # conn = cx_Oracle.connect(connect)
# # # cursor = conn.cursor()
# # # path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/MCC_TEST.json'
# # # # path = 'D:/WorkSpace/GG_Tool/New folder/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/MCC_TEST_UNICODE.json'
# # # with open(path, 'r') as fi:
# # # 	data = json.load(fi)
# # # # import _locale
# # # # _locale._getdefaultlocale = (lambda *args: ['vi-VN', 'utf-8'])
# # # for acc in data:
# # # 	if (str(acc["customerId"]) == '4476024314'):
# # # 		print(acc["name"])

# # # 		# Insert(acc["name"].encode('utf-8'), cursor)
# # # 		# # Insert(acc["name"].encode('cp437'), cursor)
# # # 		# Insert(acc["name"].encode('ISO-8859-1'), cursor)
# # # 		# Insert(acc["name"], cursor)
		
# # # 		# print(acc["name"][2:-1])
# # # 		# Insert(acc["name"][2:-1], cursor)


# # # cursor.close()

# # # path_no_video = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON_NO_VIDEO'
# # # path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# # # list_folder = next(os.walk(path))[1]
# # # for folder in list_folder:
# # # 	name = folder + '/video_url_' + folder + '.json'
# # # 	path_in = path_no_video + '/' + name
# # # 	path_out = path + '/' + name
# # # 	print (path_in)
# # # 	print (path_out)
# # # 	if os.path.exists(path_in) and os.path.exists(path_out):
# # # 		with open(path_in, 'r') as fi:
# # # 			data = json.load(fi)

# # # 		with open (path_out,'w') as f:
# # # 			json.dump(data, f)


# # ######################### Khong xoa ##################################################
# # #-------------- Do data audit ------------------
# # def InsertContentAds(cursor, ads, d):
# # 	statement = 'insert into STG_AUDIT_CONTENT ( \
# # 	AD_ID, PRODUCT_ID, CONTENT, TYPE, PREDICT_PERCENT, \
# # 	INDEX_CONTENT, SNAPSHOT_DATE, INSERT_DATE) \
# # 	values (:1, :2, :3, :4, :5, :6, :7, :8)'
# # 	print (ads['list_product'])
# # 	print (ads['ad_id'])
# # 	# now = datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()
# # 	# print (ads['audit_content'])
# # 	if ads['list_product'] != [] and 'audit_content' in ads:
# # 		#-------- Insert image ---------------
# # 		list_image = ads['audit_content']['image_urls']
# # 		if list_image != []:
# # 			for i, image in enumerate(list_image):
# # 				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], image['image_url'], 'image_url', 0, i,  \
# # 				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))

# # 		list_thumbnail = ads['audit_content']['thumbnail_urls']
# # 		if list_thumbnail != []:
# # 			for i, thumbnail in enumerate(list_thumbnail):
# # 				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], thumbnail['thumbnail_url'], 'thumbnail_url', 0, i,  \
# # 				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))

# # 		links = ads['audit_content']['links']
# # 		if links != []:
# # 			for i, link in enumerate(links):
# # 				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], link['link'], 'link', 0, i,  \
# # 				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))

# # 		messages = ads['audit_content']['messages']
# # 		if messages != []:
# # 			for i, message in enumerate(messages):
# # 				try:
# # 					cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], message['message'].encode('utf-8'), 'message', 0, i,  \
# # 					datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))
# # 				except:
# # 					print ("Qua dai")

# # 		video_ids = ads['audit_content']['video_ids']
# # 		if video_ids != [] and 'object_story_spec' in ads:
# # 			for i, video_id in enumerate(video_ids):
# # 				link = 'https://www.facebook.com/' + str(ads['object_story_spec']['page_id']) + '/videos/' + str(video_id['video_id'])
# # 				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], link, 'video_id', 0, i,  \
# # 				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))

# # def add_label_video_to_data(connect, path, date_, to_date_):
# # 	# Lấy danh sách path của các file json cần tổng hợp data
# # 	list_folder = next(os.walk(path))[1]

# # 	#========================== Auto run ===================
# # 	conn = cx_Oracle.connect(connect)
# # 	cursor = conn.cursor()
# # 	date = datetime.strptime(date_, '%Y-%m-%d').date()
# # 	to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
# # 	for folder in list_folder:
# # 		print (folder)
# # 		d = datetime.strptime(folder, '%Y-%m-%d').date()
# # 		if d <= to_date and d >= date:
# # 			path_folder = os.path.join(path, folder)
# # 			path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
# # 			print (path_file)
# # 			print ("--------------------")
# # 			if os.path.exists(path_file):
# # 				with open(path_file, 'r') as f:
# # 					data = json.load(f)
# # 					for ads in data['my_json']:
# # 						print ('ads====================')
# # 						InsertContentAds(cursor, ads, str(d))
# # 	conn.commit()
# # 	cursor.close()


# # # path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
# # # path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# # # path = 'D:/DATA/NEW_DATA_10-2016_05-2017/FULL_DATA_10-2016_06-2017/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# # # path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# # # date_ = '2016-11-26'
# # # to_date_ = '2016-12-10'

# # if __name__ == '__main__':
# #     from sys import argv
# #     path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'  
# #     connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'  
# #     script, date, to_date = argv
# #     add_label_video_to_data(connect, path, date, to_date)


# # ##################################################################################################
# # ----------- Tính total từng month -------------
# def CaculatorTotalMonth(list_camp, plan, date):
# 	# print ("vao ham")
# 	# if plan['REASON_CODE_ORACLE'] == '1708007':
# 	# 		print (plan)
# 	# ---------------- Choose time real ----------------------
# 	start_plan, end_plan = mapping_data.ChooseTime(plan)

# 	plan['NUMBER_DATE'] = CaculatorNumberDate(start_plan, end_plan)

# 	if datetime.strptime(start_plan, '%Y-%m-%d').date() <= datetime.strptime(date, '%Y-%m-%d').date():
# 		# Thang hien tai dang mapping
# 		month = int(date[5:-3])
# 		end_date = datetime.strptime(end_plan, '%Y-%m-%d').date()
# 		now = datetime.strptime(date, '%Y-%m-%d').date()

# 		if now > end_date:
# 			plan['MONTHLY'] = CaculatorListMonth(start_plan, end_plan)
# 			number_date = plan['NUMBER_DATE']
# 		else:
# 			# So ngay tu start_day den hien tai (co the tren lech 1 ngay)
# 			number_date = CaculatorNumberDate(start_plan, date)
# 			plan['MONTHLY'] = CaculatorListMonth(start_plan, date)
# 		# if plan['REASON_CODE_ORACLE'] == '1708007':
# 		# 	print (plan)
# 		if m['MONTH'] <= month:
# 			plan = CaculatorStartEndDate(plan, start_plan, end_plan)
# 			for m in plan['MONTHLY']:
# 				start = datetime.strptime(m['START_DATE'], '%Y-%m-%d').date()
# 				end = datetime.strptime(m['END_DATE'], '%Y-%m-%d').date()
# 				for camp in plan['CAMPAIGN']:
# 					d = datetime.strptime(camp['Date'], '%Y-%m-%d').date()
# 					if d >= start and d <= end:
# 						for campaign in list_camp:
# 							if str(camp['Campaign ID']) == campaign['Campaign ID'] \
# 								and str(camp['Date']) == campaign['Date']:

# 								m.get('TOTAL_CAMPAIGN_MONTHLY', CreateSum())
# 								m['DATA'] = True

# 								m['TOTAL_CAMPAIGN_MONTHLY']['CLICKS'] += float(campaign['Clicks'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['IMPRESSIONS'] += float(campaign['Impressions'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['CTR'] += float(campaign['CTR'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['AVG_CPC'] += float(campaign['Avg. CPC'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['AVG_CPM'] += float(campaign['Avg. CPM'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['COST'] += float(campaign['Cost'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['CONVERSIONS'] += float(campaign['Conversions'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['INVALID_CLICKS'] += float(campaign['Invalid clicks'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['AVG_POSITION'] += float(campaign['Avg. position'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['ENGAGEMENTS'] += float(campaign['Engagements'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['AVG_CPE'] += float(campaign['Avg. CPE'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['AVG_CPV'] += float(campaign['Avg. CPV'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['INTERACTIONS'] += float(campaign['Interactions'])
# 								m['TOTAL_CAMPAIGN_MONTHLY']['VIEWS'] += float(campaign['Views'])
# 								if 'INSTALL_CAMP' not in campaign:
# 									campaign['INSTALL_CAMP'] = 0
# 								m['TOTAL_CAMPAIGN_MONTHLY']['INSTALL_CAMP'] += float(campaign['INSTALL_CAMP'])

# 	return plan

# path_alias = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA/2017-09-29/PLAN/product_alias.json'
path = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA_03_10/2017-09-30/DATA_MAPPING/history_name.json'
# path_plan = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA/2017-09-30/PLAN/plan.json'
# # file_product = os.path.join(path_data, str(date) + '/PLAN/product_alias.json')
# import insert_data_map_to_total as insert_to_total

# with open(path_alias, 'r') as fi:
# 	data_alias = json.load(fi)


# with open(path_plan, 'r') as fi:
# 	data_plan = json.load(fi)

# with open(path_total, 'r') as fi:
# 	data_total = json.load(fi)

# # for plan_total in data_alias['ALIAS']:
# # 	if  str(plan_total['PRODUCT_ID']) == '193':
# # 		print (plan_total)

# # for plan_total in data_plan['plan']:
# # 	if  str(plan_total['PRODUCT']) == '193':
# 		# print (plan_total)
# 		# 	for plan in data_alias['ALIAS']:
# # 		if plan_total['PRODUCT'] == plan['PRODUCT_ID'] and plan['APPSFLYER_PRODUCT'] != None:
# # 			plan_total['APPSFLYER_PRODUCT'].append(plan['APPSFLYER_PRODUCT'])
# # 	print (plan_total['APPSFLYER_PRODUCT'])
# date = '2017-09-30'
# for plan_total in data_total:
# # if plan_total['REASON_CODE_ORACLE'] == '1708061':
# # # if str(plan_total['Campaign ID']) == '772872164':
# #   print (plan_total)
# 	# plan_total = insert_to_total.CaculatorTotalMonth(data_total['MAP'], plan_total, date)
# 	if plan_total['REASON_CODE_ORACLE'] == '1708007':
# 		print (plan_total)


# path_total = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA/2017-08-31/DATA_MAPPING/total_mapping.json'

# with open (path_total,'w') as f:
# 	json.dump(data_total, f)

# print ("DONE")


def insert(value, cursor):
	statement = 'insert into ODS_CAMP_FA_MAPPING_GG (ACCOUNT_ID, CAMPAIGN_ID, START_DATE, END_DATE, EFORM_TYPE, \
	UNIT_OPTION, PRODUCT, REASON_CODE_ORACLE, USER_NAME, STATUS, CAMPAIGN_NAME) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)'

	cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], datetime.strptime( value['START_DATE'], '%Y-%m-%d'),\
		datetime.strptime(value['END_DATE'], '%Y-%m-%d'), value['FORM_TYPE'], \
		value['UNIT_OPTION'], value['PRODUCT'], value['REASON_CODE_ORACLE'], value['USER_NAME'], 'USER', \
		value['CAMPAIGN_NAME']))



path_total = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA_GG/2017-09-30/LOG_MANUAL/log_manual.json'
with open(path_total, 'r') as fi:
	data_total = json.load(fi)

with open(path, 'r') as fi:
	data = json.load(fi)

connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
cursor = conn.cursor()
number = 0
print (len(data['HISTORY']))

for log in data_total['LOG']:
	if log['REASON_CODE_ORACLE'] == '1705028':
		for camp in data['HISTORY']:
			if log['CAMPAIGN_ID'] == camp['CAMPAIGN_ID']:
				print ("Gasn")
				log['CAMPAIGN_NAME'] = camp['CAMPAIGN_NAME']
			else:
				log['CAMPAIGN_NAME'] = None
		print (log)
		insert(log, cursor)
		break
		number += 1
print (number)
conn.commit()










# # '''
# #   Get campaing for account and save to folder
# # '''
# import json
# import logging
# import sys
# import os
# from googleads import adwords
# from datetime import datetime, timedelta
# # import get_accounts as get_accounts


# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)

# def TSVtoJson(report_string, date):
#   from collections import defaultdict
#   #========= Get key for json ================
#   list_pre = ['enabled', 'paused', 'removed']
#   list_key = []
#   fi = report_string.split('\n')

#   for line in fi:
#     ele = line.split('\t')
#     if (ele[0] not in list_pre) and (ele[0] != 'Total') and (len(ele) > 1):     
#         list_key = ele

#   #======== Convert a line to dictionary
#   list_json = []

#   for line in fi:
#     ele = line.split('\t')
#     dict_campaign = {}
#     if (ele[0] not in list_key) and (len(ele) > 1 ):
#       for i in range(len(list_key)):          
#         if (list_key[i] == 'Cost') or ((list_key[i].find('Avg') >= 0) and (list_key[i] != 'Avg. position')):         # Cost            
#           ele[i] = float(float(ele[i]) / 1000000)
#         elif (ele[i].isdigit()):         # Integer        
#           ele[i] = int(ele[i])
#         elif (ele[i].find('%') == len(ele[i]) - 1) and (ele[i].replace("%", "").replace(".", "").isdigit()):         # Percent  
#           ele[i] = float(ele[i].replace("%", ""))
#         elif (ele[i].replace(".", "").isdigit()):         # Float        
#           ele[i] = float(ele[i])          
#         elif (ele[i] == ' --'):      # Empty        
#           ele[i] = ""         
#         elif (ele[i].find('[') > 0 and ele[i].find(']') > 0):                
#           list_ = ele[i].split(',')
#           for u in range(len(list_)):             
#             s = list_[u]
#             for v in range(len(s)):                   
#               if (s[v].isalpha() == False):
#                 list_[u] = s.replace(s[v], '')

#           list_[0] = list_[0][1:len(list_[0])]
#           list_[-1] = list_[-1][0:len(list_[0]) -1]   
#           ele[i] = list_

#         dict_campaign[list_key[i]] = ele[i]
#       dict_campaign['Mapping'] = False
#       dict_campaign['Date'] = date
#       if ((dict_campaign['Cost'] > 0)):
#         list_json.append(dict_campaign)
#   return list_json


# def DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate):

#   adwords_client.SetClientCustomerId(customerId)
#   print (customerId)
#   report_downloader = adwords_client.GetReportDownloader(version='v201708')


#   result = []
#   report = {
#       'reportName': 'Custom date CAMPAIGN_PERFORMANCE_REPORT',
#       'dateRangeType': 'CUSTOM_DATE',
#       'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
#       'downloadFormat': 'TSV',
#       'selector': {
#         'dateRange':{'min':startDate,'max':endDate},
#         'fields': [
#           'CampaignStatus',
#           'CampaignName',
#           'AdvertisingChannelType',
#           'AdvertisingChannelSubType',
#           'CampaignId',
#           #, #budget
#           'ServingStatus',
#           'Clicks',
#           'Impressions',
#           'ImpressionReach',
#           'Ctr',
#           'AverageCpc',
#           'AverageCpm',
#           'Cost',
#           'Conversions',
#           'BiddingStrategyType',
#           'InvalidClicks',
#           'AveragePosition',
#           'Engagements',
#           'AverageCpe',
#           'VideoViewRate',
#           'VideoViews',
#           'AverageCpv',
#           'AverageCost',
#           'InteractionTypes',
#           'Interactions',
#           'InteractionRate',
#           'VideoQuartile25Rate',
#           'VideoQuartile50Rate',
#           'VideoQuartile75Rate',
#           'VideoQuartile100Rate',
#           'VideoViews',
#           'StartDate',
#           'EndDate'

#           ]
#       }
#   }
#   result = report_downloader.DownloadReportAsString(
#       report, skip_report_header=True, skip_column_header=False,
#       skip_report_summary=False, include_zero_impressions=True)
#   # print (result)
#   return result


# def DateToString(date):
#   date = date[:-6] + date[5:-3] + date[8:]
#   return date

# def DownloadNameOfAccount(adwords_client, customerId, date, to_date):
  
#   startDate = DateToString(date)
#   endDate = DateToString(to_date)
#   print (startDate)
#   print (endDate)

#   #====================== CAMPAIGN ====================
#   result_campaign = DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate)

#   result_json = TSVtoJson(result_campaign, date)
#   print (len(result_json))
#   list_name = []
#   for campaign in result_json:
#   	if (campaign['Cost'] > 0) and campaign['Campaign state'] != 'Total':
# 	    camp = {
# 	          'CAMPAIGN_NAME' : campaign['Campaign'],
# 	          'CAMPAIGN_ID' : str(campaign['Campaign ID']),
# 	          'ACCOUNT_ID' : customerId
# 	        }
# 	    list_name.append(camp)
#   print (len(list_name))
#   return list_name

#   # path_log = 'C:/Users/ltduo/Desktop/history_name.json'
#   # with open (path_log, 'r') as f:
#   #   data_total = json.load(f)

#   # # for camp in result_json:
#   # #   flag = True
#   # for name in data_total['history_name']:
#   #   if name['CAMPAIGN_ID'] == '697791306':
#   #      print (name)
#   #     if str(camp['Campaign ID']) == name['CAMPAIGN_ID']:
#   #       flag = False
#   #   if flag:
#   #     cam = {
#   #         'CAMPAIGN_NAME' : str(camp['Campaign']),
#   #         'CAMPAIGN_ID' : str(camp['Campaign ID']),
#   #         'ACCOUNT_ID' : customerId
#   #       }

#   #     print (cam)
#   #     data_total['history_name'].append(cam)
#   # with open (path_log ,'w') as f:
#   #   json.dump(data_total, f)



# adwords_client = adwords.AdWordsClient.LoadFromStorage()

# date = '2017-03-01' 
# to_date = '2017-09-30'
# customerId = '1066457627'
# DownloadNameOfAccount(adwords_client, customerId, date, to_date)