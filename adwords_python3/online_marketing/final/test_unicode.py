# import sys
# import os
# import pandas as pd
# import numpy as np
# import json
# import cx_Oracle
# import logging
# from datetime import datetime , timedelta, date
# from googleads import adwords

# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)


# PAGE_SIZE = 100


# def GetCampaign(client, acccount_id):
#   # Initialize appropriate service.
#   client.SetClientCustomerId(str(acccount_id))
#   print ("================================================================")
#   campaign_service = client.GetService('CampaignService', version='v201708')
#   print ("================================================================")

#   # Construct selector and get all campaigns.
#   offset = 0
#   # selector = {
#   #     'fields': ['Id', 'Name', 'Status'],
#   #     'paging': {
#   #         'startIndex': str(offset),
#   #         'numberResults': str(PAGE_SIZE)
#   #     }
#   # }



#   selector = {
#       'fields': ['Id', 'Name', 'Status','Amount'
#       ,'BaseCampaignId','BiddingStrategyName','BiddingStrategyType'
#       ,'BudgetId','BudgetName','BudgetStatus'
#       ,'StartDate','EndDate'
#     #  ,'frequencyCap','advertisingChannelType','advertisingChannelSubType'
#     # ,'labels'
#     ],
#       'paging': {
#           'startIndex': str(offset),
#           'numberResults': str(PAGE_SIZE)
#       }
#   }

#   from datetime import datetime , timedelta, date
#   more_pages = True
#   list_camp = []
#   while more_pages:
#     page = campaign_service.get(selector)
#     print(page['totalNumEntries'])

#     # Display results.
    
#     if 'entries' in page:
#       for campaign in page['entries']:
#         camp = {
#           'CAMPAIGN_NAME' : campaign['name'],
#           'CAMPAIGN_ID' : campaign['id'],
#           'ACCOUNT_ID' : acccount_id
#         }
#         list_camp.append(camp)
#         print(camp)
#     else:
#       print('No campaigns were found.')
#     offset += PAGE_SIZE
#     selector['paging']['startIndex'] = str(offset)
#     more_pages = offset < int(page['totalNumEntries'])
#     time.sleep(1)

#   return list_camp


# acccount_id = '6493618146'
# adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
# print (adwords_client)
# print ("================================================================")
# list_camp = GetCampaign(adwords_client, acccount_id)

# for camp in list_camp:
# 	print (camp)


# from googleads import adwords
# adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
# campaign_service = adwords_client.GetService('CampaignService', version='v201708')


# import sys
# import os
# import pandas as pd
# import numpy as np
# import json
# import cx_Oracle
# from datetime import datetime , timedelta, date

# def Read_NRU_for_month(cursor, year, month, product):
# 	#==================== Get NRU =============================
# 	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
# 	and extract (Year from SNAPSHOT_DATE) = :1 and extract (Month from SNAPSHOT_DATE) = :2 "
# 	cursor.execute(statement, (year, month))
# 	list_NRU = list(cursor.fetchall())  
# 	# print(list_NRU)


# 	#==================== Get product ID ===================
# 	statement = "Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT where PRODUCT_ID = '" + product +  "'"
# 	cursor.execute(statement)
# 	list_product = list(cursor.fetchall())

# 	ccd_nru = 0  
# 	list_nru = []
# 	for i in range(len(list_NRU)):
# 		list_NRU[i] = list(list_NRU[i])    
# 		for pro in list_product:
# 			if (list_NRU[i][1] == pro[1]):
# 				data = [list_NRU[i][0], list_NRU[i][1], list_NRU[i][2], pro[0], pro[1]]
# 				if data not in list_nru:					
# 					list_nru.append(data)
# 					ccd_nru += list_NRU[i][2] 
	
# 	return ccd_nru 
        



# def add_NRU_monthly_for_plan(connect, path_folder, list_plan):
# # ==================== Connect database =======================
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()

# 	for plan in list_plan:
# 		if ('MONTHLY' in value):
# 			for i in range(len(value['MONTHLY'])):
# 				plan['MONTHLY']['CCD_NRU'] = Read_NRU_for_month(cursor, value['CYEAR'], str(value['MONTHLY'][i]['MONTH']), value['PRODUCT'])

# 	cursor.close()
# 	return list_plan



# def AddNRU(connect, path_data, date):
	

# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# conn = cx_Oracle.connect(connect)
# cursor = conn.cursor()
# nru = Read_NRU_for_month(cursor, '8', '219')
# print(nru)




import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date
import time


def Insert(name, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_RUN_FLAG (FLAG_RUNNING, FINAL_RUNTIME) \
	values (:1, :2) '
		
	cursor.execute(statement, (name, None))
	
	print("A row inserted!.......")
	conn.commit()
	print("Committed!.......")



connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
conn = cx_Oracle.connect(connect)
cursor = conn.cursor()
path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/TEST_UNICODE.json'
# path = 'D:/WorkSpace/GG_Tool/New folder/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/MCC_TEST_UNICODE.json'
with open(path, 'r') as fi:
	data = json.load(fi)
# import _locale
# _locale._getdefaultlocale = (lambda *args: ['vi-VN', 'utf-8'])
for acc in data:
	if (str(acc["customerId"]) == '4476024314'):
		print(u'PG10- V\u1ea1n Linh Ti\u00ean C\u1ea3nh')
		print(acc["name"])
		print('\u1ea1'.decode('utf-16'))
		print(type(acc["name"]))
		print(unicode(acc["name"]))
		print(acc["name"].encode('utf-16'))
		print(acc["name"].decode('utf-16'))

		# Insert(acc["name"].encode('utf-8'), cursor)
		# # Insert(acc["name"].encode('cp437'), cursor)
		# Insert(acc["name"].encode('ISO-8859-1'), cursor)
		# Insert(acc["name"], cursor)
		
		
		# Insert(acc["name"].decode('utf-8'), cursor)


cursor.close()

# path_no_video = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON_NO_VIDEO'
# path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# list_folder = next(os.walk(path))[1]
# for folder in list_folder:
# 	name = folder + '/video_url_' + folder + '.json'
# 	path_in = path_no_video + '/' + name
# 	path_out = path + '/' + name
# 	print (path_in)
# 	print (path_out)
# 	if os.path.exists(path_in) and os.path.exists(path_out):
# 		with open(path_in, 'r') as fi:
# 			data = json.load(fi)

# 		with open (path_out,'w') as f:
# 			json.dump(data, f)


# ######################### Khong xoa ##################################################
# #-------------- Do data audit ------------------
# def InsertContentAds(cursor, ads, d):
# 	statement = 'insert into STG_AUDIT_CONTENT ( \
# 	AD_ID, PRODUCT_ID, CONTENT, TYPE, PREDICT_PERCENT, \
# 	INDEX_CONTENT, SNAPSHOT_DATE, INSERT_DATE) \
# 	values (:1, :2, :3, :4, :5, :6, :7, :8)'
# 	print (ads['list_product'])
# 	print (ads['ad_id'])
# 	if ads['list_product'] != []:
# 		#-------- Insert image ---------------
# 		list_image = ads['audit_content']['image_urls']
# 		if list_image != []:
# 			for i, image in enumerate(list_image):
# 				print (list_image)
# 				print ('insert ------------------')
# 				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], image['image_url'], 'image_url', 0, i,  \
# 				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()))

# def add_label_video_to_data(connect, path, date_, to_date_):
# 	# Lấy danh sách path của các file json cần tổng hợp data
# 	list_folder = next(os.walk(path))[1]

# 	#========================== Auto run ===================
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()
# 	date = datetime.strptime(date_, '%Y-%m-%d').date()
# 	to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
# 	for folder in list_folder:
# 		print (folder)
# 		d = datetime.strptime(folder, '%Y-%m-%d').date()
# 		if d <= to_date and d >= date:
# 			path_folder = os.path.join(path, folder)
# 			path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
# 			print (path_file)
# 			print ("--------------------")
# 			if os.path.exists(path_file):
# 				with open(path_file, 'r') as f:
# 					data = json.load(f)
# 					for ads in data['my_json']:
# 						print ('ads====================')
# 						InsertContentAds(cursor, ads, str(d))
# 	conn.commit()
# 	cursor.close()


# # path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
# # path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# # path = 'D:/DATA/NEW_DATA_10-2016_05-2017/FULL_DATA_10-2016_06-2017/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# # path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# # date_ = '2016-11-26'
# # to_date_ = '2016-12-10'

# if __name__ == '__main__':
#     from sys import argv
#     path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'  
#     connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'  
#     script, date, to_date = argv
#     add_label_video_to_data(connect, path, date, to_date)


##################################################################################################




