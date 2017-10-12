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


import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

def Read_NRU_for_month(cursor, month, product):
	#==================== Get NRU =============================
	statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
	and  extract (Month from SNAPSHOT_DATE) = :1"
	cursor.execute(statement, (month))
	list_NRU = list(cursor.fetchall())  

	#==================== Get product ID ===================
	statement = 'Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT where PRODUCT_ID = :1'
	cursor.execute(statement, (product))
	list_product = list(cursor.fetchall())

	ccd_nru = 0  
	list_nru = []
	for i in range(len(list_NRU)):
		list_NRU[i] = list(list_NRU[i])    
		for pro in list_product:
			if (list_NRU[i][1] == pro[1]):
				data = [list_NRU[i][0], list_NRU[i][1], list_NRU[i][2], pro[0], pro[1]]
				if data not in list_nru:
					list_nru.append(data)
					ccd_nru += list_NRU[i][2] 

	return ccd_nru 
        



def add_NRU_monthly_for plan(connect, path_folder, list_plan):
# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	for plan in list_plan:
		if ('MONTHLY' in value):
			for i in range(len(value['MONTHLY'])):
				plan['MONTHLY']['CCD_NRU'] = Read_NRU_for_month(cursor, str(value['MONTHLY'][i]['MONTH']), value['PRODUCT'])

	cursor.close()
	return list_plan




nru = Read_NRU_for_month(cursor, '8', '219')
print(nru)






