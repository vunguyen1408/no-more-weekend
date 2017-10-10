import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
import logging
from datetime import datetime , timedelta, date
from googleads import adwords

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


PAGE_SIZE = 100


def GetCampaign(client, acccount_id):
  # Initialize appropriate service.
  client.SetClientCustomerId(str(acccount_id))
  print ("================================================================")
  campaign_service = client.GetService('CampaignService', version='v201708')
  print ("================================================================")

  # Construct selector and get all campaigns.
  offset = 0
  # selector = {
  #     'fields': ['Id', 'Name', 'Status'],
  #     'paging': {
  #         'startIndex': str(offset),
  #         'numberResults': str(PAGE_SIZE)
  #     }
  # }



  selector = {
      'fields': ['Id', 'Name', 'Status','Amount'
      ,'BaseCampaignId','BiddingStrategyName','BiddingStrategyType'
      ,'BudgetId','BudgetName','BudgetStatus'
      ,'StartDate','EndDate'
    #  ,'frequencyCap','advertisingChannelType','advertisingChannelSubType'
    # ,'labels'
    ],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
  }

  from datetime import datetime , timedelta, date
  more_pages = True
  list_camp = []
  while more_pages:
    page = campaign_service.get(selector)
    print(page['totalNumEntries'])

    # Display results.
    
    if 'entries' in page:
      for campaign in page['entries']:
        camp = {
          'CAMPAIGN_NAME' : campaign['name'],
          'CAMPAIGN_ID' : campaign['id'],
          'ACCOUNT_ID' : acccount_id
        }
        list_camp.append(camp)
        print(camp)
    else:
      print('No campaigns were found.')
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])
    time.sleep(1)

  return list_camp


acccount_id = '6493618146'
adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
print ("================================================================")
list_camp = GetCampaign(adwords_client, acccount_id)

for camp in list_camp:
	print (camp)










