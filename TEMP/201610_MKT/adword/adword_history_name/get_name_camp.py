#!/usr/bin/env python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This example gets all campaigns.

To add a campaign, run add_campaign.py.

The LoadFromStorage method is pulling credentials and properties from a
"googleads.yaml" file. By default, it looks for this file in your home
directory. For more information, see the "Caching authentication information"
section of our README.

"""


import logging
import time
from googleads import adwords

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


PAGE_SIZE = 100


def GetCampaign(client, acccount_id):
  # Initialize appropriate service.
  client.SetClientCustomerId(str(acccount_id))
  campaign_service = client.GetService('CampaignService', version='v201708')

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


list_customer_id = ['6140218608', '5984796540', '5939646969', '4869930138', '4626844359', \
          '4047293553', '3581453552', '3320825113', '1954002502', '1547282976', \
          '8579571740', '8324896471', '7017079687', '7013760267', '6408702983', \
          '1492278512', '1124503774', '5219026641', '8915969454', '9299123796', \
          '3381621349', '7986041343', '9377025866', '6102951142', '3668363407', '4895791874', \
          '1373058452', '3699371994', '1496752295', '6787284625', '4780719992', \
          '5515537799', '3436962801', '7802963373', '5925380036', '3836577058', \
          '1163330677', '1057617213', '4798268655', '8812868246', '7976533276', \
          '9420329501', '3785612315', '9719199461', '1912353902', '4585745870', \
          '9358928000', '4566721209', '1547282976', '1359687200', '1124503774', \
          '5219026641', '8760733662', '5460890494', '4270191371', '4219579467', \
          '3959508668', '1954002502', '6585673574', '5993679244', '5990401446', \
          '7498338868', '9392975361', '9294243048', '7886422201', '6940796638', \
          '6942753385', '3818588895', '8640138177', '1493302671', '7539462658', \
          '5243164713', '9019703669', '3764021980', '8024455693', '7077229774', \
          '6708858633', '1066457627', '4092061132', '3346913196', '5886101084', \
          '3752996996', '8353864179', '1033505012', '5008396449', '6319649915', \
          '1290781574', '1669629424', '6376833586', '6493618146', '9021114325']

list_customer = ['6140218608', '5984796540']

def GetListCampOfAccount(list_customer):
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  list_camp = []
  for acccount in list_customer:
    list_camp.extend(GetCampaign(adwords_client, acccount))
  return list_camp


def CheckNameChange(path_data, list_customer, date):

  path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')

  if not os.path.exists(path_data_total_map):
    i = 0
    find = True
    date_before = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(1)
    path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
    while not os.path.exists(path_data_total_map):
      i = i + 1
      date_before = date_before - timedelta(1)
      path_data_total_map = os.path.join(path_data + '/' + str(date_before) + '/DATA_MAPPING', 'history_name' + '.json')
      if i == 60:
        find = False
        break
    # ---- Neu tim thay file total truoc do -----
  else:
    find = True


  list_diff = []
  if find:
    with open (path_data_total_map,'r') as f:
      data_total = json.load(f)


    list_camp = GetListCampOfAccount(list_customer)
    for camp in data_total['HISTORY']:
      for camp_ in list_camp:
        if str(camp['CAMPAIGN_ID']) == str(camp_['CAMPAIGN_ID']) \
          and camp['CAMPAIGN_NAME'] != camp_['CAMPAIGN_NAME'] \
          and camp['ACCOUNT_ID'] == camp_['ACCOUNT_ID']:
          camp['CAMPAIGN_NAME'] = camp_['CAMPAIGN_NAME']
          list_diff.append(camp)

    #----------- Write file history new ----------------------
    path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
    if not os.path.exists(path_folder):
      os.makedirs(path_folder)

    path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
    with open (path_data_his,'w') as f:
      json.dump(data_total, f)
    
  return list_diff

def CacualatorChange(path_data, list_customer, date):

  list_diff = CheckNameChange(path_data, list_customer, date)
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
      data_total = json.load(f)

    list_camp_find = []
    for camp in list_diff:
      for campaign in data_total['UN_CAMPAIGN']:
        if camp['CAMPAIGN_ID'] == campaign['Campaign ID'] and camp['CAMPAIGN_NAME'] != campaign['Campaign']:
          campaign['CAMPAIGN_NAME'] = camp['Campaign']
          list_camp_find.append(campaign)

    list_plan = ReadPlan(path_folder, date)
    # -------------- Call mapping ----------------
    data_map = MapAccountWithCampaign(path_data, list_plan, list_camp_find, date)





GetListCampOfAccount()