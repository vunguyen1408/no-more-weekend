'''
  Get campaing for account and save to folder
'''


import logging
import sys
from googleads import adwords

# import get_accounts as get_accounts


logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)

def TSVtoJson(report_string, date):
  from collections import defaultdict
  #========= Get key for json ================
  list_pre = ['enabled', 'paused', 'removed']
  list_key = []
  fi = report_string.split('\n')

  for line in fi:
    ele = line.split('\t')
    if (ele[0] not in list_pre) and (ele[0] != 'Total') and (len(ele) > 1):     
        list_key = ele

  #======== Convert a line to dictionary
  list_json = []

  for line in fi:
    ele = line.split('\t')
    dict_campaign = {}
    if (ele[0] not in list_key) and (len(ele) > 1 ):
      for i in range(len(list_key)):          
        if (list_key[i] == 'Cost') or ((list_key[i].find('Avg') >= 0) and (list_key[i] != 'Avg. position')):   # Cost            
          ele[i] = float(float(ele[i]) / 1000000)
        elif (ele[i].isdigit()):     # Integer        
          ele[i] = int(ele[i])
        elif (ele[i].find('%') > 0):         # Percent  
          ele[i] = float(ele[i].replace("%", ""))
        elif (ele[i].replace(".", "").isdigit()):    # Float        
          ele[i] = float(ele[i])          
        elif (ele[i] == ' --'):              # Empty        
          ele[i] = ""         
        elif (ele[i].find('[') > 0 and ele[i].find(']') > 0):# and ele[i].find(',') > 0):                 
          list_ = ele[i].split(',')
          for u in range(len(list_)):             
            s = list_[u]
            for v in range(len(s)):                   
              if (s[v].isalpha() == False):
                list_[u] = s.replace(s[v], '')

          list_[0] = list_[0][1:len(list_[0])]
          list_[-1] = list_[-1][0:len(list_[0]) -1]   
          ele[i] = list_

        dict_campaign[list_key[i]] = ele[i]
      dict_campaign['Mapping'] = False
      dict_campaign['Date'] = date
      if ((dict_campaign['Cost'] > 0)):
        list_json.append(dict_campaign)
  return list_json


def DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate):

  adwords_client.SetClientCustomerId(customerId)
  print (customerId)
  report_downloader = adwords_client.GetReportDownloader(version='v201708')
  result = []
  report = {
      'reportName': 'Custom date CAMPAIGN_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
      'downloadFormat': 'TSV',
      'selector': {
        'dateRange':{'min':startDate,'max':endDate},
        'fields': [
          'CampaignStatus',
          'CampaignName',
          'AdvertisingChannelType',
          'AdvertisingChannelSubType',
          'CampaignId',
          #, #budget
          'ServingStatus',
          'Clicks',
          'Impressions',
          'ImpressionReach',
          'Ctr',
          'AverageCpc',
          'AverageCpm',
          'Cost',
          'Conversions',
          'BiddingStrategyType',
          'InvalidClicks',
          'AveragePosition',
          'Engagements',
          'AverageCpe',
          'VideoViewRate',
          'VideoViews',
          'AverageCpv',
          'AverageCost',
          'InteractionTypes',
          'Interactions',
          'InteractionRate',
          'VideoQuartile25Rate',
          'VideoQuartile50Rate',
          'VideoQuartile75Rate',
          'VideoQuartile100Rate',
          'VideoViews',
          'StartDate',
          'EndDate'

          ]
      }
  }
  result = report_downloader.DownloadReportAsString(
      report, skip_report_header=True, skip_column_header=False,
      skip_report_summary=False, include_zero_impressions=True)
  return result


def DownloadAdGroupOfCampaign(adwords_client, customerId, startDate, endDate):

  adwords_client.SetClientCustomerId(customerId)
  print (customerId)
  report_downloader = adwords_client.GetReportDownloader(version='v201708')

  report = {
      'reportName': 'Custom date ADGROUP_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'ADGROUP_PERFORMANCE_REPORT',
      'downloadFormat': 'TSV',
      'selector': {
          'dateRange':{'min':startDate,'max':endDate},
          'fields': [
          'AdGroupStatus',
          'AdGroupName',
          'AdGroupId',
          'AdGroupType',
          #budget
          'CampaignId',
          'CampaignName',
          'Clicks',
          'Impressions',
          'Ctr',
          'AverageCpc',
          'AverageCpm',
          'AverageCpv',
          'AverageCost',
          'Cost',
          'Conversions',
          'AveragePosition',
          'Engagements',
          'AverageCpe',
          'VideoViewRate',
          'VideoViews',
          'InteractionTypes',
          'Interactions',
          'InteractionRate',
          'VideoQuartile25Rate',
          'VideoQuartile50Rate',
          'VideoQuartile75Rate',
          'VideoQuartile100Rate',
          'VideoViews',
          ]#,
          # 'predicates': [
          #     {
          #         'field': 'CampaignId',
          #         'operator': 'EQUALS',
          #         'values': [campaignId]
          #     }
          # ]
      }
  }

  result = report_downloader.DownloadReportAsString(
      report, skip_report_header=True, skip_column_header=False,
      skip_report_summary=False, include_zero_impressions=True)
  return result

def DownloadAdsOfAdGroup(adwords_client, customerId, startDate, endDate):

  adwords_client.SetClientCustomerId(customerId)
  print (customerId)
  report_downloader = adwords_client.GetReportDownloader(version='v201708')

  report = {
      'reportName': 'Custom date AD_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'AD_PERFORMANCE_REPORT',
      'downloadFormat': 'TSV',
      'selector': {
          'dateRange':{'min':startDate,'max':endDate},
          'fields': [
          'AdGroupStatus',
          'AdGroupName',
          'AdGroupId',
          'Id',
          'AdType',
          'Status',
          #budget
          'CampaignId',
          'CampaignName',
          'Clicks',
          'Impressions',
          'Ctr',
          'AverageCpc',
          'AverageCpm',
          'AverageCpv',
          'AverageCost',

          'Cost',
          'AveragePosition',
          'Engagements',
          'AverageCpe',
          'VideoViewRate',
          'VideoViews',
          'InteractionTypes',
          'Interactions',
          'InteractionRate',
          'VideoQuartile25Rate',
          'VideoQuartile50Rate',
          'VideoQuartile75Rate',
          'VideoQuartile100Rate',
          'VideoViews',

          ]#,
          # 'predicates': [
          #     {
          #         'field': 'CampaignId',
          #         'operator': 'EQUALS',
          #         'values': [campaignId]
          #     },
          #     {
          #         'field': 'AdGroupId',
          #         'operator': 'EQUALS',
          #         'values': [adGroupId]
          #     }
          # ]
      }
  }

  result = report_downloader.DownloadReportAsString(
      report, skip_report_header=True, skip_column_header=False,
      skip_report_summary=False, include_zero_impressions=True)
  return result


def DateToString(date):
  date = date[:-6] + date[5:-3] + date[8:]
  return date

def DownloadOnDate(adwords_client, customerId, path, date):
  import json
  import os

  startDate = DateToString(date)
  endDate = DateToString(date)
  path_folder = os.path.join(path, 'ACCOUNT_ID/' + customerId + '/' + date)
  print (path_folder)
  if not os.path.exists(path_folder):
    os.makedirs(path_folder)

  #====================== CAMPAIGN ====================
  result_campaign = DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate)
  path_file_campaign = os.path.join(path_folder, 'campaign_' + date)
  # with open(path_file_campaign + '.tsv', 'wb') as f:
  #   f.write(result_campaign.encode('utf-8'))
  result_json = TSVtoJson(result_campaign, date)
  with open (path_file_campaign + '.json','w') as f:
    json.dump(result_json, f)

  # #===================== ADGROUP ======================
  # result_adgroup = DownloadAdGroupOfCampaign(adwords_client, customerId, startDate, endDate)
  # path_file_adgroup = os.path.join(path_folder, 'campaign_' + date)
  # with open(path_file_adgroup + '.tsv', 'wb') as f:
  #   f.write(result_adgroup.encode('utf-8'))
  # result_json = TSVtoJson(result)
  # with open (path_file_adgroup + '.json','w') as f:
  #   json.dump(result_adgroup, f)

  # #===================== ADS ======================
  # result_ads = DownloadAdsOfAdGroup(adwords_client, customerId, startDate, endDate)
  # path_file_ads = os.path.join(path_folder, 'campaign_' + date)
  # with open(path_file_ads + '.tsv', 'wb') as f:
  #   f.write(result_ads.encode('utf-8'))
  # result_json = TSVtoJson(result)
  # with open (path_file_ads + '.json','w') as f:
  #   json.dump(result_ads, f)

# Date: 2017-06-01
def GetCampainForAccount(path, customerId, day, to_day):
  import json
  from sys import argv
  import os
  from datetime import datetime , timedelta, date


  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/googleads.yaml')

  date_ = datetime.strptime(day, '%Y-%m-%d').date()
  to_date_ = datetime.strptime(to_day, '%Y-%m-%d').date()
  n = int((to_date_ - date_).days)

  for i in range(n + 1):
    single_date = date_ + timedelta(i)
    d = single_date.strftime('%Y-%m-%d')
    DownloadOnDate(adwords_client, customerId, path, str(d))



path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_GG'
JXM = '5008396449'
ZTM = '9021114325'
JXW = '9420329501'
v = '7976533276'
# customerId = '9021114325'
list_customer_id = ['5008396449', '9021114325', '9420329501']
for customer_id in list_customer_id:
  date = '2017-06-01' 
  to_date = '2017-06-30'
  GetCampainForAccount(path, customer_id, date, to_date)
  
  








