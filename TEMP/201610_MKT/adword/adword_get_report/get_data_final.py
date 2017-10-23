'''
  Get campaign for account and save to folder
'''
import json
import os
import logging
import sys
from googleads import adwords
import get_accounts as get_accounts
import add_acc_name_into_data as add_account


logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)

def TSVtoJson(report_string, date):  
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
        elif (ele[i].find('%') == len(ele[i]) - 1) and (ele[i].replace("%", "").replace(".", "").isdigit()):         # Percent  
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
  print (result)
  return result


def DateToString(date):
  date = date[:-6] + date[5:-3] + date[8:]
  return date

def DownloadOnDate(adwords_client, customerId, path, date):
  import json
  import os

  startDate = DateToString(date)
  endDate = DateToString(date)
  path_folder = os.path.join(path, date + '/' + 'ACCOUNT_ID/' + customerId)
  print (path_folder)
  if not os.path.exists(path_folder):
    os.makedirs(path_folder)

    #====================== CAMPAIGN ====================
    result_campaign = DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate)  
    path_file_campaign = os.path.join(path_folder, 'campaign_' + date)
    
    result_json = TSVtoJson(result_campaign, date)
    
    for i in range(len(result_json)):
      result_json[i]['Account ID'] = customerId

    with open (path_file_campaign + '.json','w') as f:
      json.dump(result_json, f)
    print("Download....................")

  
def GetCampainForAccount(path, customerId, day, to_day):
  import json
  from sys import argv
  import os
  from datetime import datetime , timedelta, date


  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage('D:/WorkSpace/Adwords/Finanlly/AdWords/adwords_python3/googleads.yaml')

  date_ = datetime.strptime(day, '%Y-%m-%d').date()
  to_date_ = datetime.strptime(to_day, '%Y-%m-%d').date()
  n = int((to_date_ - date_).days)

  for i in range(n + 1):
    single_date = date_ + timedelta(i)
    d = single_date.strftime('%Y-%m-%d')
    DownloadOnDate(adwords_client, customerId, path, str(d))




def Data_Final(path_acc, path_camp, startDate, endDate):
  #=============== Get campaigns of all accounts ===============  
  list_customer_id, list_customer = add_account.get_list_customer(path_acc)  
  print(len(list_customer), len(list_customer_id))
  print()  

  for customer_id in list_customer_id:  
    GetCampainForAccount(path_camp, customer_id, startDate, endDate)


  # =========== Add account name into campaigns ==============
  add_account.addAccName(path_camp, list_customer, list_customer_id)
    


path_acc = 'D:/WorkSpace/Adwords/Finanlly/AdWords/FULL_DATA'
path_camp = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEMP_DATA'
startDate = '2017-07-01' 
endDate = '2017-07-01'
Data_Final(path_acc, path_camp, startDate, endDate)




# import json
# path_mcc  = 'D:\WorkSpace\Adwords\Finanlly\AdWords\FULL_DATA/MCC.json'
# list_mcc = []
# list_mcc_id = []

# with open(path_mcc, 'r') as fi:
#   data = json.load(fi)
# for value in data:
#   if (value['dept'] == 'MP2'):
#     print(value['name'])







