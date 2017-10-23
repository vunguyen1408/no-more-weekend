'''
  Get campaing for account and save to folder
'''
import json
import logging
import sys
import os
from googleads import adwords
from datetime import datetime, timedelta
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
        if (list_key[i] == 'Cost') or ((list_key[i].find('Avg') >= 0) and (list_key[i] != 'Avg. position')):         # Cost            
          ele[i] = float(float(ele[i]) / 1000000)
        elif (ele[i].isdigit()):         # Integer        
          ele[i] = int(ele[i])
        elif (ele[i].find('%') == len(ele[i]) - 1) and (ele[i].replace("%", "").replace(".", "").isdigit()):         # Percent  
          ele[i] = float(ele[i].replace("%", ""))
        elif (ele[i].replace(".", "").isdigit()):         # Float        
          ele[i] = float(ele[i])          
        elif (ele[i] == ' --'):      # Empty        
          ele[i] = ""         
        elif (ele[i].find('[') > 0 and ele[i].find(']') > 0):                
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

  path_log = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/log.txt'  
  fi = open(path_log, 'a+') 
  
  date = startDate[0:4] + '-' + startDate[4:6] + '-' + startDate[6:]
  line = (datetime.now().strftime('%Y-%m-%d'), '\t',  customerId, '\t', date, '\t', 'GetReportDownloader','\n')
  fi.writelines(line)
  print("Save ok")


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
  
  startDate = DateToString(date)
  endDate = DateToString(date)
  path_folder = os.path.join(path, date + '/' + 'ACCOUNT_ID/' + customerId)
  print (path_folder)
  if not os.path.exists(path_folder):
    os.makedirs(path_folder)

  #====================== CAMPAIGN ====================
    result_campaign = DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate)
    print(result_campaign)
    path_file_campaign = os.path.join(path_folder, 'campaign_' + date)
    # with open(path_file_campaign + '.tsv', 'wb') as f:
    #   f.write(result_campaign.encode('utf-8'))
    result_json = TSVtoJson(result_campaign, date)
    print (result_json)
    for i in range(len(result_json)):
      result_json[i]['Account ID'] = customerId
    with open (path_file_campaign + '.json','w') as f:
      json.dump(result_json, f)
  else:
    print("Da get campaign...........")

def GetCampainForAccount(path, customerId, day, to_day):
  
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage('D:/WorkSpace/Adwords/Finanlly/AdWords/adwords_python3/googleads.yaml')

  date_ = datetime.strptime(day, '%Y-%m-%d').date()
  to_date_ = datetime.strptime(to_day, '%Y-%m-%d').date()
  n = int((to_date_ - date_).days)

  for i in range(n + 1):
    single_date = date_ + timedelta(i)
    d = single_date.strftime('%Y-%m-%d')
    DownloadOnDate(adwords_client, customerId, path, str(d))





list_account = [ 
# WPL
'1033505012', '6376833586', '6493618146', '3764021980', '9019703669', \
'5243164713', '1290781574', '8640138177', '1493302671', '7539462658', \
'1669629424', '6940796638', '6942753385', '3818588895', '8559396163', \
'9392975361', '1756174326', '5477521592', '7498338868', '6585673574', \
'5993679244', '5990401446', '5460890494', '3959508668', '1954002502', \
'1124503774', '2789627019', '5219026641', '8760733662', '8915969454', \
'9299123796', \
# MP2
'2351496518', '3766974726', '8812868246', '3657450042', '4092061132', \
'1066457627', '7077229774', '6708858633', '2205921749', '1731093088', \
'2852598370', \
# PG1
'5008396449', '9021114325', '9420329501', '7976533276', \
# PG2
'5471697015', '8198035241', '8919123364', '8934377519', '7906284750', \
'1670552192', '6507949288', '3752996996', '5515537799', '9280946488', \
'8897792146', '4732571543', '6319649915', '4845283915', '4963434062', \
'3950481958', '8977015372', \
# PG3
'2018040612', '1237086810', '2474373259', '9203404951', '8628673438', \
'5957287971', '6267264008', '8583452877', '4227775753', '8003403685', \
'3061049910', '2395877275', '1849103506', '7000297269', '6233988585', \
'4018935765', '2675507443', '9493600480', '1609917649', '8180518027', \
'6275441244', '6743848595', '1362424990', '5430766142', '5800450880', \
'7687258619', '8303967886', '5709003531', '6201418435', '1257508037', \
'6810675582', '5953925776', '9001610198', '8135096980', '5222928599', \
'9963010276', '5062362839', '6360800174', '8844079195', '5856149801', \
'3064549723', '6198751560', '9034826980', '3265423139', '7891987656', \
'8483981986', '2686387743', '5930063870', '7061686256', '3994588490', \
'3769240354', \
# GS5
'8726724391', '1040561513', '7449117049', '3346913196', '9595118601', \
'9411633791', '4596687625', '8290128509', '3104172682', '6247736011', \
'2861959872', \
# PP
'8024455693' 
]


#============== Get Campaign for all account =============
path = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEMP_DATA'

date = '2017-05-01' 
to_date = '2017-07-31'
for customer_id in list_account:  
  GetCampainForAccount(path, customer_id, date, to_date)



# ============== Add account name ========================
# import add_acc_name_into_data as AccName
# path_data = 'D:/WorkSpace/Adwords/Finanlly/AdWords/FULL_DATA'
# list_mcc_id, list_mcc = AccName.get_list_customer(path_data)
# print(len(list_mcc))
# print(len(list_mcc_id))
# path = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/MP2_T8'
# AccName.addAccName(path, list_mcc, list_mcc_id)



# =============== Check acc not in list acc ===============
# path_data = path
# list_date = next(os.walk(path_data))[1]

# for date in list_date:
#   path_temp = os.path.join(path_data, date + '/ACCOUNT_ID')
#   list_acc = next(os.walk(path_temp))[1]    
#   for acc in list_acc:
#     if acc not in list_account:
#       print(acc)


