import logging
import time
import logging
import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime , timedelta, date
from googleads import adwords
# ----------------- package -----------------
import mapping_campaign_plan as mapping
import insert_data_map_to_total as insert_to_total
import history_name as history_name
# import down_load_name as down_load_name 

import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install
import insert_data_map as detail_map

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


PAGE_SIZE = 100


def GetCampaign(client, acccount_id):
  # Initialize appropriate service.
  from datetime import datetime , timedelta, date

  client.SetClientCustomerId(str(acccount_id))
  campaign_service = client.GetService('CampaignService', version='v201708')

  path_log = 'C:/Users/ltduo/Desktop/log.csv'  
  fi = open(path_log, 'a+') 
  
  line = (datetime.now().strftime('%Y-%m-%d') + '\t' + acccount_id + '\t' + 'CampaignService' + '\n')
  fi.write(line)

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

  # from datetime import datetime , timedelta, date
  more_pages = True
  list_camp = []
  while more_pages:
    page = campaign_service.get(selector)

    # Display results.
    
    if 'entries' in page:
      for campaign in page['entries']:
        camp = {
          'CAMPAIGN_NAME' : str(campaign['name']),
          'CAMPAIGN_ID' : campaign['id'],
          'ACCOUNT_ID' : acccount_id
        }
        list_camp.append(camp)
    else:
      print('No campaigns were found.')
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])
    # time.sleep(1)

  return list_camp

def DownloadNameOfAccount(adwords_client, customerId, date, to_date, path_log):

  startDate = down_load_name.DateToString(date)
  endDate = down_load_name.DateToString(to_date)
  print(startDate)
  print(endDate)
  #====================== CAMPAIGN ====================
  result_campaign = down_load_name.DownloadCampaignOfCustomer(adwords_client, customerId, startDate, endDate, path_log)
  result_json = down_load_name.TSVtoJson(result_campaign, date)
  print(len(result_json))
  list_name = []
  for campaign in result_json:
    if (campaign['Cost'] > 0) and campaign['Campaign state'] != 'Total':
      camp = {
            'CAMPAIGN_NAME' : campaign['Campaign'],
            'CAMPAIGN_ID' : str(campaign['Campaign ID']),
            'ACCOUNT_ID' : customerId
          }
      list_name.append(camp)
  # print(len(list_name))
  return list_name

def GetListCampOfAccount(list_customer, date, to_date):
  # import time
  # adwords_client = adwords.AdWordsClient.LoadFromStorage()
  # list_camp = []
  # for acccount in list_customer:
  #   time.sleep(5)
  #   temp = GetCampaign(adwords_client, acccount)
  #   list_camp.extend(temp)


  import time
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  list_camp = []
  
  path_log = 'C:/Users/LAP11529-local/Desktop/log.txt'
  for acccount in list_customer:
    time.sleep(5)
    temp = DownloadNameOfAccount(adwords_client, acccount, date, to_date, path_log)
    # temp = GetCampaign(adwords_client, acccount)
    list_camp.extend(temp)

  # ====== Write file name get on date =============
  list_camp_json = {}
  list_camp_json['history_name'] = list_camp

  path_data_total_map = 'C:/Users/LAP11529-local/Desktop/history_name.json'
  with open (path_data_total_map,'w') as f:
    json.dump(list_camp_json, f)
  return list_camp


def CheckNameChange(path_data, list_customer, date):
  import time
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


    # Neu tim khong thay thi tao file moi
  if not find:
    data_total = {}
    data_total['HISTORY'] = []

    path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
    path_data_total_map = path_data_his
    with open (path_data_his,'w') as f:
      json.dump(data_total, f)

  list_diff = []
  if find:
    with open (path_data_total_map,'r') as f:
      data_total = json.load(f)

    # print(path_data_total_map)
    # list_camp = GetListCampOfAccount(list_customer)
    path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/history_name.json'
    list_camp = []
    with open (path,'r') as f:
      list_camp = json.load(f)
    list_camp = list_camp['history_name']

    temp_ = []
    if len(data_total['HISTORY']) > 0:
      for camp_ in list_camp:
        if camp_['CCAMPAIGN_ID'] == '953916682':
          print (camp_)
        if str(camp_['CAMPAIGN_ID']) == '794232395' or str(camp_['CAMPAIGN_ID']) == '713543033':
          print (camp_['CAMPAIGN_NAME'])
        flag = history_name.FindNameNew(data_total['HISTORY'], str(camp_['CAMPAIGN_ID']), camp_['CAMPAIGN_NAME'])
        if flag == -1:
          list_diff.append(camp_)
          # print(camp_)
          temp = {
            'ACCOUNT_ID': camp_['ACCOUNT_ID'],
            'CAMPAIGN_ID' : str(camp_['CAMPAIGN_ID']),

            'CAMPAIGN_NAME' : camp_['CAMPAIGN_NAME'],
            'DATE_GET' : str(date),
            'UPDATE_DATE': str(date),
            'IMPORT_DATE' : None
          }

          data_total['HISTORY'].append(temp)
    else:
      for camp_ in list_camp:
        if camp_['CCAMPAIGN_ID'] == '953916682':
          print (camp_)
        temp = {
          'ACCOUNT_ID': camp_['ACCOUNT_ID'],
          'CAMPAIGN_ID' : str(camp_['CAMPAIGN_ID']),

          'CAMPAIGN_NAME' : camp_['CAMPAIGN_NAME'],
          'DATE_GET' : str(date),
          'UPDATE_DATE': str(date),
          'IMPORT_DATE' : None
          }
        list_diff.append(camp_)
        data_total['HISTORY'].append(temp)

    print (len(list_diff))
    # time.sleep(5)
    #----------- Write file history new ----------------------
    path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
    print(path_folder)
    if not os.path.exists(path_folder):
      os.makedirs(path_folder)

    path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
    ###########################################
    # with open (path_data_his,'w') as f:
    #   json.dump(data_total, f)
    ############################################
  print("====================== Length =================")
  return (list_diff, data_total)


#================= Mapping campaign and plan WPL =====================
def Map(path_folder, list_plan, list_campaign, date):
  import time 
  print (len(list_plan))
  print (len(list_campaign))
  list_campaign_map = []
  number = 0

  path_data = os.path.join(path_folder, str(date) + '/LOG_MANUAL')
  path_data_manual = os.path.join(path_data, 'log_manual.json')
  path_data_un_map = os.path.join(path_data, 'log_un_map.json')

  with open (path_data_manual,'r') as f:
    data_manual = json.load(f)
  with open (path_data_un_map,'r') as f:
    data_un_map = json.load(f)

  # for j, camp in enumerate(list_campaign):
  #   if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
  #     list_campaign_map.append(camp)

  # print(len(list_campaign))
  for j, camp in enumerate(list_campaign):
    # print (camp)
    t = False
    # print ("==========================================")
    camp['Advertising Channel'] = mapping.ChangeCampaignType(camp['Advertising Channel'])
    if 'Plan' not in camp:
      camp['Plan'] = None
      camp['STATUS'] = None

    date_ = datetime.strptime(camp['Date'], '%Y-%m-%d')

    for i, eform in enumerate(list_plan): 
      flag = True 
      if 'CAMPAIGN' not in eform:
        eform['CAMPAIGN'] = []
        eform['STATUS'] = None

      # -------------------- Choose time real ------------------------
      start, end = mapping.ChooseTime(eform)
      start = datetime.strptime(start, '%Y-%m-%d')
      end = datetime.strptime(end, '%Y-%m-%d')

      t = True

      # Duonglt check mapping auto
      if str(eform['REASON_CODE_ORACLE']) == '1710027' and str(camp['Campaign ID']) == '952021132':
        print (camp)
        print (eform)

      if (camp['Mapping'] == False): 
        flag = False
        if mapping.LogManualMap(data_manual, camp, eform, date, 1) == 1:
          flag = True
        else:
          #============= WPL -================
          if camp['Dept'].find('WPL') >= 0:
            if (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
              and (mapping.checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
              or mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) ) \
              and (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) \
              and (date_ >= start) \
              and (date_ <= end) ) \
              and  ( mapping.LogManualMap(data_un_map, camp, eform, date, 2) == 1):
              flag = True
              # print("mapping WPL")
          else:
            # ============= GS5 ================
            if camp['Dept'].find('GS5') >= 0:
              type_campaign = mapping.GetCampaignTypeOfGS5(camp['Campaign'])
              if (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
                # and (checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
                and mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) \
                and (eform['FORM_TYPE'].find(type_campaign) >= 0) \
                and (date_ >= start) \
                and (date_ <= end) ) \
                and  ( mapping.LogManualMap(data_un_map, camp, eform, date, 2) ):
                flag = True
                # print("mapping GS5")

            else:
              try:
                product_id = (camp['Campaign'].split('|'))[1]
              except IndexError as e:
                product_id = ''
              if(  (eform['PRODUCT_CODE'] != [] or eform['CCD_PRODUCT'] != []) and \
                (
                  mapping.checkProductCode(camp['Campaign'], eform['PRODUCT_CODE']) or \
                  # checkProductCode(camp['Campaign'], eform['CCD_PRODUCT']) or \
                  # checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) or \
                  mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) or \
                  product_id.find(str(eform['PRODUCT'])) >= 0
                )
                and \
                (camp['Campaign'].find(str(eform['REASON_CODE_ORACLE'])) >= 0) \
                and (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) 
                and (date_ >= start) 
                and (date_ <= end) ) \
                and ( mapping.LogManualMap(data_un_map, camp, eform, date, 2) == 1): 
                flag = True
                # if t:
                #   print("mapping =====================================\n\n\n")
        if flag:
          camp['Mapping'] = True
          camp['STATUS'] = 'SYS'
          
          eform['CAMPAIGN'].append(camp)
          number += 1


      if camp['Mapping'] == False and camp['Dept'].find('GS5') >= 0:
        for i, eform in enumerate(list_plan):  
          if 'CAMPAIGN' not in eform:
            eform['CAMPAIGN'] = []
            eform['STATUS'] = None
          # -------------------- Choose time real ------------------------
          start, end = mapping.ChooseTime(eform)
          start = datetime.strptime(start, '%Y-%m-%d')
          end = datetime.strptime(end, '%Y-%m-%d')

          unit_option = mapping.GetUnitOptionOfGS5(camp['Account Name'])
          if (eform['DEPARTMENT_NAME'] == 'GS5'): 

            if (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
              # and (checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
              and mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) \
              and (eform['UNIT_OPTION'].find(unit_option) >= 0) \
              and (date_ >= start) \
              and (date_ <= end) ) \
              or  ( mapping.LogManualMap(data_un_map, camp, eform, date, 2) == 1 ): 
              # print("mapping GS5")
              camp['Mapping'] = True
              camp['STATUS'] = 'SYS'      
              eform['CAMPAIGN'].append(camp)
              number += 1
  
  number = 0
  list_un_campaign = []
  for camp in list_campaign:
    if camp['Mapping'] == False:
      camp['STATUS'] = ""
      list_un_campaign.append(camp)
    else:
      number += 1

  data_map = {}
  data_map['UN_CAMP'] = list_un_campaign
  data_map['PLAN'] = list_plan
  print(" -------------- Mapping------ ", number)
  print(" -------------- Un mapping------ ", len(list_un_campaign))
  return data_map

def CacualatorChange(connect, path_data, list_diff, date):

  list_camp_need_remove = []
  path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
  path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
  
  if not os.path.exists(path_data_total_map):
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
    # ---- Neu tim thay file total truoc do -----
  else:
    find = True

  if find:
    # print(path_data_total_map)
    data_total = {}
    data_total['TOTAL'] = []
    data_total['UN_CAMP'] = []
    with open (path_data_total_map,'r') as f:
      data_total['TOTAL'] = json.load(f)

    with open (path_data_un_map,'r') as f:
      data_total['UN_CAMP'] = json.load(f)

    list_camp_find = []
    # print ("=========================")
    print (len(data_total['UN_CAMP']))
    # print (len(list_diff))
    for camp in list_diff:
      # print(camp)
      for campaign in data_total['UN_CAMP']:
        if str(camp['CAMPAIGN_ID']) == str(campaign['Campaign ID']):    
          temp = campaign
          campaign['Campaign'] = camp['CAMPAIGN_NAME']
          temp['Campaign'] = camp['CAMPAIGN_NAME']
          if int(campaign['Date'][5:-3]) == 10:
            list_camp_find.append(temp)
          

    list_camp_update = list_camp_find # Update name
    for camp in list_camp_find:
      if camp['Campaign ID'] == '953916682':
            print (camp)
    for camp in list_diff:
      if camp['CAMPAIGN_ID'] == '953916682':
            print (camp)
    # mp2 = 0
    # pg1 = 0
    # pg2 = 0
    # pg3 = 0
    # wpl = 0
    # for camp in list_camp_find:
    #   if camp['Account Name'].find('MP2') >= 0:
    #     mp2 += 1
    #   if camp['Campaign'].find('1708050') >= 0:
    #     pg2 += 1
    #   if camp['Account Name'].find('PG1') >= 0:
    #     pg1 += 1
    #   if camp['Account Name'].find('PG1') >= 0:
    #     pg3 += 1
    #   if camp['Account Name'].find('PG1') >= 0:
    #     wpl += 1

    # print("Camp MP2", mp2)
    # print("Camp PG1", pg1)
    # print("Camp PG2", pg2)
    # print("Camp WPL", wpl)
    # print("Camp PG3", pg3)

    mapping.ReadProductAlias(connect, path_data, date)
    list_plan = mapping.ReadPlanFromTable(connect, path_data, date)
    list_plan = mapping.ReadPlan(path_data, date)


    import time

    print (len(list_camp_find))
    # list_camp_find = list_camp_find[:1000]

    print("MAP")
    start = time.time()
    data_map = Map(path_data, list_plan['plan'], list_camp_find, date)
    print ("Mapping: ", (time.time() - start))

    ############## check code
    data_map = {}
    data_map['PLAN'] = []
    #####################

    start = time.time()
    data_total, list_plan_insert, list_plan_remove = insert_to_total.AddToTotal (data_total, data_map, date)
    print ("add to total: ", (time.time() - start))
    print (len(list_plan_remove))

    list_map_all, list_plan_un, list_map_ = detail_map.CreateDataMap(data_map['PLAN'])

    for camp in list_map_:
      for campaign in data_total['UN_CAMP']:
        if str(camp['Campaign ID']) == str(campaign['Campaign ID']) \
          and str(camp['Date']) == str(campaign['Date']):
          data_total['UN_CAMP'].remove(campaign)

    data_total['TOTAL'] = insert_to_total.CaculatorForPlan(data_total['TOTAL'])

    import time
    start = time.time()
    data_total['TOTAL'] = insert_install.InsertInstallToPlan(data_total['TOTAL'], connect, date)
    data_total['TOTAL'] = insert_install_brandingGPS.AddBrandingGPSToPlan(data_total['TOTAL'], connect, date)
    print ("Insert install: ", (time.time() - start))

    


    # print (list_map_[0])
    list_plan_remove_unmap = list_plan_remove
    list_camp_need_remove = list_map_
    list_plan_update = data_total['TOTAL']

    print("camp update", len(list_camp_update))
    print("plan update", len(list_plan_update))
    print("plan remove", len(list_plan_remove_unmap))
    print("camp remove", len(list_camp_need_remove))
    # print (list_camp_need_remove[0])

    ###########################################
    # path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
    # with open (path_data_total_map,'w') as f:
    #   json.dump(data_total['TOTAL'], f)

    # path_data_un_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'un_map_camp' + '.json')
    # with open (path_data_un_map,'w') as f:
    #   json.dump(data_total['UN_CAMP'], f)
    ##########################################

  return (list_plan_remove_unmap, list_camp_need_remove, list_plan_update, list_camp_update)


# list_customer_id = [ 
#   # WPL
#   '1033505012', '6376833586', '6493618146', '3764021980', '9019703669', \
#   '5243164713', '1290781574', '8640138177', '1493302671', '7539462658', \
#   '1669629424', '6940796638', '6942753385', '3818588895', '8559396163', \
#   '9392975361', '1756174326', '5477521592', '7498338868', '6585673574', \
#   '5993679244', '5990401446', '5460890494', '3959508668', '1954002502', \
#   '1124503774', '2789627019', '5219026641', '8760733662', '8915969454', \
#   '9299123796', \
#   # MP2
#   '2351496518', '3766974726', '8812868246', '3657450042', '4092061132', \
#   '1066457627', '7077229774', '6708858633', '2205921749', '1731093088', \
#   '2852598370', \
#   # PG1
#   '5008396449', '9021114325', '9420329501', '7976533276', \
#   # PG2
#   '5471697015', '8198035241', '8919123364', '8934377519', '7906284750', \
#   '1670552192', '6507949288', '3752996996', '5515537799', '9280946488', \
#   '8897792146', '4732571543', '6319649915', '4845283915', '4963434062', \
#   '3950481958', '8977015372', \
#   # PG3
#   '2018040612', '1237086810', '2474373259', '9203404951', '8628673438', \
#   '5957287971', '6267264008', '8583452877', '4227775753', '8003403685', \
#   '3061049910', '2395877275', '1849103506', '7000297269', '6233988585', \
#   '4018935765', '2675507443', '9493600480', '1609917649', '8180518027', \
#   '6275441244', '6743848595', '1362424990', '5430766142', '5800450880', \
#   '7687258619', '8303967886', '5709003531', '6201418435', '1257508037', \
#   '6810675582', '5953925776', '9001610198', '8135096980', '5222928599', \
#   '9963010276', '5062362839', '6360800174', '8844079195', '5856149801', \
#   '3064549723', '6198751560', '9034826980', '3265423139', '7891987656', \
#   '8483981986', '2686387743', '5930063870', '7061686256', '3994588490', \
#   '3769240354', \
#   # GS5
#   '8726724391', '1040561513', '7449117049', '3346913196', '9595118601', \
#   '9411633791', '4596687625', '8290128509', '3104172682', '6247736011', \
#   '2861959872', \
  
#   # PP
#   '8024455693' 
#   ]
# list_ = ['5062362839', '6360800174', '8180518027', '9001610198', '9493600480']

# date = '2017-01-01'
# to_date = '2017-09-30'
# GetListCampOfAccount(list_customer_id, date, to_date)

# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'

# # CacualatorChange(path_data, list_customer, date)

# list_diff = CheckNameChange(path_data, list_customer_id, date)
# CacualatorChange(path_data, list_diff, date)