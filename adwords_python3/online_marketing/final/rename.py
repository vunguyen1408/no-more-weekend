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
import down_load_name as down_load_name 

import insert_nru_into_data as insert_nru_into_data
import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install

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
  print(len(list_name))
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
    # time.sleep(5)
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


  list_diff = []
  if find:
    with open (path_data_total_map,'r') as f:
      data_total = json.load(f)

    print(path_data_total_map)
    # list_camp = GetListCampOfAccount(list_customer)
    path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/history_name.json'
    list_camp = []
    with open (path,'r') as f:
      list_camp = json.load(f)
    list_camp = list_camp['history_name']

    ###########check code : duonglt 23-10 : 12:44 PM#################
    # for camp_ in list_camp:
    #     if str(camp_['CAMPAIGN_ID']) == '734063969':
    #       print(camp_['CAMPAIGN_NAME'])
    #       print(" TTTTTTTTTTT im thay")
    #       time.sleep(5)
    # print("================= history ======================")
    ############################

    print(len(list_camp))
    temp_ = []
    for camp_ in list_camp:
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
    time.sleep(5)
    #----------- Write file history new ----------------------
    path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
    print(path_folder)
    if not os.path.exists(path_folder):
      os.makedirs(path_folder)

    path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
    ###########################################
    with open (path_data_his,'w') as f:
      json.dump(data_total, f)
    ############################################
  print("====================== Length =================")
  return (list_diff, data_total)


# list_customer = ['6140218608', '5984796540']
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/DATA_MAPPING'
# date = '2017-06-01'
# list_diff = CheckNameChange(path_data, list_customer, date)
# print(list_diff)

#================= Mapping campaign and plan WPL =====================
def Map(path_folder, list_plan, list_campaign, date):
  import time 

  list_campaign_map = []
  number = 0
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)


  for j, camp in enumerate(list_campaign_map):
    t = False
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

      # if str(camp['Campaign ID']) == '794232395' and eform['REASON_CODE_ORACLE'] == '1708062' \
      #   and eform['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN':
      #   print(camp)
      #   print(eform)
        # print("\n\n")
      t = True

      if (camp['Mapping'] == False): 
        flag = False
        #============= WPL -================
        if camp['Dept'].find('WPL') >= 0:
          if (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
            and (mapping.checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
            or mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) ) \
            and (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) \
            and (date_ >= start) \
            and (date_ <= end) ) \
            or  ( mapping.LogManualMap(path_folder, camp, eform, date) ):
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
              or  ( mapping.LogManualMap(path_folder, camp, eform, date) ):
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
              or ( mapping.LogManualMap(path_folder, camp, eform, date) ): 
              flag = True
              # if t:
              #   print("mapping =====================================\n\n\n")
        if flag:
          camp['Mapping'] = True
          plan = {}
          plan['PRODUCT_CODE'] = eform['PRODUCT_CODE']
          plan['CCD_PRODUCT'] = eform['CCD_PRODUCT']
          plan['REASON_CODE_ORACLE'] = eform['REASON_CODE_ORACLE']
          plan['FORM_TYPE'] = eform['FORM_TYPE']

          camp['Plan'] = plan

          campaign = {}
          campaign['CAMPAIGN_ID'] = str(camp['Campaign ID'])
          campaign['Date'] = camp['Date']

          temp = eform['CAMPAIGN']
          temp.append(campaign)
          eform['CAMPAIGN'] = temp

          camp['STATUS'] = 'SYS'
          eform['STATUS'] = 'SYS'
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
              or  ( mapping.LogManualMap(path_folder, camp, eform, date) ): 
              # print("mapping GS5")
              camp['Mapping'] = True
              plan = {}
              plan['PRODUCT_CODE'] = eform['PRODUCT_CODE']
              plan['CCD_PRODUCT'] = eform['CCD_PRODUCT']
              plan['REASON_CODE_ORACLE'] = eform['REASON_CODE_ORACLE']
              plan['FORM_TYPE'] = eform['FORM_TYPE']

              camp['Plan'] = plan

              campaign = {}
              campaign['CAMPAIGN_ID'] = camp['Campaign ID']
              campaign['Date'] = camp['Date']

              temp = eform['CAMPAIGN']
              temp.append(campaign)
              eform['CAMPAIGN'] = temp

              camp['STATUS'] = 'SYS'
              eform['STATUS'] = 'SYS'
              number += 1

  data_map = {}
  data_map['campaign'] = list_campaign_map
  data_map['plan'] = list_plan
  print(" -------------- Mapping------ ", number)
  print(" -------------- Un mapping------ ", len(list_campaign_map) - number)
  return data_map

def CacualatorChange(connect, path_data, list_diff, date):

  # list_diff = CheckNameChange(path_data, list_customer, date)
  path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')

  list_camp_need_remove = []
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
    print(path_data_total_map)
    with open (path_data_total_map,'r') as f:
      data_total = json.load(f)

    list_camp_find = []

    for camp in list_diff:
      # print(camp)
      for campaign in data_total['UN_CAMPAIGN']:
        if str(camp['CAMPAIGN_ID']) == str(campaign['Campaign ID']):
          # print(campaign)
        # if str(camp['CAMPAIGN_ID']) == str(campaign['Campaign ID']) and camp['CAMPAIGN_NAME'] != campaign['Campaign']: 
          # print(campaign)      
          temp = campaign
          campaign['Campaign'] = camp['CAMPAIGN_NAME']
          temp['Campaign'] = camp['CAMPAIGN_NAME']
          list_camp_find.append(temp)
          # print(temp['Campaign'] + '  ===================  ' + str(temp['Campaign ID'] ))

    # print(list_camp_find)
    list_camp_update = list_camp_find # Update name
    mp2 = 0
    pg1 = 0
    pg2 = 0
    for camp in list_camp_find:
      if camp['Account Name'].find('MP2') >= 0:
        mp2 += 1
      if camp['Campaign'].find('1708050') >= 0:
        pg2 += 1
      if camp['Account Name'].find('PG1') >= 0:
        pg1 += 1

    print("Camp MP2", mp2)
    print("Camp PG1", pg1)
    print("Camp PG2", pg2)

    # print (list_camp_find)
    # mapping.ReadPlanFromTable(connect, path_data, str(date))
    list_plan = mapping.ReadPlan(path_data, date)
    list_plan['plan'] = mapping.AddProductCode(path_data, list_plan['plan'], date)
    # -------------- Call mapping ----------------
    # print(len(list_camp_find))
    # for plan in list_plan['plan']:
    #   if plan['REASON_CODE_ORACLE'] == '1708062' and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN':
    #     print(plan)


    data_map = Map(path_data, list_plan['plan'], list_camp_find, date)
    # for plan in list_plan['plan']:
    #   if 'CAMPAIGN' not in plan:
    #     print (plan)

    # for plan in data_map['plan']:
    #   if plan['REASON_CODE_ORACLE'] == '1708062' and plan['FORM_TYPE'] == 'UNIVERSAL_APP_CAMPAIGN':
    #     print(plan)
    plan_sum, list_map_temp = insert_to_total.SumTotalManyPlan(data_map['plan'], data_map['campaign'])


    list_plan = plan_sum

    
    list_plan_update = [] # Update plan change cost
    list_plan_remove_unmap = [] # Remove camp plan un map
    list_camp_need_remove = list_map_temp  # Remove campaign mapped
    data_total['MAP'].extend(list_map_temp)
    for plan in list_plan:
      # print(plan)
      flag = True
      for plan_total in data_total['TOTAL']:
        if plan_total['PRODUCT'] == plan['PRODUCT'] \
          and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
          and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
          and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:
          plan_total['TOTAL_CAMPAIGN'] = insert_to_total.SumTwoTotal(plan_total['TOTAL_CAMPAIGN'], plan['TOTAL_CAMPAIGN'])
          flag = False

      #----- Không tìm thấy trong total ------
      if flag:
        # --------------- Tạo các thông tin month cho plan trước khi add --------------
        data_total['TOTAL'].append(plan)


    # print(list_plan)

    # -------- Xoa cac camp da duoc mapping lai ra khoi un map ----------
    for camp in list_map_temp:
      for campaign in data_total['UN_CAMPAIGN']:
        if camp['Campaign ID'] == campaign['Campaign ID'] \
          and camp['Date'] == campaign['Date']:
          data_total['UN_CAMPAIGN'].remove(campaign)

    # -------- Xoa cac plan da duoc mapping lai ra khoi un map ----------
    for plan in list_plan:
      for plan_un in data_total['UN_PLAN']:
        if plan_un['PRODUCT'] == plan['PRODUCT'] \
          and plan_un['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
          and plan_un['FORM_TYPE'] == plan['FORM_TYPE'] \
          and plan_un['UNIT_OPTION'] == plan['UNIT_OPTION'] :
          data_total['UN_PLAN'].remove(plan_un)
          list_plan_remove_unmap.append(plan_un)

    print("---------------------------------------------------")
    for plan in data_total['TOTAL']:
      plan['MONTHLY'] = {}
      plan = insert_to_total.CaculatorTotalMonth(plan, date)

      # print(plan)
    print("---------------------------------------------------")

    for plan in data_total['UN_PLAN']:
      plan['MONTHLY'] = {}
      plan = insert_to_total.CaculatorTotalMonth(plan, date)

    import time
    start = time.time()
    data_total = insert_nru_into_data.AddNRU(connect, data_total, date)
    data_total = insert_install.InsertInstallToPlan(data_total, connect, date)
    data_total = insert_install_brandingGPS.AddBrandingGPSToPlan(data_total, connect, date)
    print ("Time : ", time.time() - start)

    # for plan in data_total['TOTAL']:
    #     plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualTotal(plan)
    #     for m in plan['MONTHLY']:
    #       m['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualMonthly(plan, m)

    #------------ Get list plan update ---------------
    for plan in list_plan:
      for plan_total in data_total['TOTAL']:
        if plan_total['PRODUCT'] == plan['PRODUCT'] \
          and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
          and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
          and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:
          list_plan_update.append(plan_total)
          # print(plan)
    # # ------------- Remove campaign mapped ----------------
    # for camp in data_map['campaign']:
    #   if camp['Plan'] == None:
    #     list_camp_need_removed.append(camp)
    #     for campaign in data_total['UN_CAMPAIGN']:
    #       if camp['Campaign ID'] == campaign['Campaign ID'] and camp['Date'] == campaign['Date']:
    #         print(campaign)
    #         data_total['UN_CAMPAIGN'].remove(campaign)

    # data_total = insert_to_total.AddToTotal(data_total, data_map, date)

    print("camp update", len(list_camp_update))
    print("plan update", len(list_plan_update))
    print("plan remove", len(list_plan_remove_unmap))
    print("camp remove", len(list_camp_need_remove))
    print("======== Mapped =================")
    print("camp update", len(data_total['MAP']))

    ###########################################
    path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')
    with open (path_data_total_map,'w') as f:
      json.dump(data_total, f)
    ##########################################

  return (list_plan_remove_unmap, list_camp_need_remove, list_plan_update, list_camp_update)


list_customer_id = [ 
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
# # list_ = ['5062362839', '6360800174', '8180518027', '9001610198', '9493600480']
import add_acc_name as add_acc_name
path_acc = 'C:/Users/LAP11529-local/Desktop/VNG/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT'
list_acc, list_mcc, list_dept = add_acc_name.get_list_customer(path_acc)
date = '2017-01-01'
to_date = '2017-11-05'
GetListCampOfAccount(list_acc, date, to_date)
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'

# # CacualatorChange(path_data, list_customer, date)

# list_diff = CheckNameChange(path_data, list_customer_id, date)
# CacualatorChange(path_data, list_diff, date)