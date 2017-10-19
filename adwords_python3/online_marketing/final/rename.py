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
    time.sleep(1)

  return list_camp

def GetListCampOfAccount(list_customer):
  adwords_client = adwords.AdWordsClient.LoadFromStorage('C:/Users/ltduo/Desktop/VNG/AdWords/adwords_python3/googleads.yaml')
  list_camp = []
  for acccount in list_customer:
    temp = GetCampaign(adwords_client, acccount)
    list_camp.extend(temp)



  list_camp_json = {}
  list_camp_json['history_name'] = list_camp

  path_data_total_map = 'C:/Users/ltduo/Desktop/history_name.json'
  with open (path_data_total_map,'w') as f:
    json.dump(list_camp_json, f)



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

    print (path_data_total_map)
    # list_camp = GetListCampOfAccount(list_customer)
    path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/history_name.json'
    list_camp = []
    with open (path,'r') as f:
      list_camp = json.load(f)
    list_camp = list_camp['history_name']

    print (len(list_camp))
    for camp_ in list_camp:
      flag = history_name.FindNameNew(data_total['HISTORY'], str(camp_['CAMPAIGN_ID']), camp_['CAMPAIGN_NAME'])
      if flag == -1:
        list_diff.append(camp_)
        temp = {
          'ACCOUNT_ID': camp_['ACCOUNT_ID'],
          'CAMPAIGN_ID' : camp_['CAMPAIGN_ID'],

          'CAMPAIGN_NAME' : camp_['CAMPAIGN_NAME'],
          'DATE_GET' : str(date),
          'UPDATE_DATE': str(date),
          'IMPORT_DATE' : None
        }
        data_total['HISTORY'].append(temp)
        # print (camp_)
    #----------- Write file history new ----------------------
    path_folder = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING')
    print (path_folder)
    if not os.path.exists(path_folder):
      os.makedirs(path_folder)

    path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
    ###########################################
    # with open (path_data_his,'w') as f:
    #   json.dump(data_total, f)
    ############################################
  print ("====================== Length =================")
  print (len(list_diff))
  # print (list_diff[0])
  # print (list_diff[1])
  # print (list_diff[2])
  return list_diff


# list_customer = ['6140218608', '5984796540']
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/DATA_MAPPING'
# date = '2017-06-01'
# list_diff = CheckNameChange(path_data, list_customer, date)
# print (list_diff)

#================= Mapping campaign and plan WPL =====================
def Map(path_folder, list_plan, list_campaign, date):
  import time 

  list_campaign_map = []
  number = 0
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)

  for i, eform in enumerate(list_plan):  
    flag = True
    eform['CAMPAIGN'] = []
    eform['STATUS'] = None

    # -------------------- Choose time real ------------------------
    start, end = mapping.ChooseTime(eform)
    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')

    for j, camp in enumerate(list_campaign_map):
      camp['Advertising Channel'] = mapping.ChangeCampaignType(camp['Advertising Channel'])
      if 'Plan' not in camp:
        camp['Plan'] = None
        camp['STATUS'] = None

      date_ = datetime.strptime(camp['Date'], '%Y-%m-%d')

      if (camp['Mapping'] == False): 
        flag = False
        if camp['Account Name'].find('WPL') >= 0:
          if (  (eform['CCD_PRODUCT'] != []) and (mapping.checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
            or mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE'])) and \
            # (camp['Campaign'].find(str(eform['REASON_CODE_ORACLE'])) >= 0) and \
            (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) and \
            (date_ >= start)  and eform['DEPARTMENT_NAME'] == 'WPL'and \
            (date_ <= end) ) \
            or \
            ( mapping.LogManualMap(path_folder, camp, eform, date) ):
            flag = True
        else:
          if (  (eform['PRODUCT_CODE'] != []) and ( mapping.checkProductCode(camp['Campaign'], eform['PRODUCT_CODE']) or \
            (mapping.checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) or mapping.checkProductCode(camp['Account Name'], eform['PRODUCT_CODE'])))
            and \
            (camp['Campaign'].find(str(eform['REASON_CODE_ORACLE'])) >= 0) and \
            (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) and \
            (date_ >= start) and \
            (date_ <= end) ) \
            or \
            ( mapping.LogManualMap(path_folder, camp, eform, date) ): 
            flag = True


          if flag:
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
  print (" -------------- Mapping------ ", number)
  print (" -------------- Un mapping------ ", len(list_campaign_map) - number)
  return data_map

def CacualatorChange(path_data, list_diff, date):

  # list_diff = CheckNameChange(path_data, list_customer, date)
  path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping' + '.json')

  list_camp_need_removed = []
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
    print (path_data_total_map)
    with open (path_data_total_map,'r') as f:
      data_total = json.load(f)

    list_camp_find = []

    for camp in list_diff:
      for campaign in data_total['UN_CAMPAIGN']:
        if camp['CAMPAIGN_ID'] == campaign['Campaign ID'] and camp['CAMPAIGN_NAME'] != campaign['Campaign']:       
          temp = campaign
          campaign['Campaign'] = camp['CAMPAIGN_NAME']
          temp['Campaign'] = camp['CAMPAIGN_NAME']
          list_camp_find.append(temp)

    # print (len(list_camp_find))
    # print (list_camp_find[0])
    # print (list_camp_find[1])
    # print (list_camp_find[2])


    list_plan = mapping.ReadPlan(path_data, date)
    list_plan['plan'] = mapping.AddProductCode(path_data, list_plan['plan'], date)
    # # -------------- Call mapping ----------------
    # print (len(list_camp_find))
    # for camp in list_camp_find:
    #   if camp['Campaign'].find('JXM') >= 0:
    #     print (camp)
    data_map = Map(path_data, list_plan['plan'], list_camp_find, date)

    plan_sum, list_map_temp = insert_to_total.SumTotalManyPlan(data_map['plan'], data_map['campaign'])

    list_plan = plan_sum

    # print (len(plan_sum))
    # print (len(list_map_temp))

    list_camp_update = list_camp_find # Update name
    list_plan_update = [] # Update plan change cost
    list_plan_remove_unmap = [] # Remove camp plan un map
    list_camp_need_remove = list_map_temp  # Remove campaign mapped
    
    for plan in list_plan:
      print (plan)
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


    # print (list_plan)

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

    print ("---------------------------------------------------")
    for plan in data_total['TOTAL']:
      plan['MONTHLY'] = {}
      plan = insert_to_total.CaculatorTotalMonth(plan, date)

      # print (plan)
    print ("---------------------------------------------------")

    for plan in data_total['UN_PLAN']:
      plan['MONTHLY'] = {}
      plan = insert_to_total.CaculatorTotalMonth(plan, date)

    for plan in data_total['TOTAL']:
        plan['TOTAL_CAMPAIGN']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualTotal(plan)
        for m in plan['MONTHLY']:
          m['TOTAL_CAMPAIGN_MONTHLY']['VOLUME_ACTUAL'] = insert_to_total.GetVolumeActualMonthly(plan, m)

    #------------ Get list plan update ---------------
    for plan in list_plan:
      for plan_total in data_total['TOTAL']:
        if plan_total['PRODUCT'] == plan['PRODUCT'] \
          and plan_total['REASON_CODE_ORACLE'] == plan['REASON_CODE_ORACLE'] \
          and plan_total['FORM_TYPE'] == plan['FORM_TYPE'] \
          and plan_total['UNIT_OPTION'] == plan['UNIT_OPTION']:
          list_plan_update.append(plan_total)
          print (plan)
    # # ------------- Remove campaign mapped ----------------
    # for camp in data_map['campaign']:
    #   if camp['Plan'] == None:
    #     list_camp_need_removed.append(camp)
    #     for campaign in data_total['UN_CAMPAIGN']:
    #       if camp['Campaign ID'] == campaign['Campaign ID'] and camp['Date'] == campaign['Date']:
    #         print (campaign)
    #         data_total['UN_CAMPAIGN'].remove(campaign)

    # data_total = insert_to_total.AddToTotal(data_total, data_map, date)

    print (len(list_camp_update))
    print (len(list_plan_update))
    print (len(list_plan_remove_unmap))
    print (len(list_camp_need_remove))


    # path_data_total_map = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'total_mapping_123' + '.json')
    # with open (path_data_total_map,'w') as f:
    #   json.dump(data_total, f)

  return (list_plan_remove_unmap, list_camp_need_removed, list_plan_update, list_camp_update)


list_customer_id = ['1033505012', '1057617213', '1066457627', '1124503774', '1163330677', \
                    '1276490383', '1290781574', '1359687200', '1373058452', '1492278512', \
                    '1493302671', '1496752295', '1547282976', '1669629424', '1912353902', \
                    '1954002502', '2789627019', '3320825113', '3346913196', '3381621349', \
                    '3436962801', '3581453552', '3668363407', '3699371994', '3752996996', \
                    '3764021980', '3785612315', '3818588895', '3836577058', '3959508668', \
                    '4047293553', '4092061132', '4219579467', '4270191371', '4566721209', \
                    '4585745870', '4626844359', '4798268655', '4869930138', '4888935554', \
                    '4895791874', '5008396449', '5219026641', '5243164713', '5460890494', \
                    '5477521592', '5515537799', '5558683488', '5879354452', '5881347043', \
                    '5886101084', '5925380036', '5939646969', '5984796540', '5990401446', \
                    '5993679244', '6102951142', '6140218608', '6223856123', '6319649915', \
                    '6376833586', '6408702983', '6493618146', '6585673574', '6708858633', \
                    '6787284625', '6940796638', '6942753385', '7011814018', '7013760267', \
                    '7017079687', '7077229774', '7498338868', '7539462658', '7802963373', \
                    '7886422201', '7976533276', '7986041343', '8024455693', '8324896471', \
                    '8353864179', '8559396163', '8579571740', '8640138177', '8760733662', \
                    '8812868246', '8915969454', '9019703669', '9021114325', '9294243048', \
                    '9299123796', '9358928000', '9377025866', '9392975361', '9420329501', \
                    '9694660207', '9719199461', '7891987656', '6267264008', '8583452877', \
                    '3099510382', '5471697015', '8198035241', '8919123364', '8934377519', \
                    '7906284750', '1670552192', '3785612315', '6507949288', '3752996996', \
                    '5515537799', '9280946488', '8897792146', '4732571543', '6319649915', \
                    '4845283915', '4963434062', '3950481958', '8977015372', '2018040612', \
                    '1237086810', '2474373259', '9203404951', '8628673438', '5957287971', \
                    '3265423139', '8483981986', '2686387743', '5930063870', '7061686256', \
                    '3994588490', '3769240354', '4227775753', '8003403685', '3061049910', \
                    '2395877275', '1849103506', '7000297269', '6233988585', '4018935765', \

                    '2675507443', '9493600480', '1609917649', '8180518027', '6275441244', \
                    '6743848595', '8726724391', '1040561513', '7449117049', '3346913196', \
                    '9595118601', '1362424990', '5430766142', '9411633791', '4596687625', \
                    '8290128509', '3104172682', '6247736011', '2861959872', '5800450880', \
                    '8303967886', '6201418435', '6810675582', '5953925776', '9001610198', \
                    '8135096980', '5222928599', '9963010276', '5062362839', '6360800174', \
                    '8844079195', '5856149801', '6198751560', '9034826980', '8024455693']

# GetListCampOfAccount(list_customer_id)

path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
date = '2017-08-31'
# CacualatorChange(path_data, list_customer, date)

list_diff = CheckNameChange(path_data, list_customer_id, date)
CacualatorChange(path_data, list_diff, date)