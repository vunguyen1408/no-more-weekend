import logging
import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime , timedelta, date
from googleads import adwords

import get_accounts as get_accounts


# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)

def loadProductToListjson(file_product, list_json):
  product = pd.read_excel(file_product)
  list_pro_code = list(product['Product'])
  list_pro_id = list(product['Product ID'])
  for value in list_json:
    if (int(value['PRODUCT']) in list_pro_id):  
      value['PRODUCT_CODE'] = list_pro_code[list_pro_id.index(int(value['PRODUCT']))]
    else:
      value['PRODUCT_CODE'] = ""
  return list_json

def ChangeCampaignType(campaign_type):
  if campaign_type.find('Multi Channel') == 0:
    campaign_type = 'UNIVERSAL_APP_CAMPAIGN'
  if campaign_type.find('Search') == 0:
    campaign_type = 'SEARCH'
  if campaign_type.find('Display') == 0:
    campaign_type = 'DISPLAY'
  if campaign_type.find('Video') == 0:
    campaign_type = 'VIDEO'
  return campaign_type

def LogManualMap(campaign, plan):
  return False

#================= Mapping campaign and plan =====================
def MapAccountWithCampaign(list_plan, list_campaign, date):
  date_ = datetime.strptime(date, '%Y-%m-%d') 
  list_campaign_map = []
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)

  for i, eform in enumerate(list_plan):  
    flag = True
    eform['CAMPAIGN'] = []
    eform['STATUS'] = None
    for j, camp in enumerate(list_campaign_map):
      camp['Advertising Channel'] = ChangeCampaignType(camp['Advertising Channel'])
      if 'Plan' not in camp:
        camp['Plan'] = None
        camp['STATUS'] = None
      if (camp['Mapping'] == False):   
        if (  (eform['PRODUCT_CODE'] != '') and (camp['Campaign'].find(eform['PRODUCT_CODE']) == 0) and \
          (camp['Campaign'].find(str(eform['REASON_CODE_ORACLE'])) >= 0) and \
          (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) == 0) and \
          (date_ >= datetime.strptime(eform['START_DAY'], '%Y-%m-%d')) and \
          (date_ <= datetime.strptime(eform['END_DAY_ESTIMATE'], '%Y-%m-%d'))  ) \
          or \
          ( LogManualMap(camp, eform) ):  
          
          camp['Mapping'] = True
          plan = {}
          plan['PRODUCT_CODE'] = eform['PRODUCT_CODE']
          plan['REASON_CODE_ORACLE'] = eform['REASON_CODE_ORACLE']
          plan['FORM_TYPE'] = eform['FORM_TYPE']

          camp['Plan'] = plan

          campaign = {}
          campaign['CAMPAIGN_ID'] = camp['Campaign ID']
          campaign['Date'] = date

          temp = eform['CAMPAIGN']
          temp.append(campaign)
          eform['CAMPAIGN'] = temp

          camp['STATUS'] = 'SYS'
          eform['STATUS'] = 'SYS'

  data_map = {}
  data_map['campaign'] = list_campaign_map
  data_map['plan'] = list_plan
  return data_map

#================= Read list plan, product code, save file mapping =====================
def MapData(customer, path_folder, date): 
  # =============== List plan code ================  
  file_plan = os.path.join(path_folder, 'PLAN/plan.json')
  list_plan = {}
  with open (file_plan, 'r') as f:
    list_plan = json.load(f)
  #=================================================

  #================ Add product id to plan =================
  file_product = os.path.join(path_folder, 'PLAN/product.xlsx')
  list_plan['plan'] = loadProductToListjson(file_product, list_plan['plan'])
  #=============================================

  # # #=========== Map Account with Campaign =======================  
  # list_campaign = ReadTSVfiletoJson(file_campaign)
  path = os.path.join(path_folder, customer + '/' + str(date))
  file_campaign = os.path.join(path, 'campaign_' + str(date) + '.json')
  with open (file_campaign, 'r') as f:
    list_campaign = json.load(f)

  data_map = MapAccountWithCampaign(list_plan['plan'], list_campaign, date)

  #----------------- Write file map and unmap ------------------
  path_data_map = os.path.join(path, str(date) + '.json')
  with open (path_data_map,'w') as f:
    json.dump(data_map, f)

  # print("So luong campaign: ", len(list_campaign)) 
  # print("So luong data map: ", len(data_map['map']))  
  # print("So luong plan unmap: ", len(data_map['un_map_plan']))
  # print("So luong campaign unmap: ", len(data_map['un_map_campaign']))

  return data_map

#================= Merger data to folder MAPPING =====================
def MergerDataAccount(path_data, customer_id, date):
  #========= List acc folder ===============
  print (path_data)
  path_customer = os.path.join(path_data, customer_id)
  path_folder = os.path.join(path_customer, str(date))

  #------------------- Open json ma and un map on date ------------------------------
  path_data_map_date = os.path.join(path_folder, str(date) + '.json')

  with open (path_data_map_date,'r') as f:
    data_map_date = json.load(f)

  #------------------- Open json ma and un map on date ------------------------------
  path_folder = path_data + '/DATA_MAPPING/' + str(date)
  if not os.path.exists(path_folder):
    os.makedirs(path_folder)
  path_data_map = os.path.join(path_folder, 'mapping_' + str(date) + '.json')

  #-------------------- Init file -----------------------------
  if not os.path.exists(path_data_map):
    data_map = {}
    data_map['campaign'] = []
    data_map['plan'] = []
    with open (path_data_map,'w') as f:
      json.dump(data_map, f)
  #-----------------------------------------------------------

  with open (path_data_map,'r') as f:
    data_map = json.load(f)
  
  #--------------------- DATA MAP ---------------------
  temp_date = data_map_date['campaign']
  temp = data_map['campaign']
  temp.extend(temp_date)
  data_map['campaign'] = temp

  #------------------- DATA UN MAP PLAN -------------------
  if len(data_map['plan']) == 0:
    data_map['plan'] = list(data_map_date['plan'])
  else:
    for plan_date in data_map_date['plan']:
      for plan in data_map['plan']:
        if plan['PRODUCT_CODE'] == plan_date['PRODUCT_CODE'] \
          and plan['REASON_CODE_ORACLE'] == plan_date['REASON_CODE_ORACLE'] \
          and plan['FORM_TYPE'] == plan_date['FORM_TYPE']:
          temp_date = plan_date['CAMPAIGN']
          temp = plan['CAMPAIGN']
          temp.extend(temp_date)
          plan['CAMPAIGN'] = temp
          if (len(temp) > 0):
            plan['STATUS'] = 'SYS'


  #----------------- STATUS -------------------------------------
  # print ("------------------- Status -----------------")
  # print (date)
  # print (customer_id)
  # print ('Map: %d' %len(data_map['map']))
  # print ('Un map Campaign: %d' %len(data_map['un_map_campaign']))
  # print ('Un map Plan: %d' %len(data_map['un_map_plan']))
  #----------------------------------------------------------------

  with open (path_data_map,'w') as f:
    json.dump(data_map, f)

def DataFinalDate(path_data, date):

  #------------------- Open json ma and un map on date ------------------------------
  path_folder = path_data + '/DATA_MAPPING/' + str(date)
  path_data_map = os.path.join(path_folder, 'mapping_' + str(date) + '.json')

  if os.path.exists(path_data_map):
    with open (path_data_map,'r') as f:
      data_map = json.load(f)
    
    #--------------------- DATA MAP ---------------------
    path_folder = path_data + '/DATA_MAPPING'
    path_data_map_final = os.path.join(path_folder, 'mapping_final.json') 

    #-------------------- Init file -----------------------------
    if not os.path.exists(path_data_map_final):
      data_map_final = {}
      data_map_final['campaign'] = []
      data_map_final['plan'] = []
      with open (path_data_map_final,'w') as f:
        json.dump(data_map_final, f)
    #-----------------------------------------------------------
    with open (path_data_map_final,'r') as f:
      data_map_final = json.load(f)

    #--------------------- DATA MAP --------------------
    temp_date = data_map['campaign']
    temp = data_map_final['campaign']
    temp.extend(temp_date)
    data_map_final['campaign'] = temp

    #------------------- DATA UN MAP PLAN -------------------
    if len(data_map_final['plan']) == 0:
      data_map_final['plan'] = list(data_map['plan'])
    else:
      for plan_date in data_map['plan']:
        for plan in data_map_final['plan']:
          if plan['PRODUCT_CODE'] == plan_date['PRODUCT_CODE'] \
            and plan['REASON_CODE_ORACLE'] == plan_date['REASON_CODE_ORACLE'] \
            and plan['FORM_TYPE'] == plan_date['FORM_TYPE']:
            temp_date = plan_date['CAMPAIGN']
            temp = plan['CAMPAIGN']
            temp.extend(temp_date)
            plan['CAMPAIGN'] = temp
            if (len(temp) > 0):
              plan['STATUS'] = 'SYS'

    #--------------- Write file -----------------------------
    with open (path_data_map_final,'w') as f:
      json.dump(data_map_final, f)

def MapWithDate(customer_id, path_data, start_date, end_date):
  startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
  endDate = datetime.strptime(end_date, '%Y-%m-%d').date()   

  date = startDate
  list_campaign_unmapping = []
  list_eform_unmapping = []

  while(date <= endDate):
    print ("===========================================================")
    print (date)
    #========== map data ============
    path_folder = os.path.join(path_data, customer_id)
    path_date = os.path.join(path_folder, str(date))
  
    data_map = MapData(customer_id, path_data, str(date))
    MergerDataAccount(path_data, customer_id, date)
    date = date + timedelta(1)

def ChangeFormatDate(date):
  date = datetime.strptime(date, '%Y-%m-%d')
  return date

def FindNewName(current_name_campaign, id):
  index = -1
  for i, campaign in enumerate(current_name_campaign):
    if campaign['Campaign ID'] == id:
      date = campaign['Date']
      index = i
      break

  for i, campaign in enumerate(current_name_campaign):
    if campaign['Campaign ID'] == id:
      if ChangeFormatDate(date) < ChangeFormatDate(campaign['Date']):
        date = campaign['Date']
        index = i
  return index

#================= Update list current name of a campaign  =====================
def UpdateNewNameCampaign(customer_id, path_data, list_campaign):
  path_folder = os.path.join(path_data, customer_id)
  path_file_name_campaign = os.path.join(path_folder, customer_id + '_current_name_campaign.json')

  with open (path_file_name_campaign,'r') as f:
    current_name_campaign = json.load(f)

  list_campaign_change_name = []
  for new_campaign in list_campaign:
    index = FindNewName(current_name_campaign, new_campaign['Campaign ID'])
    #--------- No finded -------------
    if index == -1:
      list_campaign_change_name.append(new_campaign)
    else:
      if current_name_campaign[index]['Campaign'].find(new_campaign['Campaign']) != 0:
        current_name_campaign.append(new_campaign)
        list_campaign_change_name.append(new_campaign)

  return list_campaign_change_name

#================= Update data final =====================

def DataFinal(path_data, start_date, end_date):

  startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
  endDate = datetime.strptime(end_date, '%Y-%m-%d').date()   

  date = startDate
  while(date <= endDate):
    print ("===========================================================")
    print (date)
    DataFinalDate(path_data, str(date))
    date = date + timedelta(1)

JXM = '5008396449'
ZTM = '9021114325'
JXW = '9420329501'
# customer = '9021114325'
startDate = '2017-06-01'
endDate = '2017-06-30'
path_data = 'C:/Users/ltduo/Desktop/VNG/AdWords/DATA'
# MapWithDate(JXW, path_data, startDate, endDate)
DataFinal(path_data, startDate, endDate)



# if __name__ == '__main__':
#   import json
#   from sys import argv
#   import os
#   from datetime import datetime , timedelta, date

#   script, date, to_date = argv

#   date_ = datetime.strptime(date, '%Y-%m-%d')
#   to_date_ = datetime.strptime(to_date, '%Y-%m-%d')

#   path = 'C:/Users/CPU10145-local/Desktop/DATA'
#   customerId = '5008396449'
#   # Initialize client object.
#   adwords_client = adwords.AdWordsClient.LoadFromStorage()

#   date_ = datetime.strptime(date, '%Y-%m-%d')
#   to_date_ = datetime.strptime(to_date, '%Y-%m-%d')

  
  








