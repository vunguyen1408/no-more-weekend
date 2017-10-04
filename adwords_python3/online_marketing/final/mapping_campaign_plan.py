
import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

#-------------- import package -----------------
import insert_nru_to_data as nru




# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)

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

def LogManualMap(path_data, campaign, plan, date):
  # path_folder = os.path.join(path_data, str(date) + '/LOG_MANUAL')
  # path_data_total_map = os.path.join(path_folder, 'log_manual.json')
  # with open (path_data_total_map,'r') as f:
  #   data_manual_map = json.load(f)
  return False

#================= Mapping campaign and plan =====================
def MapAccountWithCampaign(path_folder, list_plan, list_campaign, date):
  date_ = datetime.strptime(date, '%Y-%m-%d') 
  now = datetime.strptime('2017-03-01', '%Y-%m-%d')
  list_campaign_map = []
  number = 0
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
          (date_ <= datetime.strptime(eform['END_DAY_ESTIMATE'], '%Y-%m-%d')) and (now <= datetime.strptime(eform['START_DAY'], '%Y-%m-%d')) ) \
          or \
          ( LogManualMap(path_folder, camp, eform, date) ):   
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
          number += 1

  data_map = {}
  data_map['campaign'] = list_campaign_map
  data_map['plan'] = list_plan
  # print (" -------------- Mapping------ ", number)
  # print (" -------------- Un mapping------ ", len(list_campaign_map) - number)
  return data_map


def ReadProductAlias(connect, path_data, date):
  file_product = os.path.join(path_data, str(date) + '/PLAN/product_alias.json')
  # ==================== Connect database =======================
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()
  statement = 'select PRODUCT_ID, GG_PRODUCT, CCD_PRODUCT from ODS_META_PRODUCT'        
  cursor.execute(statement)
  res = list(cursor.fetchall())
  list_json = []
  for product in res:
    if product[0] is not None:
      json_ = {
        'PRODUCT_ID': product[0],
        'GG_PRODUCT': product[1],
        'CCD_PRODUCT' : product[2]
      }
      list_json.append(json_)
  data_json = {}
  data_json['ALIAS'] = list_json
  with open(file_product, 'w') as fo:
    json.dump(data_json, fo)
  cursor.close()


def AddProductCode(path_folder, list_plan, date):
  #================ Add product id to plan =================
  file_product = os.path.join(path_folder, str(date) + '/PLAN/product_alias.json')
  with open(file_product, 'r') as fo:
    data = json.load(fo)

  list_temp = []
  for plan in list_plan['plan']:
    temp = plan
    temp['PRODUCT_CODE'] = ''
    for alias in data['ALIAS']:
      if (alias['PRODUCT_ID'] is not None) and (alias['GG_PRODUCT'] is not None) \
      and (int(plan['PRODUCT']) == int(alias['PRODUCT_ID'])):
        temp['PRODUCT_CODE'] = str(alias['GG_PRODUCT'])     
    list_temp.append(temp)
  # for p in list_temp:
  #   print (p['PRODUCT_CODE'])
  
  list_plan['plan'] = list_temp
  return list_plan


def ReadPlanFromTable(connect, path_folder, date):
  import datetime
  folder = os.path.join(path_folder, str(date) + '/PLAN')
  if not os.path.exists(folder):
    os.makedirs(folder)
  # print (folder)
  file_plan = os.path.join(folder, 'plan.json')

  #============================== Connect database =============================
  conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
  cursor = conn.cursor()

  #======================= Get data from database ==============================
  query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, PRODUCT, REASON_CODE_ORACLE, EFORM_NO, \
          START_DAY, END_DAY_ESTIMATE, CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, CVALUE, \
          ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, INSTALL, NRU, INSERT_DATE \
      from STG_FA_DATA_GG'

  cursor.execute(query)
  row = cursor.fetchall()
  temp = list(row)
  cursor.close()
  


  #===================== Convert data into json =================================

  list_key = ['CYEAR', 'CMONTH', 'LEGAL', 'DEPARTMENT', 'DEPARTMENT_NAME', 'PRODUCT', 
        'REASON_CODE_ORACLE', 'EFORM_NO', 'START_DAY', 'END_DAY_ESTIMATE', 'CHANNEL', 
        'FORM_TYPE', 'UNIT_OPTION', 'UNIT_COST', 'AMOUNT_USD', 'CVALUE', 'ENGAGEMENT', 
        'IMPRESSIONS', 'CLIKE', 'CVIEWS', 'INSTALL', 'NRU', 'INSERT_DATE']

  list_json= []
  for plan in temp: 
    list_temp = []
    unmap = {}
    for value in plan:
      val = value   
      if isinstance(value, datetime.datetime):            
        val = value.strftime('%Y-%m-%d')
      # elif (not isinstance(value, int)) and (value is not None) and (not isinstance(value, float)):
      #   if (value.isdigit() or value.replace(".", "").isdigit()):
      #     val = float(value)
      list_temp.append(val)
    for i in range(len(list_key)):
      unmap[list_key[i]] = list_temp[i]
    list_json.append(unmap)
  plan_ = {}
  plan_['plan'] = list_json

  #================ Add product id to plan =================
  ReadProductAlias(connect, path_folder, date)
  nru.ReadNRU(connect, path_folder, date)

  plan_ = AddProductCode(path_folder, plan_['plan'], date)
  list_json = plan_['plan']
  list_json = nru.AddNRU(path_folder, list_json, date)
  plan_['plan'] = list_json
  
  with open (file_plan, 'w') as f:
    json.dump(plan_, f)


def ReadPlan(path_folder, date):
  # =============== List plan code ================  
  file_plan = os.path.join(path_folder, str(date) +  '/PLAN/plan.json')
  list_plan = {}
  with open (file_plan, 'r') as f:
    list_plan = json.load(f)
    return list_plan



#================= Read list plan, product code, save file mapping =====================
def MapData(customer, path_folder, date): 

  # =============== List plan code ================
  list_plan = ReadPlan(path_folder, date)

  #================ Add product id to plan =================
  # list_plan = AddProductCode(path_folder, list_plan, date)

  data_map = []

  # # #=========== Map Account with Campaign =======================  
  path = os.path.join(path_folder, str(date) + '/ACCOUNT_ID/' + customer)
  file_campaign = os.path.join(path, 'campaign_' + str(date) + '.json')
  # Neu list campaign lay duoc ve khac rong
  with open (file_campaign, 'r') as f:
    list_campaign = json.load(f)
  # print (len(list_campaign))
  if len(list_campaign) > 0:

    data_map = MapAccountWithCampaign(path_folder, list_plan['plan'], list_campaign, date)

    #----------------- Write file map and unmap ------------------
  path_data_map = os.path.join(path, 'mapping_' + str(date) + '.json')
  with open (path_data_map,'w') as f:
    json.dump(data_map, f)

  #-------------- Check mapped ----------
  # path_data_map = os.path.join(path, 'mapping_' + str(date) + '.json')
  # if not os.path.exists(path_data_map):
  #   #--------------------------------------

  #   with open (file_campaign, 'r') as f:
  #     list_campaign = json.load(f)

  #   data_map = MapAccountWithCampaign(path_folder, list_plan['plan'], list_campaign, date)

  #   #----------------- Write file map and unmap ------------------
  #   path_data_map = os.path.join(path, 'mapping_' + str(date) + '.json')
  #   with open (path_data_map,'w') as f:
  #     json.dump(data_map, f)
  # else:
  #   print ("----------------- MAPPED -------------------")
  return data_map

def MapDataForAllAccount(list_customer, path_folder, date):
  for customer in list_customer:
    path_customer = os.path.join(path_folder, str(date) + '/ACCOUNT_ID/' + customer)
    if os.path.exists(path_customer):
      MapData(customer, path_folder, date)




def MapWithDate(customer_id, path_data, start_date, end_date):
  startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
  endDate = datetime.strptime(end_date, '%Y-%m-%d').date()   

  date = startDate
  list_campaign_unmapping = []
  list_eform_unmapping = []

  while(date <= endDate):
    # print ("===========================================================")
    # print (date)
    #========== map data ============
    path_folder = os.path.join(path_data, customer_id)
    path_date = os.path.join(path_folder, str(date))
  
    data_map = MapData(customer_id, path_data, str(date))
    date = date + timedelta(1)

# startDate = '2017-06-01'
# endDate = '2017-06-01'
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/END'
# list_customer_id = ['5008396449', '9021114325', '9420329501']
# for customer_id in list_customer_id:
#   MapWithDate(customer_id, path_data, startDate, endDate)
