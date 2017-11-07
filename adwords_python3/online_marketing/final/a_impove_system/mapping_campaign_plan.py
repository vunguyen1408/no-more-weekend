import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
import logging
from datetime import datetime , timedelta, date
#-------------- import package -----------------

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)

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

def LogManualMap(data_manual_map, campaign, plan, date, is_manual_map):
  if is_manual_map == 1:
    flag = 0
    for manual in data_manual_map['LOG']:
      # print (manual)
      if str(plan['PRODUCT']) == str(manual['PRODUCT']) \
        and str(plan['REASON_CODE_ORACLE']) == str(manual['REASON_CODE_ORACLE']) \
        and str(plan['FORM_TYPE']) == str(manual['FORM_TYPE']) \
        and str(plan['UNIT_OPTION']) == str(manual['UNIT_OPTION']) \
        and str(campaign['Campaign ID']) == str(manual['CAMPAIGN_ID']):
          flag = 1
          break
    return flag
  if is_manual_map == 2:
    flag = 1
    for manual in data_manual_map['LOG']:
      if str(plan['PRODUCT']) == str(manual['PRODUCT']) \
        and str(plan['REASON_CODE_ORACLE']) == str(manual['REASON_CODE_ORACLE']) \
        and str(plan['FORM_TYPE']) == str(manual['FORM_TYPE']) \
        and str(plan['UNIT_OPTION']) == str(manual['UNIT_OPTION']) \
        and str(campaign['Campaign ID']) == str(manual['CAMPAIGN_ID']):
          flag = 0
          break
    return flag

def ChooseTime(plan):
  if plan['REAL_START_DATE'] is not None:
    start_plan = plan['REAL_START_DATE']
  else:
    start_plan = plan['START_DAY']
    
  if plan['REAL_END_DATE'] is not None:
    end_plan = plan['REAL_END_DATE']
  else:
    end_plan = plan['END_DAY_ESTIMATE']
  return (start_plan, end_plan)

def checkProductCode(name, list_product_code):
  if ('cfmobile' in list_product_code) and name.upper().find('cfmobilesea'.upper()) >= 0:
    return False
  for product in list_product_code:
    if (name.find(product.upper()) >= 0) \
      or (name.find(product.lower()) >= 0) \
      or (name.find(product) >= 0) \
      or ((name.upper()).find(product.upper()) >= 0):
      return True
  return False

def ConvertCampaignToJsonContent(camp):
  campaign = {}
  campaign['Date'] = camp['Date']
  campaign['Conversions'] = camp['Conversions']
  campaign['Invalid clicks'] = camp['Invalid clicks']
  campaign['Engagements'] = camp['Engagements']
  campaign['Views'] = camp['Views']
  campaign['CTR'] = camp['CTR']
  campaign['Impressions'] = camp['Impressions']
  campaign['Interactions'] = camp['Interactions']
  campaign['Clicks'] = camp['Clicks']
  campaign['Advertising Channel'] = camp['Advertising Channel']
  campaign['Bid Strategy Type'] = camp['Bid Strategy Type']
  campaign['Cost'] = camp['Cost']
  campaign['Campaign'] = camp['Campaign']
  campaign['Campaign ID'] = camp['Campaign ID']
  campaign['Account ID'] = camp['Account ID']
  campaign['Account Name'] = camp['Account Name']
  campaign['Dept'] = camp['Dept']
  campaign['Mapping'] = camp['Mapping']
  campaign['STATUS'] = camp['STATUS']
  return campaign

#================= Mapping campaign and plan all =====================
def MapAccountWithCampaignAll(path_folder, list_plan, list_campaign, date):
  # date_ = datetime.strptime(date, '%Y-%m-%d') 
  list_campaign_map = []
  number = 0
  # Remove line total in report
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)

  for i, eform in enumerate(list_plan):  
    flag = True
    if 'CAMPAIGN' not in eform:
      eform['CAMPAIGN'] = []
      eform['STATUS'] = None

    # -------------------- Choose time real ------------------------
    start, end = ChooseTime(eform)
    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')

    for j, camp in enumerate(list_campaign_map):
      camp['Advertising Channel'] = ChangeCampaignType(camp['Advertising Channel'])
      if 'Plan' not in camp:
        camp['Plan'] = None
        camp['STATUS'] = None
        # camp['Mapping'] == False

      date_ = datetime.strptime(camp['Date'], '%Y-%m-%d')

      if (camp['Mapping'] == False): 
        # Get product id in name campaign OMG3Q|278| 1710027 1709125  ===> 278
        try:
          product_id = (camp['Campaign'].split('|'))[1]
        except IndexError as e:
          product_id = ''
        # Check manual mapping
        map_ = False
        if (LogManualMap(path_folder, camp, eform, date, 1) == 1):
          map_ = True
        elif(  (eform['PRODUCT_CODE'] != [] or eform['CCD_PRODUCT'] != []) and \
          (
            checkProductCode(camp['Campaign'], eform['PRODUCT_CODE']) or \
            checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) or \
            product_id.find(str(eform['PRODUCT'])) >= 0
          )
          and \
          (camp['Campaign'].find(str(eform['REASON_CODE_ORACLE'])) >= 0) \
          and (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) 
          and (date_ >= start) 
          and (date_ <= end) ) \
          or ( LogManualMap(path_folder, camp, eform, date, 2) == 2):  
          map_ = True
          # if camp['Campaign ID'] == '699351990':
          #   print (camp)
          # print ("===================================== MP ====================================")
        if map_:
          camp['Mapping'] = True
          camp['STATUS'] = 'SYS'
          campaign = ConvertCampaignToJsonContent(camp)
          
          eform['CAMPAIGN'].append(campaign)
          number += 1

  list_un_campaign = []
  for camp in list_campaign_map:
    if camp['Mapping'] == False:
      camp['STATUS'] = ""
      list_un_campaign.append(ConvertCampaignToJsonContent(camp))

  data_map = {}
  data_map['UN_CAMP'] = list_un_campaign
  data_map['PLAN'] = list_plan

  # print (" -------------- Campaign ------ ", len(list_campaign_map))
  # print (" -------------- Mapping------ ", number)
  # print (" -------------- Un mapping------ ", len(list_un_campaign))
  # print ("\n")
  return data_map

#================= Mapping campaign and plan WPL =====================
def MapAccountWithCampaignWPL(path_folder, list_plan, list_campaign, date):
  list_campaign_map = []


  number = 0
  # Remove line total in report
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)

  for i, eform in enumerate(list_plan):  
    flag = True
    if 'CAMPAIGN' not in eform:
      eform['CAMPAIGN'] = []
      eform['STATUS'] = None
    # -------------------- Choose time real ------------------------
    start, end = ChooseTime(eform)
    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')

    for j, camp in enumerate(list_campaign_map):
      camp['Advertising Channel'] = ChangeCampaignType(camp['Advertising Channel'])
      if 'Plan' not in camp:
        camp['Plan'] = None
        camp['STATUS'] = None
      date_ = datetime.strptime(camp['Date'], '%Y-%m-%d')
      if (camp['Mapping'] == False and eform['DEPARTMENT_NAME'] == 'WPL'): 
        # Check log manual mapping
        map_ = False
        if (LogManualMap(path_folder, camp, eform, date, 1) == 1):
          map_ = True

        elif (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
          and (checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
          or checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) ) \
          and (camp['Advertising Channel'].find(str(eform['FORM_TYPE'])) >= 0) \
          and (date_ >= start) \
          and (date_ <= end) ) \
          or  ( LogManualMap(path_folder, camp, eform, date, 2) == 1): 
          map_ = True
        if map_:
          camp['Mapping'] = True
          camp['STATUS'] = 'SYS'
          campaign = ConvertCampaignToJsonContent(camp)
          
          eform['CAMPAIGN'].append(campaign)
          number += 1

  list_un_campaign = []
  for camp in list_campaign_map:
    if camp['Mapping'] == False:
      camp['STATUS'] = ""
      list_un_campaign.append(ConvertCampaignToJsonContent(camp))

  data_map = {}
  data_map['UN_CAMP'] = list_un_campaign
  data_map['PLAN'] = list_plan

  # print (" -------------- Campaign ------ ", len(list_campaign_map))
  # print (" -------------- Mapping------ ", number)
  # print (" -------------- Un mapping------ ", len(list_un_campaign))
  # print ("\n")
  return data_map


def GetCampaignTypeOfGS5(name_campaign):
  type_campaign = ''
  if name_campaign.find('CT-01') >= 0:
    type_campaign = 'SEARCH'
  if name_campaign.find('CT-02') >= 0:
    type_campaign = 'DISPLAY'
  if name_campaign.find('CT-03') >= 0:
    type_campaign = 'DISPLAY'
  if name_campaign.find('CT-04') >= 0:
    type_campaign = 'UNIVERSAL_APP_CAMPAIGN'
  if name_campaign.find('CT-05') >= 0:
    type_campaign = 'VIDEO'
  if name_campaign.find('CT-06') >= 0:
    type_campaign = 'DISPLAY'
  if name_campaign.find('CT-07') >= 0:
    type_campaign = 'VIDEO'
  if name_campaign.find('CT-08') >= 0:
    type_campaign = 'UNIVERSAL_APP_CAMPAIGN'
  if name_campaign.find('CT-09') >= 0:
    type_campaign = 'DISPLAY'
  if name_campaign.find('CT-10') >= 0:
    type_campaign = 'DISPLAY'
  return type_campaign

def GetUnitOptionOfGS5(name_account):
  unit_option = ''
  if (name_account.upper()).find('Appinstall'.upper()) >= 0:
    unit_option = 'CPI'
  else:
    unit_option = ''
  return unit_option
#================= Mapping campaign and plan GS5 =====================
def MapAccountWithCampaignGS5(path_folder, list_plan, list_campaign, date):
  list_campaign_map = []
  number = 0
  # Remove line total in report
  for j, camp in enumerate(list_campaign):
    if (camp['Cost'] > 0) and camp['Campaign state'] != 'Total':
      list_campaign_map.append(camp)



  for j, camp in enumerate(list_campaign_map):
    camp['Advertising Channel'] = ChangeCampaignType(camp['Advertising Channel'])
    # Get type campaign
    type_campaign = GetCampaignTypeOfGS5(camp['Campaign'])
    if 'Plan' not in camp:
      camp['Plan'] = None
      camp['STATUS'] = None
    date_ = datetime.strptime(camp['Date'], '%Y-%m-%d')
    for i, eform in enumerate(list_plan):  
      if 'CAMPAIGN' not in eform:
        eform['CAMPAIGN'] = []
        eform['STATUS'] = None
      # -------------------- Choose time real ------------------------
      start, end = ChooseTime(eform)
      start = datetime.strptime(start, '%Y-%m-%d')
      end = datetime.strptime(end, '%Y-%m-%d')

      if (camp['Mapping'] == False and eform['DEPARTMENT_NAME'] == 'GS5'): 
        map_ = False
        if (LogManualMap(path_folder, camp, eform, date, 1) == 1):
          map_ = True

        elif (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
          # and (checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
          and checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) \
          and (eform['FORM_TYPE'].find(type_campaign) >= 0) \
          and (date_ >= start) \
          and (date_ <= end) ) \
          or  ( LogManualMap(path_folder, camp, eform, date) == 1): 
          map_ = True
        if map_:
          camp['Mapping'] = True
          camp['STATUS'] = 'SYS'
          campaign = ConvertCampaignToJsonContent(camp)
          
          eform['CAMPAIGN'].append(campaign)
          number += 1

    if camp['Mapping'] == False:
      for i, eform in enumerate(list_plan):  
        if 'CAMPAIGN' not in eform:
          eform['CAMPAIGN'] = []
          eform['STATUS'] = None
        # -------------------- Choose time real ------------------------
        start, end = ChooseTime(eform)
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')

        unit_option = GetUnitOptionOfGS5(camp['Account Name'])
        if (eform['DEPARTMENT_NAME'] == 'GS5'): 

          if (  (eform['CCD_PRODUCT'] != [] or eform['PRODUCT_CODE'] != []) \
            # and (checkProductCode(camp['Account Name'], eform['CCD_PRODUCT']) \
            and checkProductCode(camp['Account Name'], eform['PRODUCT_CODE']) \
            and (eform['UNIT_OPTION'].find(unit_option) >= 0) \
            and (date_ >= start) \
            and (date_ <= end) ) \
            or  ( LogManualMap(path_folder, camp, eform, date) == 1 ): 

            camp['Mapping'] = True
            camp['STATUS'] = 'SYS'
            campaign = ConvertCampaignToJsonContent(camp)
            
            eform['CAMPAIGN'].append(campaign)
            number += 1

  list_un_campaign = []
  for camp in list_campaign_map:
    if camp['Mapping'] == False:
      camp['STATUS'] = ""
      list_un_campaign.append(ConvertCampaignToJsonContent(camp))

  data_map = {}
  data_map['UN_CAMP'] = list_un_campaign
  data_map['PLAN'] = list_plan

  # print (" -------------- Campaign ------ ", len(list_campaign_map))
  # print (" -------------- Mapping------ ", number)
  # print (" -------------- Un mapping------ ", len(list_un_campaign))
  # print ("\n")
  return data_map


# ========================= PRODUCT ALIAS =============================
def ReadProductAlias(connect, path_data, date):
  path_folder = os.path.join(path_data, str(date) + '/PLAN')
  if not os.path.exists(path_folder):
    os.makedirs(path_folder)
  file_product = os.path.join(path_data, str(date) + '/PLAN/product_alias.json')
  # ==================== Connect database =======================
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()
  statement = 'select PRODUCT_ID, GG_PRODUCT, CCD_PRODUCT, APPSFLYER_PRODUCT from ODS_META_PRODUCT'        
  cursor.execute(statement)
  res = list(cursor.fetchall())
  list_json = []
  for product in res:
    if product[0] is not None:
      json_ = {
        'PRODUCT_ID': product[0],
        'GG_PRODUCT': product[1],
        'CCD_PRODUCT' : product[2],
        'APPSFLYER_PRODUCT' : product[3]
      }
      list_json.append(json_)
  data_json = {}
  data_json['ALIAS'] = list_json
  with open(file_product, 'w') as fo:
    json.dump(data_json, fo)
  cursor.close()

  return data_json

def AddProductCode(path_folder, list_plan, date):
  #================ Add product id to plan =================
  file_product = os.path.join(path_folder, str(date) + '/PLAN/product_alias.json')
  with open(file_product, 'r') as fo:
    data = json.load(fo)

  list_temp = []
  for plan in list_plan:
    temp = plan
    temp['PRODUCT_CODE'] = []
    for alias in data['ALIAS']:
      if (alias['PRODUCT_ID'] is not None) and (alias['GG_PRODUCT'] is not None) \
      and (int(plan['PRODUCT']) == int(alias['PRODUCT_ID'])):
        if str(alias['GG_PRODUCT']) not in temp['PRODUCT_CODE']:
          temp['PRODUCT_CODE'].append(str(alias['GG_PRODUCT']))     

    temp['CCD_PRODUCT'] = []
    for alias in data['ALIAS']:
      if (alias['PRODUCT_ID'] is not None) and (alias['CCD_PRODUCT'] is not None) \
      and (int(plan['PRODUCT']) == int(alias['PRODUCT_ID'])):
        if str(alias['CCD_PRODUCT']) not in temp['CCD_PRODUCT']:
          temp['CCD_PRODUCT'].append(str(alias['CCD_PRODUCT']))  

    temp['APPSFLYER_PRODUCT'] = []
    for alias in data['ALIAS']:
      if (alias['PRODUCT_ID'] is not None) and (alias['APPSFLYER_PRODUCT'] is not None) \
      and (int(plan['PRODUCT']) == int(alias['PRODUCT_ID'])):
        if str(alias['APPSFLYER_PRODUCT']) not in temp['APPSFLYER_PRODUCT']:
          temp['APPSFLYER_PRODUCT'].append(str(alias['APPSFLYER_PRODUCT'])) 

    list_temp.append(temp)
  list_plan = list_temp
  return list_plan


# ======================== PLAN ========================================
def ReadPlanFromTable(connect, path_folder, date):
  import datetime
  folder = os.path.join(path_folder, str(date) + '/PLAN')
  if not os.path.exists(folder):
    os.makedirs(folder)
  # print (folder)
  file_plan = os.path.join(folder, 'plan.json')
  # print (file_plan)

  #============================== Connect database =============================
  # conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()

  #======================= Get data from database ==============================
  query = 'select CYEAR, CMONTH, LEGAL, DEPARTMENT, DEPARTMENT_NAME, PRODUCT, REASON_CODE_ORACLE, EFORM_NO, \
          START_DAY, END_DAY_ESTIMATE, CHANNEL, EFORM_TYPE, UNIT_OPTION, UNIT_COST, AMOUNT_USD, CVALUE, \
          ENGAGEMENT, IMPRESSIONS, CLIKE, CVIEWS, INSTALL, NRU, INSERT_DATE, REAL_START_DATE, REAL_END_DATE \
      from STG_FA_DATA_GG'

  cursor.execute(query)
  row = cursor.fetchall()
  temp = list(row)
  cursor.close()

  #===================== Convert data into json =================================

  list_key = ['CYEAR', 'CMONTH', 'LEGAL', 'DEPARTMENT', 'DEPARTMENT_NAME', 'PRODUCT', 
        'REASON_CODE_ORACLE', 'EFORM_NO', 'START_DAY', 'END_DAY_ESTIMATE', 'CHANNEL', 
        'FORM_TYPE', 'UNIT_OPTION', 'UNIT_COST', 'AMOUNT_USD', 'CVALUE', 'ENGAGEMENT', 
        'IMPRESSIONS', 'CLIKE', 'CVIEWS', 'INSTALL', 'NRU', 'INSERT_DATE', 'REAL_START_DATE', 'REAL_END_DATE']

  list_json= []
  for plan in temp: 
    list_temp = []
    unmap = {}
    for value in plan:
      val = value   
      if isinstance(value, datetime.datetime):            
        val = value.strftime('%Y-%m-%d')
      list_temp.append(val)
    for i in range(len(list_key)):
      unmap[list_key[i]] = list_temp[i]
    list_json.append(unmap)
  plan_ = {}
  plan_['plan'] = list_json

  plan_['plan'] = AddProductCode(path_folder, plan_['plan'], date)
  
  with open (file_plan, 'w') as f:
    json.dump(plan_, f)
  return plan_

def ReadPlan(path_folder, date):
  # =============== List plan code ================  
  file_plan = os.path.join(path_folder, str(date) +  '/PLAN/plan.json')
  list_plan = {}
  with open (file_plan, 'r') as f:
    list_plan = json.load(f)
    return list_plan


# ============ Check account WPL or GS5 
def CheckIsAccountWPL(list_account, account_id):
  for account in list_account:
    if str(account_id) == str(account['customerId']) and (account['dept Name'].find('WPL') >= 0):
      return True
  return False

def CheckIsAccountGS5(list_account, account_id):
  for account in list_account:
    if account['dept Name'] != None:
      if str(account_id) == str(account['customerId']) and (account['dept Name'].find('GS5') >= 0):
        return True
  return False

#================= Read list plan, product code, save file mapping =====================
def MapData(customer, path_folder, list_account_wpl, list_account_gs5, date): 

  # =============== List plan code ================
  list_plan = ReadPlan(path_folder, date)

  #================ Add product id to plan =================
  # list_plan = AddProductCode(path_folder, list_plan, date)


  # # #=========== Map Account with Campaign =======================  
  path = os.path.join(path_folder, str(date) + '/ACCOUNT_ID/' + customer)
  file_campaign = os.path.join(path, 'campaign_' + str(date) + '.json')
  if os.path.exists(file_campaign):
    # Neu list campaign lay duoc ve khac rong
    with open (file_campaign, 'r') as f:
      list_campaign = json.load(f)
    # print (len(list_campaign))
    if len(list_campaign) > 0:
      # data_map = []
      # print (customer)
      # ------------- Check account ----------------
      if CheckIsAccountWPL(list_account_wpl, customer):
        # print ("================ WPL ======================")
        data_map = MapAccountWithCampaignWPL(path_folder, list_plan['plan'], list_campaign, date)
      else:
        if CheckIsAccountGS5(list_account_gs5, customer): # La account WPL
          # print ("================ GS5 ======================")
          data_map = MapAccountWithCampaignGS5(path_folder, list_plan['plan'], list_campaign, date)
        else:
          # print ("================ ALL ======================")
          data_map = MapAccountWithCampaignAll(path_folder, list_plan['plan'], list_campaign, date)

      #----------------- Write file map and unmap ------------------
      path_data_map = os.path.join(path, 'mapping_' + str(date) + '.json')
      # print (path_data_map)
      with open (path_data_map,'w') as f:
        json.dump(data_map, f)


def ReadListAccountGS5AndWPL(path_folder):
  list_account_wpl = []
  list_account_gs5 = []

  file_ = path_folder[:path_folder.rfind('/')] + '/' + 'LIST_ACCOUNT/WPL.json'
  with open (file_, 'r') as f:
    list_account_wpl = json.load(f)

  file_ = path_folder[:path_folder.rfind('/')] + '/' + 'LIST_ACCOUNT/MCC.json'
  with open (file_, 'r') as f:
    list_account_gs5 = json.load(f)

  return (list_account_wpl, list_account_gs5)

def MapDataForAllAccount(connect, list_customer, path_data, date):
  #========== Create list plan
  # Doc file product alias.
  import time
  mapping = time.time()

  ReadProductAlias(connect, path_data, date)
  list_plan = ReadPlanFromTable(connect, path_data, date)

  time_mapping = time.time() - mapping
  # print ("             Time maping: ", time_mapping)

  # Read list account WPL and GS5
  list_account_wpl, list_account_gs5 = ReadListAccountGS5AndWPL(path_data)


  for customer in list_customer:
    path_customer = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + customer)
    if os.path.exists(path_customer):
      MapData(customer, path_data, list_account_wpl, list_account_gs5, date)
