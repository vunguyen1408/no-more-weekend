import sys
import os
import numpy as np
import json
from datetime import datetime , timedelta, date


#================= Merger data to folder MAPPING =====================
def MergerDataAccount(path_data, customer_id, date):
  #========= List acc folder ===============
  # print (path_data)
  path_customer = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' +  customer_id)
  path_folder = path_customer

  #------------------- Open json ma and un map on date ------------------------------
  path_data_map_date = os.path.join(path_folder, 'mapping_' + str(date) + '.json')
  if os.path.exists(path_data_map_date):
    with open (path_data_map_date,'r') as f:
      data_map_date = json.load(f)

    #------------------- Open json ma and un map on date ------------------------------
    path_folder = path_data + '/' + str(date) + '/DATA_MAPPING'
    # print (path_folder)
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

    with open (path_data_map,'w') as f:
      json.dump(data_map, f)


def Merge(path_data, list_customer_id, date):
  for account in list_customer_id:
    path_customer = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + account)
    if os.path.exists(path_customer):
      MergerDataAccount(path_data, account, date)


def MergeWithDate(customer_id, path_data, start_date, end_date):
  startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
  endDate = datetime.strptime(end_date, '%Y-%m-%d').date()   

  date = startDate
  list_campaign_unmapping = []
  list_eform_unmapping = []

  while(date <= endDate):
    print ("===========================================================")
    print (date)
    MergerDataAccount(path_data, customer_id, str(date))
    date = date + timedelta(1)


# startDate = '2017-06-01'
# endDate = '2017-06-01'
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/END'
# list_customer_id = ['5008396449', '9021114325', '9420329501']
# for customer_id in list_customer_id:
#   MergeWithDate(customer_id, path_data, startDate, endDate)





    