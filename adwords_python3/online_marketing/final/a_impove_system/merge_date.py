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
      data_map['UN_CAMP'] = []
      data_map['PLAN'] = []
      with open (path_data_map,'w') as f:
        json.dump(data_map, f)
    #-----------------------------------------------------------

    with open (path_data_map,'r') as f:
      data_map = json.load(f)
      
    if data_map_date != []:
      #--------------------- UN MAP CAMPAIGN ---------------------
      temp_date = data_map_date['UN_CAMP']
      temp = data_map['UN_CAMP']
      temp.extend(temp_date)
      data_map['UN_CAMP'] = temp

      #------------------- DATA UN MAP PLAN -------------------
      if len(data_map['PLAN']) == 0:
        data_map['PLAN'] = list(data_map_date['PLAN'])
      else:
        for plan_date in data_map_date['PLAN']:
          flag = False
          for plan in data_map['PLAN']:
            if plan['PRODUCT_CODE'] == plan_date['PRODUCT_CODE'] \
              and plan['REASON_CODE_ORACLE'] == plan_date['REASON_CODE_ORACLE'] \
              and plan['FORM_TYPE'] == plan_date['FORM_TYPE'] \
              and plan['START_DAY'] == plan_date['START_DAY'] \
              and plan['END_DAY_ESTIMATE'] == plan_date['END_DAY_ESTIMATE']:

              # Cap nhat real date
              plan['REAL_START_DATE'] = plan_date['REAL_START_DATE']
              plan['REAL_END_DATE'] = plan_date['REAL_END_DATE']

              # Chuyen campaign maping duoc cua plan
              temp_date = plan_date['CAMPAIGN']
              temp = plan['CAMPAIGN']
              temp.extend(temp_date)
              plan['CAMPAIGN'] = temp
              flag = True

          # Plan moi
          if flag == False:
            data_map['PLAN'].append(plan_date)
    with open (path_data_map,'w') as f:
      json.dump(data_map, f)


def MergeDataMapping(path_data, list_customer_id, date):
  for account in list_customer_id:
    path_customer = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + account)
    if os.path.exists(path_customer):
      MergerDataAccount(path_data, account, date)






    