import sys
import os
import numpy as np
import json
from datetime import datetime , timedelta, date


#================= Merger data to folder MAPPING =====================
def MergerDataAccount(path_data, data_map, customer_id, date):
  #========= List acc folder ===============
  # print (path_data)
  path_folder = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' +  customer_id)


  #------------------- Open json ma and un map on date ------------------------------
  path_data_map_date = os.path.join(path_folder, 'mapping_' + str(date) + '.json')
  if os.path.exists(path_data_map_date):
    with open (path_data_map_date,'r') as f:
      data_map_date = json.load(f)

    #------------------- Open json ma and un map on date ------------------------------
    # path_folder = path_data + '/' + str(date) + '/DATA_MAPPING'
    # print (path_folder)
    
      
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
          # if str(plan_date['REASON_CODE_ORACLE']) == '1708007' and len(plan_date['CAMPAIGN']) > 0:
          #   print (plan_date)

          flag = False
          for plan in data_map['PLAN']:
            if str(plan['PRODUCT_CODE']) == str(plan_date['PRODUCT_CODE']) \
              and str(plan['REASON_CODE_ORACLE']) == str(plan_date['REASON_CODE_ORACLE']) \
              and str(plan['FORM_TYPE']) == str(plan_date['FORM_TYPE']) \
              and str(plan['START_DAY']) == str(plan_date['START_DAY']) \
              and str(plan['END_DAY_ESTIMATE']) == str(plan_date['END_DAY_ESTIMATE']):

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
  return data_map



def MergeDataMapping(path_data, list_customer_id, date):

  path_folder = path_data + '/' + str(date) + '/DATA_MAPPING'
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

  for account in list_customer_id:
    path_customer = os.path.join(path_data, str(date) + '/ACCOUNT_ID/' + account)
    if os.path.exists(path_customer):
      print (account)
      data_map = MergerDataAccount(path_data, data_map, account, date)

  # for plan_total in data_map['PLAN']:
  #   # print (plan_total)
  # # if plan_total['REASON_CODE_ORACLE'] == '1708061':
  # # # if str(plan_total['Campaign ID']) == '772872164':
  # #   print (plan_total)
  #   # plan_total = insert_to_total.CaculatorTotalMonth(data_total['MAP'], plan_total, date)
  #   if str(plan_total['REASON_CODE_ORACLE']) == '1708007':
  #     # for camp in plan_total['CAMPAIGN']:
  #     #   print (camp)
  #     print (plan_total)
  #   if plan_total['REASON_CODE_ORACLE'] == '1708007':
  #     print ("================  ======================")
  #   #----------------- Write file map and unmap ------------------

  # # with open (path_data_map,'w') as f:
  # #   json.dump(data_map, f)






    