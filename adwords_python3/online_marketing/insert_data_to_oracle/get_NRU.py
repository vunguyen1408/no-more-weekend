import os
import json
import cx_Oracle
from datetime import datetime , timedelta, date





def ReadNRU(connect, path_data, date):  
  file_nru = os.path.join(path_data, str(date) + '/PLAN/nru.json')
  # ==================== Connect database =======================
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()

  #==================== Get NRU =============================
  statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google'"
  cursor.execute(statement)
  list_NRU = list(cursor.fetchall())  
  
  #==================== Get product ID ===================
  statement = 'Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT'
  cursor.execute(statement)
  list_product = list(cursor.fetchall())

  for i in range(len(list_NRU)):
    list_NRU[i] = list(list_NRU[i])
    list_NRU[i].append(None)
    for pro in list_product:
      if (list_NRU[i][1] == pro[1]):    
        list_NRU[i][3] = pro[0]  
      
  list_json = []
  for nru in list_NRU:
    json_ = {
    'SNAPSHOT_DATE' = nru[0],
    'PRODUCT_CODE' = nru[1],
    'NRU' = nru[2],
    'PRODUCT_ID' = nru[3]
    }
    list_json.append(json_)

  data_json = {}
  data_json['NRU'] = list_json
  with open(file_nru, 'w') as fo:
    json.dump(data_json, fo)
  cursor.close()


def AddNRU(path_folder, list_plan, date):
  #================ Add product id to plan =================
  file_product = os.path.join(path_folder, str(date) + '/PLAN/nru.json')
  with open(file_product, 'r') as fi:
    data = json.load(fi)

  list_temp = []
  for plan in list_plan['plan']:    
    for nru in data['NRU']:
      if (nru['PRODUCT_ID'] is not None) \
      and (int(plan['PRODUCT']) == int(nru['PRODUCT_ID'])) \
      and (nru['SNAPSHOT_DATE'] >= datetime.strptime(plan['START_DAY'], '%Y-%m-%d')) \
      and (nru['SNAPSHOT_DATE'] <= datetime.strptime(plan['START_DAY'], '%Y-%m-%d')):
        plan['NRU'] = nru['NRU']    
  
  return list_plan


connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/insert_data_to_oracle'
date = '2017-06-01'
ReadNRU(connect, path_data, date)
file_plan = os.path.join(path_data, 'plan.json')
with open(file_plan, 'r') as fi:
    data = json.load(fi)
list_plan = data['plan']
plan = AddNRU(path_data, list_plan, date)
for p in plan:
  print(p)