import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date

def ReadNRU(connect, path_data, date):  
  file_nru = os.path.join(path_data, str(date) + '/PLAN/nru.json')
  # ==================== Connect database =======================
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()

  #==================== Get NRU =============================
  day = date[8:]
  month = date[5:-3]
  year = date[:4]
  date = month + '-' + day + '-' + year
  statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' \
  and SNAPSHOT_DATE = to_date('" + date + "', 'mm/dd/yyyy')"
  cursor.execute(statement)
  list_NRU = list(cursor.fetchall()) 
  
  #==================== Get product ID ===================
  statement = 'Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT'
  cursor.execute(statement)
  list_product = list(cursor.fetchall())

  list_json = []
  for i in range(len(list_NRU)):
    list_NRU[i] = list(list_NRU[i])
    list_NRU[i].append(None)
    for pro in list_product:
      if (list_NRU[i][1] == pro[1]):    
        list_NRU[i][3] = pro[0]    
  
    json_ = {
      'SNAPSHOT_DATE': list_NRU[i][0].strftime('%Y-%m-%d'),
      'PRODUCT_CODE': list_NRU[i][1],
      'NRU': list_NRU[i][2],
      'PRODUCT_ID': list_NRU[i][3]
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
      date = datetime.strptime(nru['SNAPSHOT_DATE'], '%Y-%m-%d')
      if (nru['PRODUCT_ID'] is not None) \
      and (int(plan['PRODUCT']) == int(nru['PRODUCT_ID'])) \
      and (date >= datetime.strptime(plan['START_DAY'], '%Y-%m-%d')) \
      and (date <= datetime.strptime(plan['END_DAY_ESTIMATE'], '%Y-%m-%d')):
        plan['NRU'] = nru['NRU']    
  
  return list_plan