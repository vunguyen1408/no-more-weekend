import os
import json
import cx_Oracle
from datetime import datetime , timedelta, date





def ReadNRU(connect):  
  # ==================== Connect database =======================
  conn = cx_Oracle.connect(connect)
  cursor = conn.cursor()

  # ==================== List plan ==========================
  # file_plan = os.path.join(path_folder, str(date) +  '/PLAN/plan.json')


  #==================== Get NRU =============================
  statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google'"
  cursor.execute(statement)
  list_NRU = list(cursor.fetchall())  
  
  #==================== Get prodcut ID ===================
  statement = 'Select PRODUCT_ID, CCD_PRODUCT from ODS_META_PRODUCT'
  cursor.execute(statement)
  list_product = list(cursor.fetchall())  

  for nru in list_NRU:
    for pro in list_product:
      if (nru[1] == pro[1]):
        nru.append(pro[0])
      else:
        nru.append('')

  for nru in list_NRU:
    print(nru)

  cursor.close()


connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
ReadNRU(connect)