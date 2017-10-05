import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date




connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'

conn = cx_Oracle.connect(connect)
cursor = conn.cursor()
date = '2017-06-01'

#==================== Get NRU =============================
day = date[8:]
month = date[5:-3]
year = date[:4]
date = month + '-' + day + '-' + year
statement = "Select SNAPSHOT_DATE, PRODUCT_CODE, NRU from STG_NRU where CHANNEL = 'Google' and SNAPSHOT_DATE = = to_date('" + date + "', 'mm/dd/yyyy')"
cursor.execute(statement)
list_NRU = list(cursor.fetchall()) 
cursor.close()
print(len(list_NRU))