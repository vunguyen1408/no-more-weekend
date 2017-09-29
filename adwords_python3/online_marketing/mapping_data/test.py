
import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'

statement = "select * from DTM_GG_PIVOT_DETAIL"

# ==================== Connect database =======================
conn = cx_Oracle.connect(connect)
cursor = conn.cursor()
cursor.execute(statement)
log_manual = cursor.fetchall()
print (log_manual)

# path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/mapping_data'
# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# ReadTableManualMap(connect, path_data)