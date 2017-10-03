
import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'

statement = "insert into CAMPAIGN_MAP_HIS_GG (PRODUCT, REASON_CODE_ORACLE, \
				EFORM_TYPE, UNIT_OPTION, \
				USER_NAME, USER_CAMPAIGN_ID, \
				UPDATE_DATE, START_DATE, END_DATE) values(:1, :2, :3, :4, :5, :6, :7, :8, :9)"

def loadProductAlias(path_data, connect):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()
	statement = "insert into CAMPAIGN_MAP_HIS_GG (PRODUCT, REASON_CODE_ORACLE, \
				EFORM_TYPE, UNIT_OPTION, \
				USER_NAME, USER_CAMPAIGN_ID, \
				UPDATE_DATE, START_DATE, END_DATE) values(:1, :2, :3, :4, :5, :6, :7, :8, :9)"

	cursor.execute(statement, (None, 'một â', None, None, None, None, None, None, None))
	conn.commit()
	print("Committed!.......")
	cursor.close()

loadProductAlias(connect)