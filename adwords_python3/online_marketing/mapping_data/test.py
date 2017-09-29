
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

def loadProductAlias(path_data, connect):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()
	statement = 'select PRODUCT_ID, GG_PRODUCT, CCD_PRODUCT from ODS_META_PRODUCT'        
	cursor.execute(statement)
	res = list(cursor.fetchall())
	list_json = []
	for product in res:
		json_ = {
			'PRODUCT_ID': product[0],
			'GG_PRODUCT': product[1],
			'CCD_PRODUCT' : product[2]
		}
		list_json.append(json_)
	print (list_json)

	# with open(path_data, 'w') as fo:
	#     json.dump(list_json, fo)
	# cursor.close()