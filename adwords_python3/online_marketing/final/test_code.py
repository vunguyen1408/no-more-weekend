import json
import codecs
import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date
import time


def Insert(name, cursor):
	#==================== Insert data into database =============================
	statement = 'insert into DTM_GG_RUN_FLAG (FLAG_RUNNING, FINAL_RUNTIME) \
	values (:1, :2) '
		
	cursor.execute(statement, (name, None))
	
	# print("A row inserted!.......")
	conn.commit()
	# print("Committed!.......")



connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
conn = cx_Oracle.connect(connect)
cursor = conn.cursor()
path = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/output_file.json'

# data = json.load(codecs.open(path, 'r', 'utf-8-sig'))
# data = json.loads(open(path).read().decode('utf-8-sig'))
# with open(path, 'r') as fi:
# 	data = json.load(fi)
# path = 'D:/WorkSpace/GG_Tool/Finally/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/output_file.json'
input_file  = codecs.open(path, "r", encoding="utf-8")
data = json.loads(input_file.read())
for acc in data:
	if (str(acc["customerId"]) == '4476024314'):
		# Insert(acc["name"].encode('utf-8'), cursor)
		print(acc["name"])
		# print(acc["name"].encode('utf-8'))
		sys.stdout = codecs.getwriter("iso-8859-1")(sys.stdout, 'xmlcharrefreplace')	
		Insert(acc["name"].encode('utf-8'), cursor)	
		Insert(acc["name"], cursor)
		# Insert(acc["name"].encode('utf-8-sig'), cursor)
		# Insert(acc["name"], cursor)
		# Insert(acc["name"].encode('iso-8859-1'), cursor)







# import json
# import sys
# import codecs
# import os
# path = 'D:/WorkSpace/GG_Tool/Finally/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/TEST_UNICODE.json'
# input_file  = open(path, "r")
# data = json.loads(input_file.read())
# output_file = codecs.open("D:/WorkSpace/GG_Tool/Finally/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/output_file.json", "w", encoding="utf-8")

# sys.stdout = codecs.getwriter("iso-8859-1")(sys.stdout, 'xmlcharrefreplace')
   
# json.dump(data, output_file, indent=4, sort_keys=True, ensure_ascii=False)

# print("Save ok............")
# input_file  = open(path, "r")
# data = json.loads(input_file.read())
# # for acc in data:
    # if (str(acc["customerId"]) == '4476024314'):
     # print( acc["name"])
# print("begin save")