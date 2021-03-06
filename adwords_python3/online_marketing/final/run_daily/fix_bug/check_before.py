import sys
import os
import pandas as pd
import numpy as np
import json
import cx_Oracle
from datetime import datetime , timedelta, date
import time


path_un = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA_GG/2017-10-31/DATA_MAPPING/un_map_camp.json'
path_total = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA_GG/2017-10-31/DATA_MAPPING/total_mapping.json'

import insert_data_map_to_total as insert_to_total

with open(path_un, 'r') as fi:
	data_un = json.load(fi)


month = [	{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-03', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-04', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-05', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-06', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-07', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-08', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-09', '%Y-%m').date()
		},
		{
			'number': 0,
			'wpl' : 0,
			'gs5' : 0,
			'month': datetime.strptime('2017-10', '%Y-%m').date()
		}
]


size_un = len(data_un)
print ("So luong camp un: ", size_un)
wpl = 0
gs5 = 0
for camp in data_un:
	if int(camp['Date'][5:-3]) != 10:
		date_ = datetime.strptime(camp['Date'][:-3], '%Y-%m').date()
		for m in month:
			if date_ == m['month']:
				m['number'] += 1
				if camp['Dept'] == 'WPL':
					m['wpl'] += 1
					print (camp)
				if camp['Dept'] == 'GS5':
					m['gs5'] += 1
					print (camp)

for m in month:
	print (m['month'].strftime('%Y-%m-%d') + '  ' + str(m['number']))
	print (m['month'].strftime('%Y-%m-%d') + '  wpl  ' + str(m['wpl']))
	print (m['month'].strftime('%Y-%m-%d') + '  gs5  ' + str(m['gs5']))

print (wpl)
print (gs5)

with open(path_total, 'r') as fi:
	data_total = json.load(fi)

# for plan in data_total:
# 	if str(plan['REASON_CODE_ORACLE']) == '1708008':
# 		print (plan)

# for plan_total in data_alias['ALIAS']:
# 	if  str(plan_total['PRODUCT_ID']) == '193':
# 		print (plan_total)