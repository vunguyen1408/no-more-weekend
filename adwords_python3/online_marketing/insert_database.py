import cx_Oracle
import json
from datetime import datetime , timedelta, date

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
	

def InsertDataDate(path_data, connect):

	#==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	#==================== Get data from database =================
	statement = '''INSERT INTO DTM_GG_PIVOT_DETAIL (
	-- SNAPSHOT_DATE, \ 				
	CYEAR \						
	-- CMONTH, \ 						
	-- LEGAL, \						
	-- DEPARTMENT, \					
	-- DEPARTMENT_NAME, \				
	-- PRODUCT \						
	-- -- PRODUCT_NAME, \					
	-- REASON_CODE_ORACLE, \			
	-- EFORM_NO, \						
	-- START_DATE, \					
	-- END_DATE, \      				
	-- CHANNEL, \						
	-- UNIT_COST, \					
	-- AMOUNT_USD, \					
	-- CVALUE, \						
	-- ENGAGEMENT, \					
	-- IMPRESSIONS, \					
	-- -- REACH, \						
	-- -- FREQUENCY, \					
	-- CLIKE, \        				
	-- -- CLICKS_ALL, \					
	-- -- LINK_CLICKS, \					
	-- CVIEWS, \						
	-- -- C3S_VIDEO_VIEW, \				
	-- INSTALL, \						
	-- NRU, \							
	-- EFORM_TYPE, \					
	-- UNIT_OPTION, \					
	-- -- OBJECTIVE, \					
	-- -- EVENT_ID, \						
	-- -- PRODUCT_ID, \					
	-- -- CCD_NRU, \						
	-- -- GG_VIEWS, \						
	-- GG_CONVERSION, \				
	-- GG_INVALID_CLICKS, \			
	-- GG_ENGAGEMENTS, \       		
	-- -- GG_VIDEO_VIEW, \				
	-- GG_CTR, \						
	-- GG_IMPRESSIONS, \				
	-- GG_INTERACTIONS, \				
	-- GG_CLICKS, \					
	-- -- GG_INTERACTION_TYPE, \			
	-- GG_COST, \   					
	-- GG_SPEND \						
	-- -- GG_APPSFLYER_INSTALL, \			
	-- -- GG_STRATEGY_BID_TYPE) \
	) VALUES (:2) '''
	# , :3, :4, :5, :6, :7
	# , :9, :10, :11, :12, :13, :14, :15, \
	# :16, :17, :18, :21, :24, :26, :27, :28, :29, :35, \
	# :36, :37, :39, :40, :41, :42, :44, :45)'''

	with open(path_data, 'r') as fi:
		data = json.load(fi)


	for value in data['monthly']:
		cursor.execute(statement, (value['CYEAR']))
		# , value['CMONTH'], value['LEGAL'], 
		# 	value['DEPARTMENT'], value['DEPARTMENT_NAME'], value['PRODUCT']))
			# , 
			# value['REASON_CODE_ORACLE'], value['EFORM_NO'], datetime.strptime(value['START_DAY'], '%Y-%m-%d'), 
			# datetime.strptime(value['END_DAY_ESTIMATE'], '%Y-%m-%d'), value['CHANNEL'], value['UNIT_COST'], 
			# float(value['AMOUNT_USD']), float(value['CVALUE']), float(value['ENGAGEMENT']), 
			# float(value['IMPRESSIONS']), float(value['CLIKE']),
			# float(value['CVIEWS']), float(value['INSTALL']), 
			# float(value['NRU']), value['FORM_TYPE'], value['UNIT_OPTION'], 
			# float(value['DATA_MONTHLY']['CONVERSIONS']), float(value['DATA_MONTHLY']['INVALID_CLICKS']), 
			# float(value['DATA_MONTHLY']['ENGAGEMENTS']), float(value['DATA_MONTHLY']['CTR']), 
			# float(value['DATA_MONTHLY']['IMPRESSIONS']), float(value['DATA_MONTHLY']['INTERACTIONS']), 
			# float(value['DATA_MONTHLY']['CLICKS']), 
			# float(value['DATA_MONTHLY']['COST']), float(value['DATA_MONTHLY']['COST']) ))


	conn.commit()
	cursor.close()
	print("ok")
	


# path_data = 'C:/Users/CPU10912-local/Desktop/monthly.json'
path_data = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/monthly.json'
InsertDataDate(path_data, connect)

	# statement = """INSERT INTO DTM_GG_PIVOT_DETAIL (
	# SNAPSHOT_DATE, \ 				#1
	# CYEAR, \						#2
	# CMONTH, \ 					#3
	# LEGAL, \						#4
	# DEPARTMENT, \					#5
	# DEPARTMENT_NAME, \			#6
	# PRODUCT, \					#7
	# PRODUCT_NAME, \				#8
	# REASON_CODE_ORACLE, \			#9
	# EFORM_NO, \					#10
	# START_DATE, \					#11
	# END_DATE, \      				#12
	# CHANNEL, \					#13
	# UNIT_COST, \					#14
	# AMOUNT_USD, \					#15
	# CVALUE, \						#16
	# ENGAGEMENT, \					#17
	# IMPRESSIONS, \				#18
	# REACH, \						#19
	# FREQUENCY, \					#20
	# CLIKE, \        				#21
	# CLICKS_ALL, \					#22
	# LINK_CLICKS, \				#23
	# CVIEWS, \						#24
	# C3S_VIDEO_VIEW, \				#25
	# INSTALL, \					#26
	# NRU, \						#27
	# EFORM_TYPE, \					#28
	# UNIT_OPTION, \				#29
	# OBJECTIVE, \					#30
	# EVENT_ID, \					#31
	# PRODUCT_ID, \					#32
	# CCD_NRU, \					#33
	# GG_VIEWS, \					#34
	# GG_CONVERSION, \				#35
	# GG_INVALID_CLICKS, \			#36
	# GG_ENGAGEMENTS, \       		#37
	# GG_VIDEO_VIEW, \				#38
	# GG_CTR, \						#39
	# GG_IMPRESSIONS, \				#40
	# GG_INTERACTIONS, \			#41
	# GG_CLICKS, \					#42
	# GG_INTERACTION_TYPE, \		#43
	# GG_COST, \   					#44
	# GG_SPEND, \					#45
	# GG_APPSFLYER_INSTALL, \		#46
	# GG_STRATEGY_BID_TYPE) \		#47
	# VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 19, :20, \
	# :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, \
	# :41, :42, :43, :44, :45, :46, :47)"""

	# with open(path_data, 'r') as fi:
	# 	data = json.load(fi)

	# for value in data['monthly']:	
	# 	cursor.execute(statement, ('', value['CYEAR'], value['CMONTH'], value['LEGAL'], \
	# 		value['DEPARTMENT'], value['DEPARTMENT_NAME'], value['PRODUCT'], '', \
	# 		value['REASON_CODE_ORACLE'], value['EFORM_NO'], value['START_DAY'], \
	# 		value['END_DAY_ESTIMATE'], value['CHANNEL'], value['UNIT_COST'], \
	# 		float(value['AMOUNT_USD']), float(value['CVALUE']), float(value['ENGAGEMENT']), float(value['IMPRESSIONS']),\
	# 		0, 0, float(value['CLIKE']), 0, \
	# 		0, float(value['CVIEWS']), 0, float(value['INSTALL']), \
	# 		float(value['NRU']), value['FORM_TYPE'], value['UNIT_OPTION'], \
	# 		'', '', '', 0, \
	# 		0, float(value['DATA_MONTHLY']['CONVERSIONS']), float(value['DATA_MONTHLY']['INVALID_CLICKS']), \
	# 		float(value['DATA_MONTHLY']['ENGAGEMENTS']), 0, float(value['DATA_MONTHLY']['CTR']), \
	# 		float(value['DATA_MONTHLY']['IMPRESSIONS']), float(value['DATA_MONTHLY']['INTERACTIONS']), float(value['DATA_MONTHLY']['CLICKS']), \
	# 		'', float(value['DATA_MONTHLY']['COST']), float(value['DATA_MONTHLY']['COST']), 0, ''))







# cursor.execute(statement, (value['SNAPSHOT_DATE'], value['CYEAR'], value['CMONTH'], value['LEGAL'], \
# 		value['DEPARTMENT'], value['DEPARTMENT_NAME'], value['PRODUCT'], value['PRODUCT_NAME'], \
# 		value['REASON_CODE_ORACLE'], value['EFORM_NO'], datetime.strptime(value['START_DATE'], '%Y-%m-%d'), \
# 		datetime.strptime(value['END_DATE'], '%Y-%m-%d'), value['CHANNEL'], value['UNIT_COST'], \
# 		float(value['AMOUNT_USD']), float(value['CVALUE']), float(value['ENGAGEMENT']), float(value['IMPRESSIONS']),\
# 		float(value['REACH']), float(value['FREQUENCY']), float(value['CLIKE']), float(value['CLICKS_ALL']), \
# 		float(value['LINK_CLICKS']), float(value['CVIEWS']), float(value['C3S_VIDEO_VIEW']), float(value['INSTALL']), \
# 		float(value['NRU']), value['EFORM_TYPE'], value['UNIT_OPTION'], \
# 		value['OBJECTIVE'], value['EVENT_ID'], value['PRODUCT_ID'], value['CCD_NRU'],\
# 		float(value['GG_VIEWS']), float(value['GG_CONVERSION']), float(value['GG_INVALID_CLICKS']), \
# 		float(value['GG_ENGAGEMENTS']), float(value['GG_VIDEO_VIEW']), float(value['GG_CTR']), \
# 		float(value['GG_IMPRESSIONS']), float(value['GG_INTERACTIONS']), float(value['GG_CLICKS']), \
# 		value['GG_INTERACTION_TYPE'], float(value['GG_COST']), float(value['GG_SPEND']), \
# 		float(value['GG_APPSFLYER_INSTALL']), value['GG_STRATEGY_BID_TYPE']))



# statement = '''INSERT INTO DTM_GG_PIVOT_DETAIL (SNAPSHOT_DATE, CYEAR, CMONTH, LEGAL, DEPARTMENT, \
# 	DEPARTMENT_NAME, PRODUCT, PRODUCT_NAME, REASON_CODE_ORACLE, EFORM_NO, START_DATE, END_DATE, \
# 	CHANNEL) \
# 	VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)'''

# 	value = {
# 		'SNAPSHOT_DATE': '2017-06', 
# 		'CYEAR': '2017', 
# 		'CMONTH': '06', 
# 		'LEGAL': 'VNG', 
# 		'DEPARTMENT': '0902', 
# 		'DEPARTMENT_NAME': 'PG1', 
# 		'PRODUCT': '221', 
# 		'PRODUCT_NAME': 'JXM Mobi', 
# 		'REASON_CODE_ORACLE': '2017-06', 
# 		'EFORM_NO': 'FA-PA170427002', 
# 		'START_DATE': '2017-06-01', 
# 		'END_DATE': '2017-06-30', 
# 		'CHANNEL': 'GG'	
# 	}

