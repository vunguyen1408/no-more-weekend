import sys
import json
import os
import time
import cx_Oracle
from datetime import datetime , timedelta, date


import manual_mapping as manual
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map


# def ManualMapping (connect, path_data, date):
# 	# =============================== Manual mapping =========================================
# 	print ("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + date + " =================")
# 	insert_manual = time.time()
# 	list_map, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
# 	time_insert_manual = time.time() - insert_manual
# 	print ("---------- Time insert manual mapping to total : ", time_insert_manual)
	
# 	monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
# 	monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
# 	plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)

def ManualMapping (connect, path_data, date):	
	# =============================== Manual mapping =========================================
	print ("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + date + " =================")
	insert_manual = time.time()

	list_map, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)	
	
	# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)

	time_insert_manual = time.time() - insert_manual
	print ("---------- Time insert manual mapping to total : ", time_insert_manual)	

	return time_insert_manual





def ManualFlow(connect, path_data, date):
#=================== Read flag running =============================	
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	statement = 'select * from DTM_GG_RUN_FLAG'		
	cursor.execute(statement)
	running = cursor.fetchall()

	if (len(running) == 0):
		statement = "insert into DTM_GG_RUN_FLAG (FLAG_RUNNING, FINAL_RUNTIME) values (:1, :2)"
		cursor.execute(statement, ('False', None))
		conn.commit()

		statement = 'select * from DTM_GG_RUN_FLAG'		
		cursor.execute(statement)
		running = cursor.fetchall()

	if (running[0] == 'False'):
		#======= Turn on flag running ==================		
		statement = "update DTM_GG_RUN_FLAG set FLAG_RUNNING = 'True' when FLAG_RUNNING = 'False'"
		cursor.execute(statement)
		conn.commit()
		
		time_flow = ManualMapping (connect, path_data, date)

		statement = "update DTM_GG_RUN_FLAG set FINAL_RUNTIME = :1, FLAG_RUNNING = 'False' when FLAG_RUNNING = 'True'"
		cursor.execute(statement, (time_flow))
		conn.commit()
	cursor.close()



path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
from sys import argv
script, date = argv
ManualFlow(connect, path_data, date)