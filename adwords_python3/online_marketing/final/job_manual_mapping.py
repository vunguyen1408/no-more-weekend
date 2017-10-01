import sys
import json
import os
import time
from datetime import datetime , timedelta, date


import manual_mapping as manual
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map


def ManualMapping (connect, path_data, date):
	# =============================== Manual mapping =========================================
	print ("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + date + " =================")
	insert_manual = time.time()
	list_map, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
	time_insert_manual = time.time() - insert_manual
	print ("---------- Time insert manual mapping to total : ", time_insert_manual)
	
	monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)


start = time.time()

from sys import argv
script, date = argv

path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
ManualMapping (connect, path_data, date)


time_flow = time.time() - start
print ("Time flow 5 min: ", time_flow)