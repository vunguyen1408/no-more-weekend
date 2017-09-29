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
	list_plan_remove, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
	time_insert_manual = time.time() - insert_manual
	print ("---------- Time insert manual mapping to total : ", time_insert_manual)



date = '2017-06-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
ManualMapping (connect, path_data, date)