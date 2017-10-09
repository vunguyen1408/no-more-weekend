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
	caculator_manual = time.time()
	list_map, list_plan_remove_unmap, list_camp_remove_unmap = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
	time_caculator_manual = time.time() - caculator_manual
	print ("---------- Time caculator manual mapping to total : ", time_caculator_manual)


	with open('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/log.txt', 'a') as f:
		import datetime
		i = datetime.datetime.now()
		f.write("Current date & time = %s" % i)
		f.write(str(list_plan_remove_unmap) + '\n')
		f.write("==========================================================")
		f.write(str(list_camp_remove_unmap) + '\n')
		f.write("DONNNNNNNNNNNNNNNNNE\n\n")


	print (list_plan_remove_unmap != [])
	print (list_camp_remove_unmap != [])
	print (len(list_camp_remove_unmap))
	if list_plan_remove_unmap != [] or list_camp_remove_unmap != []:
		update_manual = time.time()
		monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		time_update_manual = time.time() - update_manual
		print ("---------- Time update manual mapping to total : ", time_update_manual)
	else:
		print (" Not change")
		# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)


date = '2017-08-31'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
ManualMapping (connect, path_data, date)