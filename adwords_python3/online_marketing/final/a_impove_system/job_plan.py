



















import sys
import json
import os
import time
import logging
from datetime import datetime , timedelta, date


import rule_plan_file as plan_file
import merger_data_rule_plan as merger_data

# import plan_change as plan_change
# import merge_data_manual_mapping as merge_data_manual_mapping

def ChangePlan(connect, path_data, date):
	# =============================== Manual mapping =========================================
	print("\n============= RUN RULE PLAN CHANGE: =================")
	start = time.time()

	list_camp_remove_unmap, list_camp_insert_unmap, \
	list_plan_insert_total, list_plan_update_total, \
	list_plan_remove_total, list_data_insert_map, \
	list_remove_manual = plan_file.ClassifyPlan(connect, path_data, date)

	end = time.time() - start
	print("             Time for rule plan in file: ", end)

	
	if list_camp_remove_unmap != [] or list_camp_insert_unmap != [] or \
	list_plan_insert_total != [] or list_plan_update_total != [] or \
	list_plan_remove_total != [] or list_data_insert_map != [] or \
	list_remove_manual != [] :
		merger = time.time()
		merger_data_for_plan(path_data, date, list_camp_remove_unmap, list_camp_insert_unmap, \
						list_plan_insert_total, list_plan_update_total, \
						list_plan_remove_total, list_data_insert_map, \
						list_remove_manual)
		
		end_merger = time.time() - merger
		print("        Time merger data for rule plan change : ", end_merger)
	else:
		print(" Not change")




# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEST_DATA'
# date = '2017-10-31' 

# list_plan_insert_total, list_plan_update_total, \
# list_plan_remove_total, list_data_insert_map, \
# list_remove_manual = ClassifyPlan(connect, path_data, date)