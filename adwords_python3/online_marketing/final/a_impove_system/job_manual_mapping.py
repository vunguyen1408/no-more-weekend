import sys
import json
import os
import time
import logging
from datetime import datetime , timedelta, date


import manual_mapping as manual
import merge_data_manual_mapping as merge_data_manual_mapping

# import plan_change as plan_change
# import merge_data_manual_mapping as merge_data_manual_mapping

def ManualMapping (connect, path_data, date):
	# =============================== Manual mapping =========================================
	print("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + str(datetime.now()) + " =================")
	caculator_manual = time.time()
	list_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update = manual.ManualMap(connect, path_data, date)
	time_caculator_manual = time.time() - caculator_manual
	print("---------- Time caculator manual mapping to total : ", time_caculator_manual)

	# --------------- 
	# for map_ in list_map:
	# 	print (map_)
	# 	time.sleep(4)
	# 	print ("\n\n")

	print(list_plan_remove_unmap != [])
	print(list_camp_remove_unmap != [])
	print(len(list_camp_remove_unmap))
	if list_plan_remove_unmap != [] or list_camp_remove_unmap != []:
		update_manual = time.time()

		merge_data_manual_mapping.merger_data_manual_mapping(connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update)
		
		time_update_manual = time.time() - update_manual
		print("---------- Time update manual mapping to total : ", time_update_manual)
	else:
		print(" Not change")
		


date = '2017-08-31'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
ManualMapping (connect, path_data, date)