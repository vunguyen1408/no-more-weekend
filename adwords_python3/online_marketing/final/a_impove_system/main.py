import logging
import sys
import json
import os
import time
from datetime import datetime , timedelta, date
# from googleads import adwords

# ------------ PACKAGE --------------
import mapping_campaign_plan as mapping_data
import add_acc_name as add_acc_name
import merge_date as merge_date
import insert_data_map_to_total as insert_to_total

import insert_install_brandingGPS_to_plan as insert_install_brandingGPS
import insert_install as insert_install

import insert_monthly_detail as insert_monthly_detail
import insert_monthly_sum as insert_monthly_sum
import insert_plan_sum as insert_plan_sum
import insert_data_map as insert_data_map

def Daily(connect, path_data, date, list_customer_id):
	#----------------------------------------- Begin ---------------------------------------------
	start_work_flow = time.time()
	

	#======================== Mapping data for list account ============================
	print ("\n\n======================= RUN MAPPING WITH DATE : " + date + " =========================")
	mapping = time.time()
	mapping_data.MapDataForAllAccount(connect, list_customer_id, path_data, date)
	time_mapping = time.time() - mapping
	print ("             Time maping: ", time_mapping)

	
	#============================== Merge data ===============================
	print ("\n\n======================= RUN MERGE WITH DATE : " + date + " =========================")
	merge = time.time()
	merge_date.MergeDataMapping(path_data, list_customer_id, date)
	time_merge = time.time() - merge
	print ("             Time merge: ", time_merge)


	# ============================== Insert data mapping to total ===============================
	print ("\n\n============= RUN INSERT DATA MAPPING TO TOTAL WITH DATE : " + date + " =================")
	insert_total = time.time()

	list_data_map, list_plan_insert, list_plan_remove, list_plan_update \
	= insert_to_total.InsertDateToTotal(path_data, date)

	time_insert_total = time.time() - insert_total
	print ("            Time insert data mapping to total : ", time_insert_total)

	# ======================= Insert branding install ====================================
	start = time.time()
	insert_install.InsertInstall(path_data, connect, date)
	insert_install_brandingGPS.AddBrandingGPS(path_data, connect, date)
	print ("Time : ", time.time() - start)


	# =============================== Update to database =========================================
	print ("\n\n============= RUN INSERT DATA TO DATABASE WITH DATE : " + date + " =================")
	insert_databse = time.time()
	list_plan_insert = []
	list_plan_update = []
	insert_monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_plan_insert, list_plan_update, date)
	insert_monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_plan_insert, list_plan_update, date)
	insert_plan_sum.InsertPlanSumToDatabase(path_data, connect, list_plan_insert, list_plan_update, date)
	insert_data_map.InsertDataMapToDatabase(path_data, connect, list_plan_insert, list_plan_update, date)
	time_insert_databse = time.time() - insert_databse
	print ("            Time insert data to database : ", time_insert_databse)


	
	time_run_work_flow  = time.time() - start_work_flow
	print ("            TOTAL TIME : ", time_run_work_flow)
	#----------------------------------------- END ---------------------------------------------

def ManyDate(connect, path_data, start_date, end_date):
	# Initialize client object.
	adwords_client = None
	# adwords_client = adwords.AdWordsClient.LoadFromStorage()
	path_acc = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT'
	list_acc, list_mcc, list_dept = add_acc_name.get_list_customer(path_acc)
	# print (len(list_acc))

	date_ = datetime.strptime(start_date, '%Y-%m-%d').date()
	to_date_ = datetime.strptime(end_date, '%Y-%m-%d').date()
	n = int((to_date_ - date_).days)
	s = time.time()
	for i in range(n + 1):
		single_date = date_ + timedelta(i)
		d = single_date.strftime('%Y-%m-%d')
		Daily(connect, path_data, str(d), list_acc)
	e = time.time() - s
	print (" TIME : ", e)


# start_date = '2017-06-01'
# end_date = '2017-06-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# ManyDate(connect, path_data, start_date, end_date)

if __name__ == '__main__':
    from sys import argv
    
    script, start_date, end_date = argv
    ManyDate(connect, path_data, start_date, end_date)
