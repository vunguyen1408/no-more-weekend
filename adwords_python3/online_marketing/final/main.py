import logging
import sys
import json
import os
import time
from datetime import datetime , timedelta, date
from googleads import adwords

# ------------ PACKAGE --------------
import download_report as download
import mapping_campaign_plan as mapping_data
import insert_install_to_data as install
import merge_date as merge_date
import insert_data_map_to_total as insert_to_total
import manual_mapping as manual
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map

def Daily(connect, path_data, date):
	"""
		Run daily: 
		1. Duyet account - download report, luu tru
		2. Select install, mapping install.
		3. 
	"""
	

	start_work_flow = time.time()
	#========================== Download report =================================
	print ("======================= RUN GET REPORT WITH DATE : " + date + " =========================")
	download_report = time.time()
	# # --------- Doc list account
	list_customer_id = ['5008396449', '9021114325', '9420329501']
	# # Initialize client object.
	# adwords_client = adwords.AdWordsClient.LoadFromStorage()
	# for account in list_customer_id:
	# 	download.DownloadOnDate(adwords_client, account, path_data, date)
	time_download_report = time.time() - download_report
	print ("---------- Time get report: ", time_download_report)


	#======================== Insert install to data date ==============================
	print ("\n\n======================= RUN INSERT INSTALL WITH DATE : " + date + " =========================")
	insert_install = time.time()
	install.RunInsertInstall(connect, path_data, list_customer_id, date)
	time_insert = time.time() - insert_install

	#------------------ Read log manual mapping and get plan ---------------------
	mapping_data.ReadPlanFromTable(connect, path_data, date)
	manual.ReadTableManualMap(connect, path_data, date)
	#----------------------------------------------------------------

	print ("---------- Time insert install: ", time_insert)

	#======================== Mapping data for list account ============================
	print ("\n\n======================= RUN MAPPING WITH DATE : " + date + " =========================")
	mapping = time.time()
	mapping_data.MapDataForAllAccount(list_customer_id, path_data, date)
	time_mapping = time.time() - mapping
	print ("---------- Time maping: ", time_mapping)



	#============================== Merge data ===============================
	print ("\n\n======================= RUN MERGE WITH DATE : " + date + " =========================")
	merge = time.time()
	merge_date.Merge(path_data, list_customer_id, date)
	time_merge = time.time() - merge
	print ("---------- Time merge: ", time_merge)


	#============================== Insert data mapping to total ===============================
	print ("\n\n============= RUN INSERT DATA MAPPING TO TOTAL WITH DATE : " + date + " =================")
	insert_total = time.time()
	insert_to_total.InsertDateToTotal(path_data, date)
	time_insert_total = time.time() - insert_total
	print ("---------- Time insert data mapping to total : ", time_insert_total)

	#=============================== Manual mapping =========================================
	# print ("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + date + " =================")
	# insert_manual = time.time()
	# list_plan_remove, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
	# time_insert_manual = time.time() - insert_manual
	# print ("---------- Time insert manual mapping to total : ", time_insert_manual)

	#=============================== Update to database =========================================
	print ("\n\n============= RUN INSERT DATA TO DATABASE WITH DATE : " + date + " =================")
	insert_databse = time.time()
	list_plan_remove = []
	list_map = []
	list_camp_remove = []
	monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# detail_map.InsertDetailMapToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)

	time_insert_databse = time.time() - insert_databse
	print ("---------- Time insert data to database : ", time_insert_total)

	#----------------------------------------- END ---------------------------------------------
	time_run_work_flow  = time.time() - start_work_flow
	print (time_run_work_flow)

def ManyDate(connect, path_data, start_date, end_date):
	# Initialize client object.
	adwords_client = None
	# adwords_client = adwords.AdWordsClient.LoadFromStorage()

	date_ = datetime.strptime(start_date, '%Y-%m-%d').date()
	to_date_ = datetime.strptime(end_date, '%Y-%m-%d').date()
	n = int((to_date_ - date_).days)

	for i in range(n + 1):
		single_date = date_ + timedelta(i)
		d = single_date.strftime('%Y-%m-%d')
		Daily(connect, path_data, str(d))

start_date = '2017-06-01'
end_date = '2017-06-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
ManyDate(connect, path_data, start_date, end_date)

