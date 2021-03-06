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
import history_name as history
def Daily(connect, path_data, date):
	"""
		Run daily: 
		1. Duyet account - download report, luu tru
		2. Select install, mapping install.
		3. 
	"""

	# # --------- Doc list account
	list_customer_id = ['6140218608', '5984796540', '5939646969', '4869930138', '4626844359', \
						'4047293553', '3581453552', '3320825113', '1954002502', '1547282976', \
						'8579571740', '8324896471', '7017079687', '7013760267', '6408702983', \
						'1492278512', '1124503774', '5219026641', '8915969454', '9299123796', \
						'3381621349', '7986041343', '9377025866', '6102951142', '3668363407', '4895791874', \
						'1373058452', '3699371994', '1496752295', '6787284625', '4780719992', \
						'5515537799', '3436962801', '7802963373', '5925380036', '3836577058', \
						'1163330677', '1057617213', '4798268655', '8812868246', '7976533276', \
						'9420329501', '3785612315', '9719199461', '1912353902', '4585745870', \
						'9358928000', '4566721209', '1547282976', '1359687200', '1124503774', \
						'5219026641', '8760733662', '5460890494', '4270191371', '4219579467', \
						'3959508668', '1954002502', '6585673574', '5993679244', '5990401446', \
						'7498338868', '9392975361', '9294243048', '7886422201', '6940796638', \
						'6942753385', '3818588895', '8640138177', '1493302671', '7539462658', \
						'5243164713', '9019703669', '3764021980', '8024455693', '7077229774', \
						'6708858633', '1066457627', '4092061132', '3346913196', '5886101084', \
						'3752996996', '8353864179', '1033505012', '5008396449', '6319649915', \
						'1290781574', '1669629424', '6376833586', '6493618146', '9021114325']

	start_work_flow = time.time()
	#========================== Download report =================================
	# print ("======================= RUN GET REPORT WITH DATE : " + date + " =========================")
	# download_report = time.time()
	# # Initialize client object.
	# adwords_client = adwords.AdWordsClient.LoadFromStorage()
	# for account in list_customer_id:
	# 	download.DownloadOnDate(adwords_client, account, path_data, date)
	# time_download_report = time.time() - download_report
	# print ("            Time get report: ", time_download_report)

	#======================== History name ==================================
	history.InsertHistoryName(connect, path_data, list_customer_id, date)

	#======================== Insert install to data date ==============================
	# print ("\n\n======================= RUN INSERT INSTALL WITH DATE : " + date + " =========================")
	# insert_install = time.time()
	install.RunInsertInstall(connect, path_data, list_customer_id, date)

	# time_insert = time.time() - insert_install



	#------------------ Read log manual mapping and get plan NRU ---------------------
	mapping_data.ReadPlanFromTable(connect, path_data, date)
	
	mapping_data.ReadProductAlias(connect, path_data, date)
	manual.ReadTableManualMap(connect, path_data, date)
	#----------------------------------------------------------------
	# print ("             Time insert install: ", time_insert)



	#======================== Mapping data for list account ============================
	# print ("\n\n======================= RUN MAPPING WITH DATE : " + date + " =========================")
	# mapping = time.time()
	mapping_data.MapDataForAllAccount(list_customer_id, path_data, date)
	# time_mapping = time.time() - mapping
	# print ("             Time maping: ", time_mapping)



	#============================== Merge data ===============================
	# print ("\n\n======================= RUN MERGE WITH DATE : " + date + " =========================")
	# merge = time.time()
	merge_date.Merge(path_data, list_customer_id, date)
	# time_merge = time.time() - merge
	# print ("             Time merge: ", time_merge)


	#============================== Insert data mapping to total ===============================
	# print ("\n\n============= RUN INSERT DATA MAPPING TO TOTAL WITH DATE : " + date + " =================")
	# insert_total = time.time()
	insert_to_total.InsertDateToTotal(path_data, date)
	# time_insert_total = time.time() - insert_total
	# print ("            Time insert data mapping to total : ", time_insert_total)

	# # =============================== Manual mapping =========================================
	# print ("\n\n============= RUN INSERT MANUAL MAPPING TO TOTAL WITH DATE : " + date + " =================")
	# insert_manual = time.time()
	# list_plan_remove, list_plan_remove, list_camp_remove = manual.GetCampaignUnMapForManualMap(connect, path_data, date)
	# time_insert_manual = time.time() - insert_manual
	# print ("---------- Time insert manual mapping to total : ", time_insert_manual)

	#=============================== Update to database =========================================
	# print ("\n\n============= RUN INSERT DATA TO DATABASE WITH DATE : " + date + " =================")
	# insert_databse = time.time()
	list_plan_remove = []
	list_map = []
	list_camp_remove = []
	
	# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)
	# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove, list_camp_remove, date)

	# time_insert_databse = time.time() - insert_databse
	# print ("            Time insert data to database : ", time_insert_total)

	#----------------------------------------- END ---------------------------------------------
	time_run_work_flow  = time.time() - start_work_flow
	print (date)
	print ("            TOTAL TIME : ",time_run_work_flow)
	print ("\n\n")

def ManyDate(connect, path_data, start_date, end_date):
	# Initialize client object.
	adwords_client = None
	# adwords_client = adwords.AdWordsClient.LoadFromStorage()

	date_ = datetime.strptime(start_date, '%Y-%m-%d').date()
	to_date_ = datetime.strptime(end_date, '%Y-%m-%d').date()
	n = int((to_date_ - date_).days)
	s = time.time()
	for i in range(n + 1):
		single_date = date_ + timedelta(i)
		d = single_date.strftime('%Y-%m-%d')
		Daily(connect, path_data, str(d))
	e = time.time() - s
	print (" TIME : ", e)


# start_date = '2017-06-01'
# end_date = '2017-06-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# ManyDate(connect, path_data, start_date, end_date)

if __name__ == '__main__':
    from sys import argv
    
    script, start_date, end_date = argv
    ManyDate(connect, path_data, start_date, end_date)
