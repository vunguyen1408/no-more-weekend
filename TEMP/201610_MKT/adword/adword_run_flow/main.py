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
import insert_nru_to_plan as nru
def Daily(connect, path_data, date):
	"""
		Run daily: 
		1. Duyet account - download report, luu tru
		2. Select install, mapping install.
		3. 
	"""

	# # --------- Doc list account
	list_customer_id = ['1033505012', '1057617213', '1066457627', '1124503774', '1163330677', \
		'1276490383', '1290781574', '1359687200', '1373058452', '1492278512', \
		'1493302671', '1496752295', '1547282976', '1669629424', '1912353902', \
		'1954002502', '2789627019', '3320825113', '3346913196', '3381621349', \
		'3436962801', '3581453552', '3668363407', '3699371994', '3752996996', \
		'3764021980', '3785612315', '3818588895', '3836577058', '3959508668', \
		'4047293553', '4092061132', '4219579467', '4270191371', '4566721209', \
		'4585745870', '4626844359', '4798268655', '4869930138', '4888935554', \
		'4895791874', '5008396449', '5219026641', '5243164713', '5460890494', \
		'5477521592', '5515537799', '5558683488', '5879354452', '5881347043', \
		'5886101084', '5925380036', '5939646969', '5984796540', '5990401446', \
		'5993679244', '6102951142', '6140218608', '6223856123', '6319649915', \
		'6376833586', '6408702983', '6493618146', '6585673574', '6708858633', \
		'6787284625', '6940796638', '6942753385', '7011814018', '7013760267', \
		'7017079687', '7077229774', '7498338868', '7539462658', '7802963373', \
		'7886422201', '7976533276', '7986041343', '8024455693', '8324896471', \
		'8353864179', '8559396163', '8579571740', '8640138177', '8760733662', \
		'8812868246', '8915969454', '9019703669', '9021114325', '9294243048', \
		'9299123796', '9358928000', '9377025866', '9392975361', '9420329501', \
		'9694660207', '9719199461', '7891987656', '6267264008', '8583452877', \
		'3099510382', '5471697015', '8198035241', '8919123364', '8934377519', \
		'7906284750', '1670552192', '3785612315', '6507949288', '3752996996', \
		'5515537799', '9280946488', '8897792146', '4732571543', '6319649915', \
		'4845283915', '4963434062', '3950481958', '8977015372', '2018040612', \
		'1237086810', '2474373259', '9203404951', '8628673438', '5957287971', \
		'3265423139', '8483981986', '2686387743', '5930063870', '7061686256', \
		'3994588490', '3769240354', '4227775753', '8003403685', '3061049910', \
		'2395877275', '1849103506', '7000297269', '6233988585', '4018935765', \

		'2675507443', '9493600480', '1609917649', '8180518027', '6275441244', \
		'6743848595', '8726724391', '1040561513', '7449117049', '3346913196', \
		'9595118601', '1362424990', '5430766142', '9411633791', '4596687625', \
		'8290128509', '3104172682', '6247736011', '2861959872', '5800450880', \
		'8303967886', '6201418435', '6810675582', '5953925776', '9001610198', \
		'8135096980', '5222928599', '9963010276', '5062362839', '6360800174', \
		'8844079195', '5856149801', '6198751560', '9034826980', '8024455693']

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

	
	#======================== Insert nru to plan ==============================
	# print ("\n\n======================= RUN INSERT NRU WITH DATE : " + date + " =========================")
	# insert_nru = time.time()
	nru.Add_Data_To_Plan(connect, path_data, date)
	# time_insert = time.time() - insert_install

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
