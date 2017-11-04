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

def Daily(connect, path_data, date, list_customer_id):
	#----------------------------------------- Begin ---------------------------------------------
	start_work_flow = time.time()
	

	#======================== Mapping data for list account ============================
	print ("\n\n======================= RUN MAPPING WITH DATE : " + date + " =========================")
	mapping = time.time()
	mapping_data.MapDataForAllAccount(list_customer_id, path_data, date)
	time_mapping = time.time() - mapping
	print ("             Time maping: ", time_mapping)

	
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
