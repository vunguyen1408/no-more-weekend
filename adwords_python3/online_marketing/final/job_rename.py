import sys
import json
import os
import time
from datetime import datetime , timedelta, date


import rename as rename
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map
import insert_nru_to_plan as nru
import history_name as history_name

import merge_data_manual_mapping as merge_data_manual_mapping

def Rename (connect, path_data, list_customer_id, date):
	# =============================== Manual mapping =========================================
	print ("\n\n============= RUN RENAME WITH DATE : " + str(datetime.now()) + " =================")
	caculator_manual = time.time()

	list_diff, data_total = rename.CheckNameChange(path_data, list_customer_id, date)
	list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update, list_camp_update = rename.CacualatorChange(path_data, list_diff, date)

	time_caculator_manual = time.time() - caculator_manual
	print ("---------- Time caculator rename : ", time_caculator_manual)

	# --------------- 
	print (len(list_plan_remove_unmap))
	print (len(list_camp_remove_unmap))
	print (len(list_plan_update))
	print (len(list_camp_update))


	# print (list_camp_remove_unmap != [])
	# print (len(list_camp_remove_unmap))
	if list_camp_remove_unmap != [] or list_camp_update != []:
		update_manual = time.time()
		print ("insert data")
		# nru.Add_Data_To_Plan(connect, path_data, date)
		# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)

		merge_data_manual_mapping.UpdateRename(connect, list_camp_update, data_total)
		merge_data_manual_mapping.merger_data_manual_mapping(connect, list_camp_remove_unmap, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update)
		time_update_manual = time.time() - update_manual
		print ("---------- Time update rename to total : ", time_update_manual)
	else:
		print (" Not change")
		# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)



list_customer_id = [ 
     # WPL
     '1033505012', '6376833586', '6493618146', '3764021980', '9019703669', \
     '5243164713', '1290781574', '8640138177', '1493302671', '7539462658', \
     '1669629424', '6940796638', '6942753385', '3818588895', '8559396163', \
     '9392975361', '1756174326', '5477521592', '7498338868', '6585673574', \
     '5993679244', '5990401446', '5460890494', '3959508668', '1954002502', \
     '1124503774', '2789627019', '5219026641', '8760733662', '8915969454', \
     '9299123796', \
     # MP2
     '2351496518', '3766974726', '8812868246', '3657450042', '4092061132', \
     '1066457627', '7077229774', '6708858633', '2205921749', '1731093088', \
     '2852598370', \
     # PG1
     '5008396449', '9021114325', '9420329501', '7976533276', \
     # PG2
     '5471697015', '8198035241', '8919123364', '8934377519', '7906284750', \
     '1670552192', '6507949288', '3752996996', '5515537799', '9280946488', \
     '8897792146', '4732571543', '6319649915', '4845283915', '4963434062', \
     '3950481958', '8977015372', \
     # PG3
     '2018040612', '1237086810', '2474373259', '9203404951', '8628673438', \
     '5957287971', '6267264008', '8583452877', '4227775753', '8003403685', \
     '3061049910', '2395877275', '1849103506', '7000297269', '6233988585', \
     '4018935765', '2675507443', '9493600480', '1609917649', '8180518027', \
     '6275441244', '6743848595', '1362424990', '5430766142', '5800450880', \
     '7687258619', '8303967886', '5709003531', '6201418435', '1257508037', \
     '6810675582', '5953925776', '9001610198', '8135096980', '5222928599', \
     '9963010276', '5062362839', '6360800174', '8844079195', '5856149801', \
     '3064549723', '6198751560', '9034826980', '3265423139', '7891987656', \
     '8483981986', '2686387743', '5930063870', '7061686256', '3994588490', \
     '3769240354', \
     # GS5
     '8726724391', '1040561513', '7449117049', '3346913196', '9595118601', \
     '9411633791', '4596687625', '8290128509', '3104172682', '6247736011', \
     '2861959872', \
     
     # PP
     '8024455693' 
     ]


date = '2017-09-30'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/TEMP_DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
Rename (connect, path_data, list_customer_id, date)