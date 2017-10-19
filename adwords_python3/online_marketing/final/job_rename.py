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

import merge_data_manual_mapping as merge_data_manual_mapping

def Rename (connect, path_data, list_customer_id, date):
	# =============================== Manual mapping =========================================
	print ("\n\n============= RUN RENAME WITH DATE : " + str(datetime.now()) + " =================")
	caculator_manual = time.time()

	list_diff = rename.CheckNameChange(path_data, list_customer_id, date)
	list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update, list_camp_update = rename.CacualatorChange(path_data, list_diff, date)

	time_caculator_manual = time.time() - caculator_manual
	print ("---------- Time caculator rename : ", time_caculator_manual)

	# --------------- 
	

	print (list_camp_need_removed != [])
	print (len(list_camp_remove_unmap))
	if list_camp_remove_unmap != []:
		update_manual = time.time()
		nru.Add_Data_To_Plan(connect, path_data, date)
		# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# merge_data_manual_mapping.UpdateRename(connect, list_camp_update)
		# merge_data_manual_mapping.merger_data_manual_mapping(connect, list_camp_remove_unmap, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update)
		time_update_manual = time.time() - update_manual
		print ("---------- Time update rename to total : ", time_update_manual)
	else:
		print (" Not change")
		# monthly_detail.InsertMonthlyDetailToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# monthly_sum.InsertMonthlySumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# plan_sum.InsertPlanSumToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)
		# detail_map.InsertDataMapToDatabase(path_data, connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, date)



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
date = '2017-08-31'
path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
Rename (connect, path_data, list_customer_id, date)