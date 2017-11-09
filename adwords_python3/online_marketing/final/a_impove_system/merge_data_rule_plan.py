import cx_Oracle
import json
import os
from datetime import datetime , timedelta, date


import insert_monthly_detail as monthly_detail
import insert_monthly_sum as monthly_sum
import insert_plan_sum as plan_sum
import insert_data_map as detail_map





def merger_data_for_plan(path_data, date, list_camp_remove_unmap, list_camp_insert_unmap, \
						list_plan_insert_total, list_plan_update_total, \
						list_plan_remove_total, list_data_insert_map, \
						list_remove_manual):

	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()


	
	#=================== List campaign remove from table detail_unmap =====================
	if (len(list_camp_remove_unmap) > 0):
		for camp in list_camp_remove_unmap:
			detail_map.DeleteCamp(camp, cursor)
	

	#=================== List campaign insert into detail_unmap =====================
	if (len(list_camp_insert_unmap) > 0):
		for camp in list_camp_insert_unmap:
			value = detail_map.ConvertJsonCamp(camp)
			detail_map.InsertDetailUnmap(value, cursor)
	

	# =============== List plan insert into total ============================
	if (len(list_plan_insert_total) > 0):
		for plan in list_plan_insert_total:
			json_ = plan_sum.ConvertJsonPlanSum(plan)
			plan_sum.InsertPlanSum(json_, cursor)

			if ('MONTHLY' in plan):
				for i in range(len(plan['MONTHLY'])):
					json_ = monthly_sum.ConvertJsonMonthlySum(i, plan)
					monthly_sum.InsertMonthlySum(json_, cursor)

					json_ = monthly_detail.ConvertJsonMonthlyDetail(i, plan)
					monthly_detail.InsertMonthlyDetail(json_, cursor)

	
	# ============ List plan update into total =====================
	if (len(list_plan_update_total) > 0):
		for plan in list_plan_update_total:
			json_ = plan_sum.ConvertJsonPlanSum(plan)
			plan_sum.UpdatePlanPlanSum(json_, cursor)

			if ('MONTHLY' in plan):
				for i in range(len(plan['MONTHLY'])):
					json_ = monthly_sum.ConvertJsonMonthlySum(i, plan)
					monthly_sum.UpdatePlanMonthlySum(json_, cursor)

					json_ = monthly_detail.ConvertJsonMonthlyDetail(i, plan)
					monthly_detail.UpdatePlanMonthlyDetail(json_, cursor)
	


	
	# ============ List plan remove from total =====================
	if (len(list_plan_remove_total) > 0):
		for plan in list_plan_remove_total:			
			plan_sum.DeletePlanSum(plan, cursor)			
			monthly_sum.DeleteMonthlySum(plan, cursor)			
			monthly_detail.DeleteMonthlyDetail(plan, cursor)
			detail_map.DeletePlan(plan, cursor)
	

	start = time.time()
	#=================== List data insert into detail_unmap =====================
	if (len(list_data_insert_map) > 0):
		for plan in list_data_insert_map:
			value = detail_map.ConvertJsonMap(plan)
			detail_map.InsertDetailUnmap(value, cursor)
	print ("Time insert for list_data_insert_map : ", (time.time() - start))
	
	#=================== Commit and close connect =================
	# conn.commit()
	# print("Committed!.......")
	cursor.close()

	#=================== List log remove from log manual =====================
	remove_log_manual.RemoveManualLog(path_data, date, list_remove_manual)