import cx_Oracle
import json
import os
from datetime import datetime , timedelta, date
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map



def merger_data_daily(connect, list_data_map, list_plan_remove, list_plan_update):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	# =========== List plan remove ==================
	if (len(list_plan_remove) > 0):
		for plan in list_plan_remove:
			detail_map.DeletePlan(plan, cursor)


	# =========== List data map ==================	
	if  (len(list_data_map) > 0):
		for value in list_data_map:					
			json_ = detail_map.ConvertJsonMap(value)	
			try:		
				detail_map.InsertDetailUnmap(json_, cursor)
			except UnicodeEncodeError as e:				
				json_['CAMPAIGN_NAME'] = value['Campaign'].encode('utf-8')
				detail_map.InsertDetailUnmap(json_, cursor)



	# ============ List plan update =====================
	if (len(list_plan_update) > 0):
		for plan in list_plan_update:
			json_ = plan_sum.ConvertJsonPlanSum(plan)
			plan_sum.UpdatePlanSum(json_, cursor)

			if ('MONTHLY' in plan):
				for i in range(len(plan['MONTHLY'])):
					json_ = monthly_sum.ConvertJsonMonthlySum(i, plan)
					monthly_sum.UpdateMonthlySum(json_, cursor)

					json_ = monthly_detail.ConvertJsonMonthlyDetail(i, plan)
					monthly_detail.UpdateMonthlyDetail(json_, cursor)

	#=================== Commit and close connect =================
	conn.commit()
	print("Committed!.......")
	cursor.close()