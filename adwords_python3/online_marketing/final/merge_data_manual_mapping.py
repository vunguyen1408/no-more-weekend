import cx_Oracle
import json
import os
from datetime import datetime , timedelta, date
import insert_report_monthly_detail as monthly_detail
import insert_report_monthly_sum as monthly_sum
import insert_report_plan_sum as plan_sum
import insert_report_detail_map as detail_map
import history_name as history_name

def UpdateRename(connect, list_camp_update, data):
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()
	#==================== Update data into database =============================
	statement = 'update DTM_GG_PIVOT_DETAIL_UNMAP \
	set CAMPAIGN_NAME = :1 \
	where CAMPAIGN_ID = :2 and SNAPSHOT_DATE = :3'	
	for value in list_camp_update:
		try:
			cursor.execute(statement, (value['Campaign'], value['Campaign ID'], value['Date']))
		except:
			cursor.execute(statement, (value['Campaign'].encode('utf-8'), value['Campaign ID'], value['Date']))

		print ((value['Campaign'], value['Campaign ID'], value['Date']))

	for i in data['HISTORY']:
		history_name.MergerCampList(i, cursor)

	conn.commit()
	print("Committed!.......")
	cursor.close()


def merger_data_manual_mapping(connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()

	# =========== List Plan Remove ==================
	if (len(list_plan_remove_unmap) > 0):
		for plan in list_plan_remove_unmap:
			detail_map.DeletePlan(plan, cursor)


	# =========== List Campaign Remove ==================
	if (len(list_camp_remove_unmap) > 0):
		for plan in list_camp_remove_unmap:
			detail_map.DeleteCamp(plan, cursor)


	# =========== List data manual map ==================
	print ("Length map : ", len(list_map))
	if  (len(list_map) > 0):
		for value in list_map:					
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


# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# list_map = []
# list_plan_remove_unmap = []
# list_camp_remove_unmap = []
# list_plan_update = []

# merger_data_manual_mapping(connect, list_map, list_plan_remove_unmap, list_camp_remove_unmap, list_plan_update)