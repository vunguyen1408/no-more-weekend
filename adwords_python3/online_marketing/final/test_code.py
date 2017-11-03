import json
import os 
from datetime import datetime, timedelta, date



def RemoveLogManual(path_data, date, list_remove_manual):
	path_log_manual = os.path.join(path_data, str(date), 'LOG_MANUAL/log_manual.json')
	with open(path_log_manual, 'r') as fi:
		data_manual  = json.load(fi)

	for value in list_remove_manual:
		for data in data_manual['LOG']:
			if value['PLAN']['PRODUCT'] == data['PRODUCT'] \
			and value['PLAN']['REASON_CODE_ORACLE'] == data['REASON_CODE_ORACLE'] \
			and value['PLAN']['FORM_TYPE'] == data['FORM_TYPE'] \
			and value['PLAN']['UNIT_OPTION'] == data['UNIT_OPTION']:

				pass

def GetMinDate(list_date):
	list_temp = []
	for date in list_date:
		list_temp.append(datetime.strptime(date, '%Y-%m-%d'))

	min_date = list_temp[0]
	for temp in list_temp:
		if (temp < min_date):
			min_date = temp

	min_date = min_date.strftime('%Y-%m-%d')	
	return min_date


def GetMaxDate(list_date):
	list_temp = []
	for date in list_date:
		list_temp.append(datetime.strptime(date, '%Y-%m-%d'))

	max_date = list_temp[0]
	for temp in list_temp:
		if (temp > max_date):
			max_date = temp

	max_date = max_date.strftime('%Y-%m-%d')	
	return max_date


def DivideRangeDate(value):

	list_result = []
	
	min_date = datetime.strptime(GetMinDate(value['LIST_DATE']), '%Y-%m-%d')
	max_date = datetime.strptime(GetMaxDate(value['LIST_DATE']), '%Y-%m-%d')
	log = {
		'PLAN': value['PLAN'],
		'CAMPAIGN_ID': value['CAMPAIGN_ID'],
		'START_DATE': min_date.strftime('%Y-%m-%d'),
		'END_DATE': min_date.strftime('%Y-%m-%d')
	}
	
	date = min_date
	value['LIST_DATE'].remove(date.strftime('%Y-%m-%d'))
	flag = True
	while (date <= max_date) and flag:
		date += timedelta(1)
		if date.strftime('%Y-%m-%d') not in value['LIST_DATE']:
			flag = False
		else:
			value['LIST_DATE'].remove(date.strftime('%Y-%m-%d'))

	if flag == False:
		log['END_DATE'] = (date - timedelta(1)).strftime('%Y-%m-%d')
		# DivideRangeDate(value)

	if (date > max_date):
		log['END_DATE'] = max_date.strftime('%Y-%m-%d')

	print(value['LIST_DATE'])
	if len(value['LIST_DATE']) > 0:
		result = DivideRangeDate(value)
		list_result.extend(result)

	list_result.append(log)	
	return list_result


def ConvertListCamp(list_remove_manual):
	list_result = []
	list_log = []	
	
	for plan in list_remove_manual:
		list_temp = []
		list_camp_id = []
		for camp in plan['CAMPAIGN_MANUAL_MAP']:
			if camp['CAMPAIGN_ID'] not in list_camp_id:				
				list_camp_id.append(camp['CAMPAIGN_ID'])
				_camp = {
						"PLAN": plan['PLAN'],
						"CAMPAIGN_ID": camp['CAMPAIGN_ID'],
						"LIST_DATE": [camp['UPDATE_DATE']],
						"START_DATE": None,
						"END_DATE": None
				}
				list_temp.append(_camp)
			else:
				list_temp[list_camp_id.index(camp['CAMPAIGN_ID'])]['LIST_DATE'].append(camp['UPDATE_DATE'])
			
		list_result.extend(list_temp)	

	for result in list_result:
		log = DivideRangeDate(result)
		list_log.extend(log)

	return list_log


def RemoveManualLog(path_data, date, list_remove_manual):
	file_log = os.path.join(path_data, str(date) + '/LOG_MANUAL/log_manual.json')
	with open(file_log, 'r') as fi:
		data_log = json.load(fi)

	list_log = ConvertListCamp(list_remove_manual)

	for log in list_log:
		for manual in data_log['LOG']:
			if log['PLAN']['PRODUCT'] == manual['PRODUCT'] and \
				log['PLAN']['REASON_CODE_ORACLE'] == manual['REASON_CODE_ORACLE'] and \
				log['PLAN']['FORM_TYPE'] == manual['FORM_TYPE'] and \
				log['PLAN']['UNIT_OPTION'] == manual['UNIT_OPTION']:
			






# path = 'C:/Users/CPU10912-local/Desktop/remove.json'
# with open(path, 'r') as fi:
# 	list_remove_manual = json.load(fi)

list_remove_manual = [{
  "PLAN": {
      "CYEAR": "17",
      "CMONTH": "6",
      "LEGAL": "VNG",
      "DEPARTMENT": "0902",
      "DEPARTMENT_NAME": "PG1",
      "PRODUCT": "221",
      "REASON_CODE_ORACLE": "1706008",
      "EFORM_NO": "FA-PA170529003",
      "START_DAY": "2017-06-01",
      "END_DAY_ESTIMATE": "2017-06-30",
      "CHANNEL": "GG",
      "FORM_TYPE": "SEARCH",
      "UNIT_OPTION": "CPI",
      "UNIT_COST": "1.3",
      "AMOUNT_USD": 39000,
      "CVALUE": 30000,
      "ENGAGEMENT": None,
      "IMPRESSIONS": None,
      "CLIKE": None,
      "CVIEWS": None,
      "INSTALL": 30000,
      "NRU": None,
      "INSERT_DATE": "2017-09-13",
	},
	"CAMPAIGN_MANUAL_MAP": [
		{
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-01"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-02"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-03"
        },
        {
          "CAMPAIGN_ID": 682222537,
          "UPDATE_DATE": "2017-06-04"
        },
        {
          "CAMPAIGN_ID": 682222537,
          "UPDATE_DATE": "2017-06-05"
        },
        {
          "CAMPAIGN_ID": 682222537,
          "UPDATE_DATE": "2017-06-06"
        }
      ]
	},
	{
  "PLAN": {
      "CYEAR": "17",
      "CMONTH": "6",
      "LEGAL": "VNG",
      "DEPARTMENT": "0902",
      "DEPARTMENT_NAME": "PG1",
      "PRODUCT": "221",
      "REASON_CODE_ORACLE": "1706008",
      "EFORM_NO": "FA-PA170529003",
      "START_DAY": "2017-06-01",
      "END_DAY_ESTIMATE": "2017-06-30",
      "CHANNEL": "GG",
      "FORM_TYPE": "SEARCH",
      "UNIT_OPTION": "CPI",
      "UNIT_COST": "1.3",
      "AMOUNT_USD": 39000,
      "CVALUE": 30000,
      "ENGAGEMENT": None,
      "IMPRESSIONS": None,
      "CLIKE": None,
      "CVIEWS": None,
      "INSTALL": 30000,
      "NRU": None,
      "INSERT_DATE": "2017-09-13",
	},
	"CAMPAIGN_MANUAL_MAP": [
		{
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-01"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-02"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-03"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-04"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-05"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-06-06"
        }
      ]
	},
	{"PLAN": {
      "CYEAR": "17",
      "CMONTH": "6",
      "LEGAL": "VNG",
      "DEPARTMENT": "0902",
      "DEPARTMENT_NAME": "PG1",
      "PRODUCT": "221",
      "REASON_CODE_ORACLE": "1706008",
      "EFORM_NO": "FA-PA170529003",
      "START_DAY": "2017-06-01",
      "END_DAY_ESTIMATE": "2017-06-30",
      "CHANNEL": "GG",
      "FORM_TYPE": "SEARCH",
      "UNIT_OPTION": "CPI",
      "UNIT_COST": "1.3",
      "AMOUNT_USD": 39000,
      "CVALUE": 30000,
      "ENGAGEMENT": None,
      "IMPRESSIONS": None,
      "CLIKE": None,
      "CVIEWS": None,
      "INSTALL": 30000,
      "NRU": None,
      "INSERT_DATE": "2017-09-13",
	},
	"CAMPAIGN_MANUAL_MAP": [
		{
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-09-01"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-09-02"
        },
        {
          "CAMPAIGN_ID": 682545537,
          "UPDATE_DATE": "2017-09-03"
        },
        {
          "CAMPAIGN_ID": 6111111117,
          "UPDATE_DATE": "2017-10-04"
        },
        {
          "CAMPAIGN_ID": 6111111117,
          "UPDATE_DATE": "2017-10-05"
        },
        {
          "CAMPAIGN_ID": 6111111117,
          "UPDATE_DATE": "2017-11-06"
        }
      ]
	}
	]


list_log = ConvertListCamp(list_remove_manual)

for log in list_log:
	print(log)







