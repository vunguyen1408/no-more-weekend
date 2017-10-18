# import sys
# import os
# import pandas as pd
# import numpy as np
# import json
# import cx_Oracle
# from datetime import datetime , timedelta, date

# def InsertCampList(value, cursor):
# 	#==================== Insert data into database =============================
# 	statement = 'insert into STG_CAMPAIGN_LIST_GG ( \
# 	ACCOUNT_ID, CAMPAIGN_ID, CAMPAIGN_NAME, INSERT_DATE, \
# 	UPDATE_DATE, STATUS, IMPORT_DATE) \
# 	values (:1, :2, :3, :4, :5, :6, :7) '
	
# 	try:
# 		cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'], \
# 			datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), None, None))
# 	except UnicodeEncodeError as e:
# 		cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], value['CAMPAIGN_NAME'].encode('utf-8'), \
# 			datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), None, None))
# 		# print (e)
	
# 	# print("A row inserted!.......")

# def UpdateCampList(value, cursor):
# 	#==================== Insert data into database =============================

# 	statement = 'update STG_CAMPAIGN_LIST_GG \
# 	set CAMPAIGN_NAME = :1, INSERT_DATE = :2, UPDATE_DATE = :3 \
# 	where ACCOUNT_ID = :4 and CAMPAIGN_ID = :5'
	
# 	try:
# 		cursor.execute(statement, (value['CAMPAIGN_NAME'], datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), \
# 			datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), value['ACCOUNT_ID'], value['CAMPAIGN_ID']))
# 	except UnicodeEncodeError as e:
# 		cursor.execute(statement, (value['CAMPAIGN_NAME'].encode('utf-8'), datetime.strptime(value['DATE_GET'], '%Y-%m-%d'), \
# 			datetime.strptime(value['UPDATE_DATE'], '%Y-%m-%d'), value['ACCOUNT_ID'], value['CAMPAIGN_ID']))
# 		# print (e)

# 	# print("   A row updated!.......")


# def MergerCampList(value, cursor):
# 	#==================== Insert data into database =============================
# 	statement = 'select CAMPAIGN_NAME from STG_CAMPAIGN_LIST_GG \
# 	where ACCOUNT_ID = :1 and CAMPAIGN_ID = :2 and INSERT_DATE = :3'	
		
# 	cursor.execute(statement, (value['ACCOUNT_ID'], value['CAMPAIGN_ID'], datetime.strptime(value['DATE_GET'], '%Y-%m-%d')))
# 	res = cursor.fetchall()

# 	if (len(res) == 0):
# 		InsertCampList(value, cursor)
# 	elif (value['CAMPAIGN_NAME'] != res[0][0]):
# 		print ("---------------------------- UPDATE NAME ----------------------------------")
# 		UpdateCampList(value, cursor)
# 	# print("	A row mergered!.......")

# def InsertHistoryName(connect, path_data, list_account, date):
# 	conn = cx_Oracle.connect(connect)
# 	cursor = conn.cursor()
# 	# for account in list_account:
# 	# 	AccountFromCampaign(account, path_data, date)
# 	path_data_his = os.path.join(path_data + '/' + str(date) + '/DATA_MAPPING', 'history_name' + '.json')
# 	print (path_data_his)
# 	if os.path.exists(path_data_his):
# 		with open (path_data_his,'r') as f:
# 			data = json.load(f)
# 		for i in data['HISTORY']:
# 			print ('==============================')
# 			MergerCampList(i, cursor)
# 	conn.commit()
# 	# print("Committed!.......")
# 	cursor.close()

# list_account = []
# end_date = '2017-08-31'
# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_GG/DATA'
# connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'
# InsertHistoryName(connect, path_data, list_account, end_date)




from googleads import adwords
import json
import os
import pandas as pd
from urllib.request import urlopen

PAGE_SIZE = 500

def SaveAccountTree(account, accounts, links, level, list_acc, list_mcc, list_mcc_id, list_dept, dept = None):
	"""Save an account tree.

	Args:
	account: dict The account to display.
	accounts: dict Map from customerId to account.
	links: dict Map from customerId to child links.
	level: int level of the current account in the tree.
	list_acc: list acc output get from API
	list_mcc: list id of mcc 
	list_mcc_id: list id of mcc account
	list_dept: list dept of mcc account
	dept: dept of current account
	"""
  
	if account['customerId'] in links:    
		if str(account['customerId']) in list_mcc_id:      
			dept = str(account['customerId'])

		for child_link in links[account['customerId']]:
			child_account = accounts[child_link['clientCustomerId']]     
		  
			print(type(child_account['name']))
			child_note = {
			          'customerId': child_account['customerId'],
			          'name': child_account['name'],
			          'level': level,
			          'children': [],
			          'deptId': dept,
			          'dept Name': list_mcc[list_mcc_id.index(dept)],
			          'dept': list_dept[list_mcc_id.index(dept)]
			}

			account['children'].append(child_note) 

			if child_note not in list_acc:
				list_acc.append(child_note)  
		       
			SaveAccountTree(child_note, accounts, links, level + 1, list_acc, list_mcc, list_mcc_id, list_dept, dept)

	if level == 1:     
		return (account, list_acc)
	  


def GetAllAcount():
  # Initialize appropriate service.
	adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/googleads.yaml')
	print("begin")
	managed_customer_service = adwords_client.GetService(
	'ManagedCustomerService', version='v201708')

	print("continue")
	html = urlopen("http://www.google.com/")
	print(html.read)
	print("ok")

  # Construct selector to get all accounts.
	offset = 0
	selector = {
	  	'fields': ['CustomerId', 'Name'],
	  	'paging': {
	      'startIndex': str(offset),
	      'numberResults': str(PAGE_SIZE)
	  }
	}
	more_pages = True
	accounts = {}
	child_links = {}
	parent_links = {}
	root_account = None

	while more_pages:
		print("start")
		managed_customer_service = adwords_client.GetService(
		'ManagedCustomerService', version='v201708')
		print(managed_customer_service)
		
		# Get serviced account graph.
		# print(managed_customer_service.get(selector))
		page = managed_customer_service.get(selector)
		if 'entries' in page and page['entries']:
		  # Create map from customerId to parent and child links.
			if 'links' in page:
				for link in page['links']:
					if link['managerCustomerId'] not in child_links:
						child_links[link['managerCustomerId']] = []
					child_links[link['managerCustomerId']].append(link)
					if link['clientCustomerId'] not in parent_links:
						parent_links[link['clientCustomerId']] = []
					parent_links[link['clientCustomerId']].append(link)
			# Map from customerID to account.
			for account in page['entries']:
				accounts[account['customerId']] = account
		offset += PAGE_SIZE
		selector['paging']['startIndex'] = str(offset)
		print (int(page['totalNumEntries']))    
		more_pages = offset < int(page['totalNumEntries'])

	# Find the root account.
	for customer_id in accounts:
		if customer_id not in parent_links:
			root_account = accounts[customer_id]

  # =================Get list dept of all account =========================

	path_dept = 'C:/Users/CPU10912-local/Desktop/Dept.xlsx'
	dept = pd.read_excel(path_dept)

	list_mcc = list(dept['MCC Level 3'])  
	list_mcc_id = list(dept['ID'])
	list_dept = list(dept['Dept'])
	for i in range(len(list_mcc_id)):
		list_mcc_id[i] = list_mcc_id[i].replace('-', '')
	list_mcc.append(None)
	list_mcc_id.append(None)
	list_dept.append(None)

  #===================Get account and store as tree =====================

  # Display account.
	list_acc = []  
	if root_account: 
		dept = None   
	if (str(root_account['customerId']) in list_mcc_id):      
		dept = str(root_account['customerId'])
	root_note = {
	          'customerId': root_account['customerId'],
	          'name': root_account['name'],
	          'level': 0,
	          'children': [],
	          'deptId': dept,
	          'dept Name': list_mcc[list_mcc_id.index(dept)],
	          'dept': list_dept[list_mcc_id.index(dept)]
	}
	list_acc.append(root_note)
	root_note = SaveAccountTree(root_note, accounts, child_links, 1, list_acc, list_mcc, list_mcc_id, list_dept, root_note['deptId'])
	return (root_note, list_acc)


# import _locale
# _locale._getdefaultlocale = (lambda *args: ['vi-VN', 'utf-8'])
file_json = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT/MCC_TEST.json'
# file_json = 'D:/WorkSpace/Adwords/Finanlly/AdWords/FULL_DATA/WPL.json'
root_note, list_acc = GetAllAcount()
with open(file_json, 'w') as fo:
	json.dump(root_note[1], fo)  #, ensure_ascii=False