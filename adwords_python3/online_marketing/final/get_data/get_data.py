import get_accounts as get_accounts
import download_report as download_report
import add_acc_name as add_acc_name

import os
import time


def get_data_all_account(path_acc, path_camp, path_log, path_config, startDate, endDate):
	#========================================== Get data for MCC account =========================================================
	path_dept = os.path.join(path_acc, 'Dept.xlsx')
	p_config = os.path.join(path_config, 'googleads_MCC.yaml')
	file_json = os.path.join(path_acc, 'MCC.json')
	root_note, list_acc = get_accounts.GetAllAcount(p_config)
	with open(file_json, 'w') as fo:
		json.dump(root_note[1], fo)

	list_mcc_id, list_mcc, list_dept = add_acc_name.get_customer(file_json)	
	for customer_id in list_mcc_id:    
		download_report.GetCampainForAccount(path_camp, path_config, customer_id, startDate, endDate, path_log, list_mcc, list_mcc_id, list_dept)
		time.sleep(1)


	#========================================== Get data for WPL account ==========================================================	
	p_config = os.path.join(path_config, 'googleads_WPL.yaml')
	file_json = os.path.join(path_acc, 'WPL.json')
	root_note, list_acc = get_accounts.GetAllAcount(p_config)
	with open(file_json, 'w') as fo:
		json.dump(root_note[1], fo)

	list_mcc_id, list_mcc, list_dept = add_acc_name.get_customer(file_json)
	for customer_id in list_mcc_id:    
		download_report.GetCampainForAccount(path_camp, path_config, customer_id, startDate, endDate, path_log, list_mcc, list_mcc_id, list_dept)
		time.sleep(1)



def get_all_account(path_acc, path_camp, path_log, path_config, startDate, endDate):
	#========================================== Get data for MCC account =========================================================
	path_dept = os.path.join(path_acc, 'Dept.xlsx')
	p_config = os.path.join(path_config, 'googleads_MCC.yaml')
	file_json = os.path.join(path_acc, 'MCC.json')
	root_note, list_acc = get_accounts.GetAllAcount(p_config)
	with open(file_json, 'w') as fo:
		json.dump(root_note[1], fo)	


	#========================================== Get data for WPL account ==========================================================	
	p_config = os.path.join(path_config, 'googleads_WPL.yaml')
	file_json = os.path.join(path_acc, 'WPL.json')
	root_note, list_acc = get_accounts.GetAllAcount(p_config)
	with open(file_json, 'w') as fo:
		json.dump(root_note[1], fo)

	list_mcc_id, list_mcc, list_dept = add_acc_name.get_list_customer(path_acc)	
	return list_mcc_id



def get_all_camp(path_acc, path_camp, path_log, path_config, startDate, endDate):
	#========================================== Get data for MCC account =========================================================	
	p_config = os.path.join(path_config, 'googleads_MCC.yaml')
	file_json = os.path.join(path_acc, 'MCC.json')

	list_mcc_id, list_mcc, list_dept = add_acc_name.get_customer(file_json)	

	for customer_id in list_mcc_id:    
		download_report.GetCampainForAccount(path_camp, path_config, customer_id, startDate, endDate, path_log, list_mcc, list_mcc_id, list_dept)
		time.sleep(1)


	#========================================== Get data for WPL account ==========================================================	
	p_config = os.path.join(path_config, 'googleads_WPL.yaml')
	file_json = os.path.join(path_acc, 'WPL.json')

	list_mcc_id, list_mcc, list_dept = add_acc_name.get_customer(file_json)

	for customer_id in list_mcc_id:    
		download_report.GetCampainForAccount(path_camp, path_config, customer_id, startDate, endDate, path_log, list_mcc, list_mcc_id, list_dept)
		time.sleep(1)

	





# path_acc = '/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/final/LIST_ACCOUNT'
# path_camp = 
# path_log = 
# path_config = '/home/marketingtool/Workspace/Python/enviroment'
# startDate = '2017-03-01' 
# endDate = '2017-03-01'

# list_acc = get_all_account(path_acc, path_camp, path_log, path_config, startDate, endDate)
# list_acc, list_mcc, list_dept = add_acc_name.get_list_customer(path_acc)
# get_all_camp(path_acc, path_camp, path_log, path_config, startDate, endDate)




path_acc = 'D:/WorkSpace/Adwords/Finanlly/AdWords/FULL_DATA'
path_camp = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEST'
path_log = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/log.txt' 
path_config = 'D:/WorkSpace/GG_Tool/Finally/no-more-weekend/adwords_python3/online_marketing/final'
startDate = '2017-10-26' 
endDate = '2017-10-26'

startTime = time.time()
print('================== START TIME =======================')
print(startTime)
print('===========================================================')

# list_acc = get_all_account(path_acc, path_camp, path_log, path_config, startDate, endDate)
list_acc, list_mcc, list_dept = add_acc_name.get_list_customer(path_acc)	
get_all_camp(path_acc, path_camp, path_log, path_config, startDate, endDate)


endTime = time.time()

print('================== TOTAL TIME DAILY =======================')
print("Total time for daily: ", endTime - startTime)

