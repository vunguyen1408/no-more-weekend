import json
import os

def addAccName(path_data, list_mcc, list_mcc_id, list_dept):
	list_date = next(os.walk(path_data))[1]

	for date in list_date:
		path_temp = os.path.join(path_data, date + '/ACCOUNT_ID')
		list_acc = next(os.walk(path_temp))[1]		
		for acc in list_acc:
			path_temp_1 = os.path.join(path_temp, acc)
			list_file = next(os.walk(path_temp_1))[2]
			for file in list_file:
				if (file[0:9] == 'campaign_'):
					path_file = os.path.join(path_temp_1, file)
					with open(path_file, 'r') as fi:
						data = json.load(fi)
					for value in data:
						value['Account Name'] = list_mcc[list_mcc_id.index(value['Account ID'])]
						value['Dept'] = list_dept[list_mcc_id.index(value['Account ID'])]
					with open(path_file, 'w') as fo:
						json.dump(data, fo)
						# print(acc, file)



def get_list_customer(path_data):
	path_mcc = os.path.join(path_data, 'MCC.json')
	path_wpl = os.path.join(path_data, 'WPL.json')

	list_mcc = []
	list_mcc_id = []
	list_dept  = []

	with open(path_mcc, 'r') as fi:
		data = json.load(fi)
	for value in data:
		list_mcc.append(value['name'])
		list_mcc_id.append(str(value['customerId']))
		list_dept.append(value['dept'])

	with open(path_wpl, 'r') as fi:
		data = json.load(fi)
	for value in data:
		list_mcc.append(value['name'])
		list_mcc_id.append(str(value['customerId']))
		list_dept.append(value['dept'])

	return list_mcc_id, list_mcc, list_dept



path_data = 'D:/WorkSpace/Adwords/Finanlly/AdWords/FULL_DATA'
list_mcc_id, list_mcc, list_dept = get_list_customer(path_data)
print(len(list_mcc))
print(len(list_mcc_id))
path_data = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEMP_DATA_T3_T9'
addAccName(path_data, list_mcc, list_mcc_id, list_dept)
