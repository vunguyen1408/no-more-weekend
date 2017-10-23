import os
import pandas as pd


def check_data(p_data, p_excel):
	# =========== Read status data =============
	if os.path.exists(p_excel):
	  account = pd.read_excel(p_excel)

	  list_mcc = list(account['MCC'])  
	  list_mcc_id = list(account['MCC ID'])  
	  list_mcc_sub= list(account['MCC sub'])  
	  list_mcc_sub_id = list(account['MCC sub ID'])  
	  list_account_name = list(account['Account'])  
	  list_account = list(account['Account ID'])  
	  list_note = list(account['Note'])  
	  T3 = list(account['Thang 3'])
	  T4 = list(account['Thang 4'])
	  T5 = list(account['Thang 5'])
	  T6 = list(account['Thang 6'])
	  T7 = list(account['Thang 7'])
	  T8 = list(account['Thang 8'])
	  T9 = list(account['Thang 9'])
	  

	  # for i in range(len(list_acc)):
	  #   print(list_acc[i], "=======",T7[i],"=======", T8[i])


	#========= Check new data ==================

	###========== Get full list account ==============####	
	list_freq_T3 = []
	list_freq_T4 = []
	list_freq_T5 = []
	list_freq_T6 = []
	list_freq_T7 = []
	list_freq_T8 = []
	list_freq_T9 = []

	
	for i in range(len(list_account)):
		list_account[i] = str(list_account[i])
		list_freq_T3.append(0)
		list_freq_T4.append(0)
		list_freq_T5.append(0)
		list_freq_T6.append(0)
		list_freq_T7.append(0)
		list_freq_T8.append(0)
		list_freq_T9.append(0)


	list_date = next(os.walk(p_data))[1]
	for date in list_date:
		path_date = os.path.join(p_data, date + '/ACCOUNT_ID')
		
		list_acc = next(os.walk(path_date))[1]
		for acc in list_acc:		
			path_acc = os.path.join(path_date, acc)
			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '03'):
				list_freq_T3[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '04'):
				list_freq_T4[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '05'):
				list_freq_T5[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '06'):
				list_freq_T6[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '07'):
				list_freq_T7[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '08'):
				list_freq_T8[list_account.index(acc)] += 1

			if os.path.exists(path_acc + '/campaign_' + date +'.json') and (date[5:7] == '09'):
				list_freq_T9[list_account.index(acc)] += 1

	list_result = []
	for i in range(len(list_account)):	
		if (list_freq_T3[i] == 31):
			T3[i] = 'x'

		if (list_freq_T4[i] == 30):
			T4[i] = 'x'

		if (list_freq_T5[i] == 31):
			T5[i] = 'x'
			
		if (list_freq_T6[i] == 30):
			T6[i] = 'x'
		
		if (list_freq_T7[i] == 31):
			T7[i] = 'x'		

		if (list_freq_T8[i] == 31):
			T8[i] = 'x'

		if (list_freq_T9[i] == 30):
			T9[i] = 'x'

		list_result.append([list_mcc[i], list_mcc_id[i], list_mcc_sub[i], list_mcc_sub_id[i], 
			list_account_name[i], list_account[i], T3[i], T4[i], T5[i], T6[i], T7[i], T8[i], T9[i], list_note[i]])


	df = pd.DataFrame({
		'MCC': [i[0] for i in list_result],
		'MCC ID': [i[1] for i in list_result],
		'MCC sub': [i[2] for i in list_result],
		'MCC sub ID': [i[3] for i in list_result],
		'Account': [i[4] for i in list_result],
		'Account ID': [i[5] for i in list_result],
		'Thang 3': [i[6] for i in list_result],
		'Thang 4': [i[7] for i in list_result],
		'Thang 5': [i[8] for i in list_result],
		'Thang 6': [i[9] for i in list_result],
		'Thang 7': [i[10] for i in list_result],
		'Thang 8': [i[11] for i in list_result],
		'Thang 9': [i[12] for i in list_result],
		'Note': [i[13] for i in list_result]								
	})
				
		
	
	df = df[['MCC', 'MCC ID', 'MCC sub', 'MCC sub ID', 'Account', 'Account ID', 
	'Thang 3', 'Thang 4', 'Thang 5', 'Thang 6', 'Thang 7', 'Thang 8', 'Thang 9', 'Note']]  
		
	print ("=============================================")
	writer = pd.ExcelWriter(p_excel, engine='xlsxwriter')	
	df.to_excel(writer, sheet_name = 'Get_data', index=False)
	writer.save()
	print('Export file succeeded!...')  





p_data = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/TEMP_DATA'
p_excel = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/Check_list_data.xlsx'
check_data(p_data, p_excel)





