import sys
import json
import os
import time
from datetime import datetime , timedelta, date

path_old = 'C:/Users/ltduo/Desktop/VNG/DATA/ACCOUNT_ID'
path_new = 'C:/Users/ltduo/Desktop/VNG/DATA/END'



list_folder = next(os.walk(path_old))[1]

for account in list_folder:
	path_account =  os.path.join(path_old,  account)
	list_date = next(os.walk(path_account))[1]
	for date in list_date:
		path_file =  os.path.join(path_old, account + \
			'/' + date + '/' + 'campaign_' + date + '.json')
		if os.path.exists(path_file):
			#--------------- Chuyen data ---------------
			with open (path_file,'r') as f:
				data = json.load(f)

			path_folder_out =  os.path.join(path_new, date + '/ACCOUNT_ID/' + account)
			if not os.path.exists(path_folder_out):
				os.makedirs(path_folder_out)
			path_file_out = os.path.join(path_folder_out, 'campaign_' + date + '.json')
			if not os.path.exists(path_file_out):
				with open (path_file_out,'w') as f:
					json.dump(data, f)