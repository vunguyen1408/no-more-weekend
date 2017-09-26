import cx_Oracle
import json
from datetime import datetime , timedelta, date



def GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source, path_file):
	# ==================== Connect database =======================
	conn = cx_Oracle.connect(connect)
	cursor = conn.cursor()

	statement = "select * from ods_appsflyer where SNAPSHOT_DATE >= '" + start_date \
	+ "' and SNAPSHOT_DATE <= '" + end_date + "' and MEDIA_SOURCE = '" + media_source +  "'"
    
	
	cursor.execute(statement)

	list_install = cursor.fetchall()

	for i in list_install:
		d = str(i[0])
		temp = d.split('/')
		d = temp[2] + '-' + temp[0] + '-' + temp[1] 
		d = str(datetime.strptime(d, '%Y-%m-%d').date())
		i[0] = str(d)
	install = {}
	install['list_install'] = list_install
	with open(path_file, 'w') as f:
		json.dump(install, f)

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
start_date = '06/01/2017'
end_date = '06/30/2017'
media_source = 'googleadwords_int'
path_file = 'install.json'
GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source, path_file)