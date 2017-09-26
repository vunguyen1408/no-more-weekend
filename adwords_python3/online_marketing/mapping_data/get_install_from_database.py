import cx_Oracle
import json
from datetime import datetime , timedelta, date



def GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source, path_file):
    # ==================== Connect database =======================
    conn = cx_Oracle.connect(connect)
    cursor = conn.cursor()

    statement = statement = "select * from ods_appsflyer where SNAPSHOT_DATE >= to_date('" + start_date \
    + "', 'mm/dd/yyyy') and SNAPSHOT_DATE <= to_date('" + end_date + "', 'mm/dd/yyyy') and MEDIA_SOURCE = '" + media_source +  "'"

    cursor.execute(statement)

    list_install = cursor.fetchall()
    list_out = []
    for i in list_install:
        d = str(i[0])[:10]
        d = str(datetime.strptime(d, '%Y-%m-%d').date())
        temp = []
        temp.append(d)
        temp.append(i[1])
        temp.append(i[2])
        temp.append(i[3])
        temp.append(i[4])
        temp.append(i[5])
        temp.append(i[6])
        list_out.append(temp)
        print (temp)
    install = {}
    install['list_install'] = list_out
    with open(path_file, 'w') as f:
        json.dump(install, f)

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'
start_date = '06/01/2017'
end_date = '06/30/2017'
media_source = 'googleadwords_int'
path_file = 'install.json'
GetDataSummaryAppsFlyer(connect, start_date, end_date, media_source, path_file)
