
import cx_Oracle
import json
from datetime import datetime , timedelta, date


# ==================== Connect database =======================

connect = 'MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV'

conn = cx_Oracle.connect(connect)
cursor = conn.cursor()

statement = 'select PRODUCT, REASON_CODE_ORACLE, \
			EFORM_TYPE, UNIT_OPTION, \
			USER_NAME, USER_CAMPAIGN_ID, \
			UPDATE_DATE, START_DATE, END_DATE from CAMPAIGN_MAP_HIS_GG'


cursor.execute(statement)
log_manual = cursor.fetchall()

print (log_manual)
log = log_manual[0]


print (log[7])
d = str(log[7])[:10]
d = str(datetime.strptime(d, '%Y-%m-%d').date())

print (d)
print (type(d))
