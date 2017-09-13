import cx_Oracle

conn = cx_Oracle.connect('MARKETING_TOOL_02/MARKETING_TOOL_02_9999@10.60.1.42:1521/APEX42DEV')
cursor = conn.cursor()

query = 'select * from MARKETING_TOOL_02.PYTHON_STG_TEST_01'
cursor.execute(query)

row = cursor.fetchall()
print (row)