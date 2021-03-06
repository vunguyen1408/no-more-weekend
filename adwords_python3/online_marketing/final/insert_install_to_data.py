import sys
import os
import pandas as pd
import cx_Oracle
import numpy as np
import json
from datetime import datetime , timedelta, date


def GetDataSummaryAppsFlyer(connect, date, media_source1, media_source2, path_file):
    # ==================== Connect database =======================
    conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
    cursor = conn.cursor()

    day = date[8:]
    month = date[5:-3]
    year = date[:4]
    date = month + '/' + day + '/' + year
    statement = "select * from ods_appsflyer where SNAPSHOT_DATE \
    = to_date('" + date + "', 'mm/dd/yyyy') and (MEDIA_SOURCE like '" + media_source1 +  "' or MEDIA_SOURCE like '" + media_source2 +  "')"

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
    install = {}
    install['list_install'] = list_out
    with open(path_file, 'w') as f:
        json.dump(install, f)


def ReadDataInstall(path_file_install):
    with open(path_file_install, 'r') as f:
        data = json.load(f)
        # print ("==========================doc duoc file install====================")
        return data['list_install']
        
def AddInstall(path_file, list_install):
    with open(path_file, 'r') as f:
        data = json.load(f)

    for campaign in data:
        if 'INSTALL_CAMP' not in campaign:
            campaign['INSTALL_CAMP'] = 0
            for install in list_install:
                if str(campaign['Campaign ID']) == str(install[2]):
                    campaign['INSTALL_CAMP'] = install[3]
    with open(path_file, 'w') as f:
        json.dump(data, f)

def InsetInstallToDate(path_data, list_install, list_customer_id, date):

    #------------ Creat list install on date ---------------
    list_install_date = []
    for install in list_install:
        if install[0] == date:
            list_install_date.append(install)

    path_list_account_id = os.path.join(path_data, str(date) + '/ACCOUNT_ID')
    # list_folder_account = next(os.walk(path_list_account_id))[1]
    list_folder_account = list_customer_id

    for account in list_folder_account:
        path_account_id = os.path.join(path_list_account_id, account)
        if os.path.exists(path_account_id):
            path_file = os.path.join(path_account_id, 'campaign_' + str (date) + '.json')
            if os.path.exists(path_file):
                AddInstall(path_file, list_install_date)


def InsetInstall(path_data, path_file_install, start_date, end_date):
    startDate = datetime.strptime(start_date, '%Y-%m-%d').date()  
    endDate = datetime.strptime(end_date, '%Y-%m-%d').date() 
    date = startDate

    while(date <= endDate):
        # print ("===========================================================")
        # print (date)
        list_install = ReadDataInstall(path_file_install)
        InsetInstallToDate(path_data, list_install, str(date))
        date = date + timedelta(1)

def RunInsertInstall(connect, path_data, list_customer_id, date):
    media_source1 = 'googleadwords_int'
    media_source2 = 'googleadwords_sem'
    path_folder_appsflyer = os.path.join(path_data, str(date) + '/APPS_FLYER')
    if not os.path.exists(path_folder_appsflyer):
        os.makedirs(path_folder_appsflyer)
    path_file = os.path.join(path_folder_appsflyer, 'install.json')

    GetDataSummaryAppsFlyer(connect, str(date), media_source1, media_source2, path_file)
    list_install = ReadDataInstall(path_file)
    InsetInstallToDate(path_data, list_install, list_customer_id, date)
    # print ("================= Insert install campleted ===================")



# startDate = '2017-06-01'
# endDate = '2017-06-01'
# path_data = 'C:/Users/ltduo/Desktop/VNG/DATA/END'
# path_file_install = 'C:/Users/ltduo/Desktop/VNG/DATA/APP_FLYER/install.json'
# connect = ''
# RunInsertInstall(connect, path_data, startDate)