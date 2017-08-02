import os, os.path
#from os.path import splitext, basename, join
import io
import json
import time
import csv
from datetime import datetime , timedelta, date



def get_date(date_):
    date_ = date_[:10]
    date = date_[6] + date_[7] + date_[8] + date_[9] + '-' + date_[0] + date_[1] + '-' + date_[3] + date_[4]
    return date

def parse_csv_to_json_file_EMC(path_file):
    list_json = []
    list_ = []
    list_unique = []

    with open(path_file, 'r') as f:
        reader=csv.reader(f)
        for row in reader:
            list_.append(row)
        list_ = list(list_[1:])

        for row in list_:
            if row[5] not in list_unique:
                list_unique.append(row[5])
            flag = False
            for json_ in list_json:
                if row[0] == json_['event_id']:
                    flag = True
                    list_campaign = list(json_['list_campaign'])
                    campaign = row[6]
                    list_campaign. append(campaign)
                    json_['list_campaign'] = list(list_campaign)
                    break
            # Chua ton tai trong list_json
            if flag == False:
                list_campaign = []
                list_campaign.append(row[6])
                content = {
                    'event_id' : row[0],
                    'start_date' : get_date(row[3]),
                    'end_date' : get_date(row[4]),
                    'product' : row[5],
                    'list_campaign' : list(list_campaign)
                }
                list_json.append(content)
    return list_json

def parse_json_insight(path_insight, folder):
    # Lay tat ca noi dung cua cac file insight trong mot ngay, chuyen thanh 1 list Json
    folder_insight = os.path.join(path_insight, folder)
    list_folder_a_insight = next(os.walk(folder_insight))[1]
    data_insight = []
    if len(list_folder_a_insight) > 0:
        data_insight = '{ "my_json" :['
        for i in list_folder_a_insight:
            # Delete infor time
            temp = i[4:]
            insight = temp + "_insight.txt"
            folder_file_insight = os.path.join(folder_insight, i)
            file_insight = os.path.join(folder_file_insight, insight)
            if os.path.exists(file_insight):
                with open(file_insight) as f:
                    content = f.readlines()
                for row in content:
                    data_insight = data_insight + row + ','
        data_insight = data_insight[:len(data_insight) - 1] + ']}'
        data_insight = json.loads(data_insight)
    else:
         data_insight = '{ "my_json" :[]}'
    return data_insight

def add_content(list_json, path_audit_content, path_insight):
    list_folder_json_content = next(os.walk(path_audit_content))[1]
    for json_ in list_json: 
        start_date = datetime.strptime(json_['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(json_['end_date'], '%Y-%m-%d').date()
        print (start_date)
        print (end_date)
        print (json_['product'])    
        for folder in list_folder_json_content:
            date = datetime.strptime(folder, '%Y-%m-%d').date()
            # Trong mot ngay thuoc khoang
            print (folder)
            if date >= start_date and date <= end_date:
                # Lay thong tin file audit content
                folder_audit = os.path.join(path_audit_content, folder)
                audit_content = "ads_creatives_audit_content_"+ folder +".json"
                path_file_audit_content = os.path.join(folder_audit, audit_content)
                if os.path.exists(path_file_audit_content):
                    with open(path_file_audit_content, 'r') as f_json:
                        data_json = json.load(f_json)
                    # Lay tat ca noi dung cua cac file insight trong mot ngay, chuyen thanh 1 list Json
                    data_insight = parse_json_insight(path_insight, folder)
                    # Duyet de kiem tra va them thong tin product vao cac audit_content
                    product = []
                    for j in data_json['my_json']:
                        flag = True
                        list_product = []
                        # Find in data_insight of date
                        for k in data_insight['my_json']:
                            if str(j['ad_id']) == str(k['ad_id']):
                                # Neu campaign_id ton tai trong "Event" dang duyet thi them id product
                                for campaign in json_['list_campaign']:
                                    if campaign == k['campaign_id']:
                                        flag = False
                                        list_product =  list(j['list_product'])
                                        if json_['product'] not in list_product:
                                            list_product.append(str(json_['product']))
                                        j['list_product'] = list(list_product)
                    with open (path_file_audit_content,'w') as f_out:
                        json.dump(data_json,f_out)
            print ("==================================================================")
        

def group_by_product(path_audit_content):
    list_folder = next(os.walk(path_audit_content))[1]
    list_unique_product = []
    for folder in list_folder:
        folder_audit = os.path.join(path_audit_content, folder)
        audit_content = "ads_creatives_audit_content_"+ folder +".json"
        path_file_audit_content = os.path.join(folder_audit, audit_content)
        if os.path.exists(path_file_audit_content):
            with open(path_file_audit_content, 'r') as f_json:
                data_json = json.load(f_json)
            print (folder)
            for j in data_json['my_json']:
                try:
                    list_product = j['list_product']
                    for p in list_product:
                        if p not in list_unique_product:
                            list_unique_product.append(p)
                except KeyError as e:
                    print ("-")
            print (list_unique_product)
            print ("==========================================")
    for i in list_unique_product:
        print (i)

def compare(path_audit_content, path_insight):
    list_folder = next(os.walk(path_audit_content))[1]
    list_not_compare = []

    for folder in list_folder:
        num = 0
        folder_audit = os.path.join(path_audit_content, folder)
        audit_content = "ads_creatives_audit_content_"+ folder +".json"
        path_file_audit_content = os.path.join(folder_audit, audit_content)
        if os.path.exists(path_file_audit_content):
            with open(path_file_audit_content, 'r') as f_json:
                data_json = json.load(f_json)
                data_insight = parse_json_insight(path_insight, folder)
                for j in data_json['my_json']:
                    for k in data_insight['my_json']:
                        if str(j['ad_id']) == str(k['ad_id']):
                            num += 1
                            break

            list_not_compare.append([folder, len(data_json['my_json']), num, len(data_json['my_json']) - num])
    path_out = 'C:/Users/CPU10145-local/Desktop/audit_contet_insight.csv'
    with open(path_out, 'w+', newline="") as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerow(['date', 'number json', 'number json finded', 'miss'])
        wr.writerows(list_not_compare)

def add_list(path_audit_content):
    list_folder = next(os.walk(path_audit_content))[1]
    for folder in list_folder:
        print (folder)
        folder_audit = os.path.join(path_audit_content, folder)
        audit_content = "ads_creatives_audit_content_"+ folder +".json"
        path_file_audit_content = os.path.join(folder_audit, audit_content)
        if os.path.exists(path_file_audit_content):
            with open(path_file_audit_content, 'r') as f_json:
                data_json = json.load(f_json)
                for j in data_json['my_json']:
                    j['list_product'] = []
                with open (path_file_audit_content,'w') as f_out:
                    json.dump(data_json,f_out)



path_audit_content = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path_insight = 'D:/DATA_CHECK/MARKETING_TOOL_02'
path_file_event_map_campaign = 'D:/DATA_CHECK/EVENT_MAP_CAMPAIGN.txt'

# path_audit_content = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path_insight = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02'
# path_file_event_map_campaign = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02/EXPORT_DATA/EVENT_MAP_CAMPAIGN.txt'

# path_audit_content = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# path_insight = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02'
# path_file_event_map_campaign = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02/EXPORT_DATA/EVENT_MAP_CAMPAIGN.txt'

add_list(path_audit_content)
list_json = parse_csv_to_json_file_EMC(path_file_event_map_campaign)
add_content(list_json, path_audit_content, path_insight)

# statistic(path_audit_content)
# group_by_product(path_audit_content)
# compare(path_audit_content, path_insight)
# add_list(path_audit_content)
