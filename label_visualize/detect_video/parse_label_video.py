import argparse
import sys
import time
import os
import json
from datetime import datetime , timedelta, date
import time

def parse_label(path_file_video):
    with open(path_file_video, 'r') as f:
        data = json.load(f)
        for value in data['my_json']:
            if len(value['video_label']) > 0:
                result = value['video_label'][1:-1].split(",")
                list_result = []
                for i, label in enumerate(result):
                    label = label.replace("\\", "")
                    if i == 0:
                        label = label[1:-1]
                    else:
                        label = label[2:-1]
                    list_result.append(label)
                value['video_label'] = list(list_result)
    with open (path_file_video,'w') as f:
        json.dump(data, f)

def add_list_video_empty(data):
    for value in data['my_json']:
        for video in  value['audit_content']['video_ids']:
            video['video_label'] = []
    return data


def add_label_video_to_data(path, date_, to_date_, flag):
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    list_folder = next(os.walk(path))[1]
    #========================== Auto run ===================

    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    for folder in list_folder:
        d = datetime.strptime(folder, '%Y-%m-%d').date()

        if d <= to_date and d >= date:
            print (d)

            #==============================================
            path_folder = os.path.join(path, folder)
            path_folder_videos = os.path.join(path_folder, 'videos')
            path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
            path_file_video = os.path.join(path_folder, 'video_url_' + str(folder) + '.json')
            print (path_file)
            print (path_file_video)
            if os.path.exists(path_file) and os.path.exists(path_file_video):
                print ("===============================================")
                if flag:
                    parse_label(path_file_video)

                print ("===============================================")
                if os.path.exists(path_file) and os.path.exists(path_file_video):
                    with open(path_file, 'r') as f:
                        data = json.load(f)
                    with open(path_file_video, 'r') as f:
                        data_video = json.load(f)
                    data = add_list_video_empty(data)
                    for vaule in data_video['my_json']:
                        i = vaule['index_json']
                        j = vaule['index_video']
                        if 'video_ids' in data['my_json'][i]['audit_content']:
                            print (vaule)
                            data['my_json'][i]['audit_content']['video_ids'][j]['video_label'] = vaule['video_label']
                            print (data['my_json'][i])
                            print ("===============================================")

                    with open (path_file,'w') as f:
                        json.dump(data, f)


# path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# date_ = '2016-11-26'
# to_date_ = '2016-12-10'

if __name__ == '__main__':
    from sys import argv
    script, date, to_date, flag = argv
    add_label_video_to_data(path, date, to_date, flag)
