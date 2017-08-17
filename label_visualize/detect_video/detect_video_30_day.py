import argparse
import sys
import os
import json
from datetime import datetime , timedelta, date
import time

from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)


def analyze_labels(path):

    list_label = []
    """ Detects labels given a GCS path. """
    video_client = (video_intelligence_service_client.
                    VideoIntelligenceServiceClient())

    features = [enums.Feature.LABEL_DETECTION]
    print ("==================================================")
    print (path)
    operation = video_client.annotate_video(path, features)

    print('\nProcessing video for label annotations:')

    while not operation.done():
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(20)

    print('\nFinished processing.')

    results = operation.result().annotation_results[0]
    for label in results.label_annotations:
        list_label.append(label.description)
        print (label.description)
    return list_label

def get_label_videos(folder, path_folder_videos, video_json):

    list_index = []
    list_file = next(os.walk(path_folder_videos))[2]
    for file_ in list_file:
        file_json = {
            'name': file_,
            'index': int(file_[11:-4])
        }
        list_index.append(file_json)

    for i, value in enumerate(video_json['my_json']):
        if not (len(value['video_label']) > 0):
            for file_ in list_index:
                if file_['index'] == i:
                    link = 'gs://python_video/' + folder + '/' + file_['name']
                    list_label = analyze_labels(link)
                    value['video_label'] = list(list_label)
    return video_json

def get_30_date(path_full_data, date, video_json):
    list_neighbor = []
    list_folder = next(os.walk(path))[1]

    delta = 31
    vdate = datetime.strptime(date, '%Y-%m-%d').date()

    list_video_json_before = []
    list_name = []
    json_count = 0

    #================ lay data truoc 30 ngay =============
    for i in range(int (delta)):
        single_date = vdate - timedelta(i)
        folder = os.path.join(path_full_data, single_date.strftime('%Y-%m-%d'))
        file_name = folder + '/' + 'video_url_'+ single_date.strftime('%Y-%m-%d') + '.json'
        if os.path.exists(file_name):
            with open (file_name,'r') as file_json:
                data = json.load(file_json)
                for value in data['my_json']:
                    if (len(value['video_label']) > 0) and (value['file_name'] not in list_name):
                        list_name.append(value['file_name'])
                        list_video_json_before.append(value)

    #============ Update data neu da ton tai=============
    for value in video_json['my_json']:
        for json_ in list_video_json_before:
            if (value['file_name'] == json_['file_name']) and (len(json_['video_label']) > 0):
                value['video_label'] = list(json_['video_label'])
    return (list_video_json_before, video_json)

def add_label_video_to_data(path, date_ = '2016-10-01', to_date_ = '2016-10-01'):
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
                with open (path_file_video,'r') as file_json:
                    video_json = json.load(file_json)
                    list_video_json_before, video_json = get_30_date(path, folder, video_json)
                    video_json = get_label_videos(folder, path_folder_videos, video_json)
                    print (video_json)
                    with open (path_file_video,'w') as f:
                        json.dump(video_json, f)
                # for folder in list_folder:
                path_folder = os.path.join(path, folder)
                path_file_videos = os.path.join(path_folder, 'video_url_' + str(folder) + '.json')
                path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
                if os.path.exists(path_file) and os.path.exists(path_file_videos):
                    with open(path_file, 'r') as f:
                        data = json.load(f)
                    with open(path_file_videos, 'r') as f:
                        data_video = json.load(f)
                    for vaule in data_video['my_json']:
                        i = vaule['index_json']
                        j = vaule['index_video']
                        if 'video_ids' in data['my_json'][i]['audit_content']:
                            print (vaule['video_label'])
                            data['my_json'][i]['audit_content']['video_ids'][j]['video_label'] = vaule['video_label']
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
    script, date, to_date = argv
    add_label_video_to_data(path, date, to_date)
