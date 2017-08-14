import argparse
import sys
import time
import os
import json

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

def get_label_videos(path, path_folder_videos):
    # Lấy danh sách path của các file json cần tổng hợp data

    list_link = []
    list_file = next(os.walk(path_folder_videos))[2]
    date = ""
    for file in list_file:
        date = file[:10]
        link = 'gs://python_video/' + file[:10] + '/' + file
        link_json = {
            'link': link,
            'index': int(file[11:-4])
        }
        list_link.append(link_json)
        

    path_file = os.path.join(path, (str(date) + '/video_url_' + str(date) + '.json'))
    with open(path_file, 'r') as f:
        data = json.load(f)
    print  (data['my_json'][0])
    # Get label
    for link in list_link:
        list_label = analyze_labels(link['link'])
        index = link['index']
        data['my_json'][index]['video_label'] = str(list_label)
        file_name = data['my_json'][index]['file_name']
        for value in data['my_json']:
            if value['file_name'] == file_name:
                value['video_label'] = str(list_label)
    with open (path_file,'w') as f:
        json.dump(data, f)

def add_label_video_to_data(path, folde):
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    list_folder = next(os.walk(path))[1]
    #========================== Auto run ===================
    from datetime import datetime , timedelta, date
    import time
    date_ = '2016-11-16'
    to_date_ = '2016-11-25'
    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    for folder in list_folder:
        d = datetime.strptime(folder, '%Y-%m-%d').date()
        
        if d <= to_date and d >= date:
            print (d)
        #     #==============================================
        #     path_folder = os.path.join(path, folder)
        #     path_folder_videos = os.path.join(path_folder, 'videos')
        #     path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
        #     if os.path.exists(path_file):
        #         get_label_videos(path, path_folder_videos)

        #     # for folder in list_folder:
        #     path_folder = os.path.join(path, folder)
        #     path_file_videos = os.path.join(path_folder, 'video_url_' + str(folder) + '.json')
        #     path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
        #     if os.path.exists(path_file) and os.path.exists(path_file_videos):
        #         with open(path_file, 'r') as f:
        #             data = json.load(f)
        #         with open(path_file_videos, 'r') as f:
        #             data_video = json.load(f)
        #         for vaule in data_video['my_json']:
        #             i = vaule['index_json']
        #             j = vaule['index_video']
        #             if 'video_ids' in data['my_json'][i]['audit_content']:
        #                 data['my_json'][i]['audit_content']['video_ids'][j]['video_label'] = vaule['video_label']
        #                 print (data['my_json'][i])
        #                 print ("===============================================")
                    
        #         with open (path_file,'w') as f:
        #             json.dump(data, f)


# path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'


path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
add_label_video_to_data(path, '2016-10-04')

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('pdate', help='The date you\'d like to label.')
#     args = parser.parse_args()
#     g_vdate=args.pdate
#     print(g_vdate)
#     add_label_video_to_data(path, g_vdate)
