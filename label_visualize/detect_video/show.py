import argparse
import sys
import os
import json
from datetime import datetime , timedelta, date
import time

from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)



def add_label_video_to_data(path, date_, to_date_):
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
                    for value in video_json:
                        print ("============================================================================")
                        print (d)
                        print (value)



# path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# date_ = '2016-11-26'
# to_date_ = '2016-12-10'

if __name__ == '__main__':
    from sys import argv
    script, date, to_date = argv
    add_label_video_to_data(path, date, to_date)
