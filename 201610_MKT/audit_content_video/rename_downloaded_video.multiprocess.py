
"""
    Project : Online marketing tool - Audit content - Audit audio
    Company : VNG Corporation

    Description: Call gcloud speech API to get text for audio

    Examples of Usage:
        python detect_audio.py 2016-10-01 2017-06-29
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function




import argparse
import io
import sys
import os
import json
import subprocess
from datetime import datetime , timedelta, date
import time

from multiprocessing import Process, Manager
import itertools


def do_work(list_index,in_queue, out_list):


    while True:
        item = in_queue.get()
        #line_no, line = item
        _i,_value = item
        #print ('_i:',_i,' _value:',_value)

        # exit signal
        #if line == None:
        #print(line)
        if 'None->Exit' in _value :
            #print('exit')
            return

        # work
        time.sleep(.1)
        #loop 2
        for _file in list_index:

            if _file['video_index'] == _i and 'video_renamed' not in _value:

                # link = 'gs://python_video/' + folder + '/' + file_['name']
                #file_name = p_path_folder_work + '/' + _file['name']
                file_name=_file['full_name']
                new_file_name=_file['path'] + '/'+_value['file_name']
                print("mv ", file_name," ",new_file_name)
                subprocess.call(["mv", file_name,new_file_name])
                _value['video_renamed']=1

        #print(result)
        result = (_i, _value)

        out_list.append(result)






def rename_video(p_folder, p_path_folder_work, p_work_json, p_process_num):
    print(len(p_work_json['my_json']))
    #list file
    list_index = []
    list_file = next(os.walk(p_path_folder_work))[2]

    for _file in list_file:
        print(_file)

        #YYYY-MM-DD-xxx.mp4
        if  len(str(_file)) <20:
            file_json = {
                'name': _file,
                'video_index': int(_file[11:-4]),
                'full_name': p_path_folder_work + '/' + _file,
                'path': p_path_folder_work
                #'image_index': int(_file[-7:-4])
            }
            list_index.append(file_json)

    #multiprocessing
    num_workers = int(p_process_num)

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=do_work, args=(list_index, work, results))
        p.start()
        pool.append(p)

    # produce data
    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
    #print(type(work_json))
    iters = itertools.chain(p_work_json['my_json'], ({'None->Exit'},)*num_workers)
    for num_and_line in enumerate(iters):
        work.put(num_and_line)

    for p in pool:
        p.join()

    return_json={}
    return_json['my_json']=[]

    for _json in results:
        return_json['my_json'].append(_json[1])

#    return p_work_json
    return return_json



def rename_downloaded_video(p_path='/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON', p_from_date = '2016-10-01', p_to_date = '2016-10-01',p_process_num):
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    list_folder = next(os.walk(p_path))[1]

    #========================== Auto run ===================

    date = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    to_date = datetime.strptime(p_to_date, '%Y-%m-%d').date()
    for _folder in list_folder:
        d = datetime.strptime(_folder, '%Y-%m-%d').date()
        if d <= to_date and d >= date:
            # print (d)

            #==============================================
            #base folder
            path_folder = os.path.join(p_path, _folder)
            path_folder_work = os.path.join(path_folder, 'videos')
            #dest file
            path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(_folder) + '.json')
            path_file_work = os.path.join(path_folder, 'video_url_' + str(_folder) + '.json')
            # print (path_file)
            # print (path_file_video)
            if os.path.exists(path_file) and os.path.exists(path_file_work):

                #cap nhat video_json
                with open (path_file_work,'r') as _file_json:
                    work_json = json.load(_file_json)
                    # video_json = get_label_videos(folder, path_folder_audios, video_json)
                    #list_json_before, work_json = get_30_date(p_path, _folder, work_json)
                    work_json = rename_video(_folder, path_folder_work, work_json,p_process_num)
                    with open (path_file_work,'w') as _f:
                        json.dump(work_json, _f)


# path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
# path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# path = 'D:/DATA/NEW_DATA_10-2016_05-2017/FULL_DATA_10-2016_06-2017/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# date_ = '2016-11-26'
# to_date_ = '2016-12-10'

if __name__ == '__main__':
    from sys import argv
    g_path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, date, to_date, process_num = argv
    rename_downloaded_video(g_path, date, to_date, process_num)
