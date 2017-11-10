
"""
    Project : Online marketing tool - Audit content - Audit audio
    Company : VNG Corporation

    Description: Call gcloud speech API to get text for audio

    1. get folder by from date - to date
    1. convert video to audio by list hash


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

from hash_md5 import hash_md5

def do_work(list_index,in_queue, out_list):

    while True:
        item = in_queue.get()
        #line_no, line = item
        line_no, video = item

        # exit signal

        if 'None->Exit' in video :
            #print('exit')
            return

        # work
        time.sleep(.1)
        #file_video = os.path.join(path_video, video)

        for _file in list_index:

            if _file['video_index'] == line_no and 'video_renamed' not in video:

                # link = 'gs://python_video/' + folder + '/' + file_['name']
                #file_name = p_path_folder_work + '/' + _file['name']
                file_name=_file['full_name']
                new_file_name=_file['path'] + '/'+video['file_name']
                print("mv ", file_name," ",new_file_name)
                subprocess.call(["mv", file_name,new_file_name])
                video['video_renamed']=1

        result = (line_no, video)


        out_list.append(result)





def run_rename_downloaded_video(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'videos')
    folder_dest = os.path.join(folder_date, 'video_audios')


    file_source = os.path.join(folder_date, 'video_url_' + str(p_date) + '.json')
    file_dest = os.path.join(folder_date, 'test_video_url_' + str(p_date) + '.json')

    print(file_source)
    if os.path.exists(file_source):
        with open (file_source,'r') as _file:
            work_json = json.load(_file)
    else:
        print('exit')
        return

    list_index = []
    list_file = next(os.walk(folder_source))[2]

    for _file in list_file:
        #print(_file)
        #old name : YYYY-MM-DD-xxx.mp4
        if  len(str(_file)) <20:
            file_json = {
                'name': _file,
                'video_index': int(_file[11:-4]),
                'full_name': folder_source + '/' + _file,
                'path': folder_source
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
        p = Process(target=do_work, args=( list_index, work, results))
        p.start()
        pool.append(p)

    # produce data
    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
    #print(type(work_json))
    iters = itertools.chain(work_json['my_json'], ({'None->Exit'},)*num_workers)
    for num_and_line in enumerate(iters):
        work.put(num_and_line)

    for p in pool:
        p.join()

    return_json={}
    return_json['my_json']=[]

    for _json in results:
        return_json['my_json'].append(_json[1])

    with open (file_dest,'w') as _f:
        json.dump(return_json, _f)

    #return return_json


def rename_downloaded_video_date(p_base_dir, p_date ,p_process_num):
    run_rename_downloaded_video(p_base_dir,p_date,p_process_num)


def rename_downloaded_video_period(p_base_dir, p_from_date , p_to_date ,p_process_num):

    start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        rename_downloaded_video_date (p_base_dir, date,p_process_num)
        start += timedelta(1)


if __name__ == '__main__':
    from sys import argv

    parser = argparse.ArgumentParser(description='Convert Video to Audio in period')

    parser.add_argument('base_dir', metavar='base_dir', help='base_dir')

    parser.add_argument('from_date', metavar='from_date',
                        help='from_date')
    parser.add_argument('to_date', metavar='to_date',
                        help='to_date')
    parser.add_argument('process_num', metavar='process_num',
                        help='process_num')
    args = parser.parse_args()

    #p_base_dir = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, base_dir ,from_date, to_date, process_num = argv
    rename_downloaded_video_period( base_dir, from_date, to_date, process_num)
