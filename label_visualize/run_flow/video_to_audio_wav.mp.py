
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

def do_work(in_queue, out_list):

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
        file_video = video['full_name']

        #file_name = video[0:video.rfind('.') ]+'_%03d' + '.png'
        file_name_check =  video['hash_md5']+ '.wav'
        file_work_check = os.path.join(video['path'], file_name_check)
        file_name = video['hash_md5']+ '.wav'
        file_work = os.path.join(video['path'], file_name)

        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])

        if not os.path.exists(file_work):
            subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "wav", file_work])

        if os.path.exists(file_work):
            subprocess.call(["sox", file_work, "--channels=1", "--bits=16", file_work])


        result = (line_no, video)


        out_list.append(result)





def run_convert_video_to_audio(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_work = os.path.join(folder_date, 'videos')

    file_work = os.path.join(folder, 'video_hash_' + str(p_date) + '.json')


    if os.path.exists(file_work):
        with open (file_work,'r') as _file:
            work_json = json.load(_file)


    for _json in work_json['hash_md5']:
        file_json = {
            'hash_md5':_json['hash_md5']
            'name': _json['file_name'][0],
            'full_name': folder_work + '/' +  _json['file_name'][0],
            'path': p_folder_work
            #'image_index': int(_file[-7:-4])
        }
        list_file.append(file_json)

    #multiprocessing
    num_workers = int(p_process_num)

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=do_work, args=( work, results))
        p.start()
        pool.append(p)

    # produce data
    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
    #print(type(work_json))
    iters = itertools.chain(list_file, ({'None->Exit'},)*num_workers)
    for num_and_line in enumerate(iters):
        work.put(num_and_line)

    for p in pool:
        p.join()

    return_json={}
    return_json['my_json']=[]

    for _json in results:
        return_json['my_json'].append(_json[1])

    return return_json


def convert_video_to_audio_date(p_base_dir, p_date = '2016-10-01',p_process_num):
    run_convert_video_to_audio(p_base_dir,p_date,p_process_num)


def convert_video_to_audio_period(p_base_dir, p_from_date = '2016-10-01', p_to_date = '2016-10-01',p_process_num):
	start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
	end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

	while(start <= end):
		date = os.path.join(p_base_dir, start.strftime('%Y-%m-%d'))
        convert_video_to_audio_date (p_base_dir, date,p_process_num)
		start += timedelta(1)



if __name__ == '__main__':
    from sys import argv

    parser = argparse.ArgumentParser(description='Convert Video to Audio in period')
    parser.add_argument('base_dir', metavar='base_dir',
                        help='base_dir')
    parser.add_argument('from_date', metavar='from_date',
                        help='from_date')
    parser.add_argument('to_date', metavar='to_date',
                        help='to_date')
    parser.add_argument('process_num', metavar='process_num',
                        help='process_num')
    args = parser.parse_args()

    #g_path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, from_date, to_date, process_num = argv
    convert_video_to_audio_period(base_dir, from_date, to_date, process_num)
