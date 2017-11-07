
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
        file_work_check = os.path.join(video['folder_dest'], file_name_check)

        file_name = video['hash_md5']+ '.wav'
        file_work = os.path.join(video['folder_dest'], file_name)

        file_name_2 = video['hash_md5']+ '.16.wav'
        file_work_2 = os.path.join(video['folder_dest'], file_name_2)

        file_name_3 = video['hash_md5']+ '.flac'
        file_work_3 = os.path.join(video['folder_dest'], file_name_3)

        file_name_4 = video['hash_md5']+ '.16.flac'
        file_work_4 = os.path.join(video['folder_dest'], file_name_4)


        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])



        if not os.path.exists(file_work):
            #subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "wav", file_work])
            #print (file_video)
            subprocess.call(["ffmpeg", "-i", file_video, file_work])

        if not os.path.exists(file_work_2):
            subprocess.call(["sox", file_work, "--channels=1", "--bits=16", file_work_2])

        if not os.path.exists(file_work_3):
            subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_work_3])

        if not os.path.exists(file_work_4):
            subprocess.call(["sox", file_work_3, "--channels=1", "--bits=16", file_work_4])



        result = (line_no, video)


        out_list.append(result)

def do_work_2(in_queue, out_list):

    while True:
        item = in_queue.get()
        #line_no, line = item
        line_no, line = item

        # exit signal

        if 'None->Exit' in line :
            #print('exit')
            return

        # work
        time.sleep(.1)
        #file_video = os.path.join(path_video, video)
        file_source = line['full_name']

        #file_name = video[0:video.rfind('.') ]+'_%03d' + '.png'
        #file_name_check =  video['hash_md5']+ '.wav'
        #file_work_check = os.path.join(video['folder_dest'], file_name_check)
        #file_name = video['name']
        #file_work = os.path.join(video['folder_dest'], file_name)

        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])


        #if os.path.exists(file_work):
        if 'audio_hash_md5' not in line:
            line['audio_hash_md5']=hash_md5(file_source)


        result = (line_no, line)


        out_list.append(result)


#def run_hash_audio(path_folder, path_file, folder):
def run_hash_audio(p_base_dir,p_date,p_process_num):


    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'video_audios')
    folder_dest = os.path.join(folder_date, 'video_audios')

    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)

    file_source = os.path.join(folder_date, 'video_hash_' + str(p_date) + '.json')
    file_dest = os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')


    if os.path.exists(file_source):
        with open (file_source,'r') as _file:
            source_json = json.load(_file)
    else:
        return


    # merge with file dest
    if  not os.path.exists(file_dest) or os.path.getsize(file_dest)==0 :
        #init


        for _json in source_json['hash_md5']:
            file_json={
                'video_hash_md5':_json['hash_md5'],
                'audio_name': _json['hash_md5']+ '.16.wav',
                'full_name': folder_source + '/' +  _json['hash_md5']+ '.16.wav',
                'folder_dest': folder_dest
                #'image_index': int(_file[-7:-4])
            }

            if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
                list_file.append(file_json)


    else:
        #update
        with open (file_dest,'r') as _file:
            dest_json = json.load(_file)

        for _json in source_json['hash_md5']:
            for _dest_json in dest_json['hash_md5']:
                found=-1

                if _dest_json['video_hash_md5']==_json['hash_md5'] :
                    #update
                    file_json = {
                        'video_hash_md5':_dest_json['video_hash_md5'],
                        'audio_name': _json['hash_md5']+ '.16.wav',
                        'audio_hash_md5':_dest_json['audio_hash_md5'],
                        'full_name': folder_source + '/' +  _json['hash_md5']+ '.16.wav'
                        #'image_index': int(_file[-7:-4])
                    }
                    if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
                        list_file.append(file_json)
                    break

                if found < 0 :
                    file_json = {
                        'video_hash_md5':_json['hash_md5'],
                        'audio_name': _json['hash_md5']+ '.16.wav',
                        'full_name': folder_source + '/' +  _json['hash_md5']+ '.16.wav',
                        'folder_dest': folder_dest
                        #'image_index': int(_file[-7:-4])
                    }
                    if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
                        list_file.append(file_json)

    #multiprocessing
    num_workers = int(p_process_num)

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=do_work_2, args=( work, results))
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
    return_json['hash_md5']=[]

    for _json in results:
        work_hash={}
        work_hash['video_hash_md5']=_json[1]['video_hash_md5']
        work_hash['audio_name']=_json[1]['audio_name']
        work_hash['audio_hash_md5']=_json[1]['audio_hash_md5']

        return_json['hash_md5'].append(work_hash)

    with open (file_dest,'w') as _file:
            json.dump(return_json, _file)

    #return return_json


def run_convert_video_to_audio(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'videos')
    folder_dest = os.path.join(folder_date, 'video_audios')
    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)

    file_source = os.path.join(folder_date, 'video_hash_' + str(p_date) + '.json')

    print(file_source)
    if os.path.exists(file_source):
        with open (file_source,'r') as _file:
            work_json = json.load(_file)
    else:
        print('exit')
        return


    for _json in work_json['hash_md5']:



        file_json = {
            'hash_md5':_json['hash_md5'],
            'source_name': _json['file_name'][0],
            'full_name': folder_source + '/' +  _json['file_name'][0],
            'folder_dest': folder_dest
            #'image_index': int(_file[-7:-4])
        }

        if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
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
    return_json['hash_md5']=[]

    for _json in results:
        return_json['hash_md5'].append(_json[1])

    return return_json


def convert_video_to_audio_date(p_base_dir, p_date ,p_process_num):
    run_convert_video_to_audio(p_base_dir,p_date,p_process_num)
    run_hash_audio(p_base_dir,p_date,p_process_num)


def convert_video_to_audio_period(p_base_dir, p_from_date , p_to_date ,p_process_num):

    start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        convert_video_to_audio_date (p_base_dir, date,p_process_num)
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
    convert_video_to_audio_period( base_dir, from_date, to_date, process_num)
