
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
import os,glob
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

        file_image_check=video['folder_dest']+ '/' + video['hash_md5']+'.*' + '.png'
        file_image=video['folder_dest']+ '/' + video['hash_md5']+'.%03d' + '.png'

        if not glob.glob(file_image_check):
            subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])



        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])



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


        #file_name = video[0:video.rfind('.') ]+'_%03d' + '.png'
        #file_name_check =  video['hash_md5']+ '.wav'
        #file_work_check = os.path.join(video['folder_dest'], file_name_check)
        #file_name = video['name']
        #file_work = os.path.join(video['folder_dest'], file_name)

        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])


        #if os.path.exists(file_work):
        if 'video_image_hash_md5' not in line:
            file_source = line['full_name']
            line['video_image_hash_md5']=hash_md5(file_source)
            line['video_image_name']=os.path.basename(file_source)


        result = (line_no, line)


        out_list.append(result)


#def run_hash_audio(path_folder, path_file, folder):
def run_hash_video_image(p_base_dir,p_date,p_process_num):


    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'video_images')
    folder_dest = os.path.join(folder_date, 'video_images')

    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)

    file_source = os.path.join(folder_date, 'video_hash_' + str(p_date) + '.json')
    file_dest = os.path.join(folder_date, 'video_image_hash_' + str(p_date) + '.json')
    file_dest_test = os.path.join(folder_date, 'test_video_image_hash_' + str(p_date) + '.json')


    if os.path.exists(file_source):
        with open (file_source,'r') as _file:
            source_json = json.load(_file)
    else:
        return


    # merge with file dest
    if  not os.path.exists(file_dest) or os.path.getsize(file_dest)==0 :
        #init
        #print('init')

        for _json in source_json['hash_md5']:

            #append all file available
            file_image_check=folder_source+ '/' + _json['hash_md5']+'.*' + '.png'
            list_image = glob.glob(file_image_check)
            for _file in list_image:
	            file_json={
	                'video_hash_md5':_json['hash_md5'],
	                'full_name': _file
	            }

	            if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
	                list_file.append(file_json)


    else:
        #update
        #print('update')
        with open (file_dest,'r') as _file:
            dest_json = json.load(_file)



        for _json in source_json['hash_md5']:

            #print(_json)
            found=[]
            for _i,_dest_json in enumerate(dest_json['hash_md5']):
                #print(_dest_json)
                if _dest_json['video_hash_md5']==_json['hash_md5'] and 'video_image_hash_md5' in _dest_json:
                    found.append(_i)

            if len(found) ==  0 :

                # add all file
                file_image_check=folder_dest+ '/' + _json['hash_md5']+'.*' + '.png'
                list_image = glob.glob(file_image_check)

                for _file in list_image:
                    file_json={
                        'video_hash_md5':_json['hash_md5'],
                        'full_name': _file
                        }

                    if os.path.exists(file_json['full_name']) and os.path.getsize(file_json['full_name']) >0:
                        list_file.append(file_json)


            else:

                #skip all data

                for _i in found:
                    file_json=dest_json['hash_md5'][_i]
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


    list_result = []

    #
    print(len(results))

    for _json in results:

        temp_json=_json[1]

        find_hash = -1
        #group by video_image_hash_md5
        for _i,_hash in enumerate(list_result):
            #print(_i)

            #group by video_image_hash_md5
            if _hash['video_image_hash_md5']==temp_json['video_image_hash_md5']:

                #file exist then stop  looking
                find_file=-1
                for _file  in list_result[_i].get('video_image_name',[]):
                    if _file==temp_json['video_image_name']:
                        find_file=1
                        break

                #file not exist then add to list_result
                if find_file < 0:
                #append
                    list_result[_i]['video_image_name'].append(temp_json['video_image_name'])

                find_hash=_i
                break

        if find_hash <0:

            new_hash=temp_json
            #remove key unnecessary
            new_hash.pop('full_name', None)
            list_result.append(new_hash)


    return_json['hash_md5']=list_result

    #print(return_json)
    with open (file_dest,'w') as _file:
            json.dump(return_json, _file)

    #return return_json



def run_convert_video_to_image(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'videos')
    folder_dest = os.path.join(folder_date, 'video_images')
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


    #print(len( work_json['hash_md5']))
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


def convert_video_to_image_date(p_base_dir, p_date ,p_process_num):
    #run_convert_video_to_image(p_base_dir,p_date,p_process_num)
    run_hash_video_image(p_base_dir,p_date,p_process_num)


def convert_video_to_image_period(p_base_dir, p_from_date , p_to_date ,p_process_num):

    start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        convert_video_to_image_date (p_base_dir, date,p_process_num)
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
    convert_video_to_image_period( base_dir, from_date, to_date, process_num)
