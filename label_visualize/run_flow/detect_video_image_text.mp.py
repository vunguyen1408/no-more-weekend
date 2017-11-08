
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


from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)

from google.cloud import vision
from google.cloud.vision import types



def detect_text(p_path):
    """Detects text in the file."""
    client = vision.ImageAnnotatorClient()

    # [START migration_text_detection]
    with io.open(p_path, 'rb') as image_file:
        content = image_file.read()

    image = types.Image(content=content)

    response = client.text_detection(image=image)
    texts = response.text_annotations
    #print('Texts:')

    list_text_info=[]

    for text in texts:



        #print('\n"{}"'.format(text.description))

        vertices = (['({},{})'.format(vertex.x, vertex.y)
                    for vertex in text.bounding_poly.vertices])

        #print('bounds: {}'.format(','.join(vertices)))

        text_info={}
        text_info['description'] =text.description
        text_info['bounds'] =','.join(vertices)
        list_text_info.append(text_info)

    return list_text_info

    # [END migration_text_detection]
# [END def_detect_text]


def do_work(in_queue, out_list):

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
        #file_source_flac = line['full_name_flac']
        #file_source_wav= line['full_name_wav']



        if 'image_text' not in line  :

            file_source = os.path.join(line['folder_name'], line['video_image_name'])



            if os.path.exists(file_source) and os.path.getsize(file_source)  >0:
                line['image_texts'] = []

                text={}
                #file_text['text']=detect_text(file_name)
                text['texts']=detect_text(file_source)
                text['api_call']=1
                line['image_texts'].append(text)

        else:
            #print('exist')
            #exist -> update

            api_count=line['image_text'].get('api_call',0)
            #speech_count=line['audio_text'].get('speech_found',-1)


            #2 condion
            if api_count==0 :
                #print('update')

                file_source = os.path.join(line['folder_name'], line['video_image_name'])
                if os.path.exists(file_source) and os.path.getsize(file_source)  >0:

                    text={}
                    #file_text['text']=detect_text(file_name)
                    text['texts']=detect_text(file_source)
                    line['image_texts'].append(text)

                    text['api_call']=api_count+1
                    line['image_text']=text

                            ###############

        result = (line_no, line)


        out_list.append(result)




def get_detected_value(p_base_dir, p_date, p_delta, p_work_json):


    start=datetime.strptime(p_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_date, '%Y-%m-%d').date()

    #start with - delta
    start-= timedelta(p_delta)
    #list_work_json_before=[]

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        folder_date=os.path.join(p_base_dir, date)
        file_source = os.path.join(folder_date, 'video_image_hash_' + str(p_date) + '.json')

        if os.path.exists(file_source) and os.path.getsize(file_source) >0 :
            with open (file_source,'r') as _file_json:
                data = json.load(_file_json)

            #loop source
            for _source_json in data['hash_md5']:

                    #loop dest
                    for _dest_json in p_work_json:

                        if _source_json['video_image_hash_md5']==_dest_json['video_image_hash_md5']:
                            if 'image_text' in _source_json  :
                                #make sure data empty
                                if not len(_source_json['image_text']) > 0:
                                    #update
                                    _dest_json['image_text']=_source_json['image_text']
                            break


        start += timedelta(1)

    return p_work_json




def run_detect_video_image_text(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'video_images')
    folder_dest = os.path.join(folder_date, 'video_images')
    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)

    file_source_1 = os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')
    file_source_2 = os.path.join(folder_date, 'video_image_hash_' + str(p_date) + '.json')
    file_dest= os.path.join(folder_date, 'video_image_hash_' + str(p_date) + '.json')
    file_dest_test= os.path.join(folder_date, 'test_video_image_hash_' + str(p_date) + '.json')


    with open (file_source_1,'r') as _file:
        work_json_1 = json.load(_file)

    with open (file_source_2,'r') as _file:
        work_json_2 = json.load(_file)

    #look up for video_hash dont have transcript
    list_video_1=[]



    for _json in work_json_1['hash_md5']:
        if 'audio_text' in _json:

            if 'transcript' in _json['audio_text'] :
                if _json['audio_text']['transcript'] is None :
                    list_video_1.append(_json['video_hash_md5'])
                else:
                    #video have no transcript after call api
                    if  not _json['audio_text']['transcript'].get('transcript','')  and _json['audio_text'].get('api_call',0) > 0  :
                        list_video_1.append(_json['video_hash_md5'])


    #print(len(list_video_1))
    #print(list_video_1)

    list_image=[]

    for _json in work_json_2['hash_md5']:
        for _video in list_video_1:
            if _video==_json['video_hash_md5']:
                file_json = {
                    'video_hash_md5':_json['video_hash_md5'],
                    'video_image_hash_md5':_json['video_image_hash_md5'],
                    'video_image_name': _json['video_image_name'][0],
                    'folder_name': folder_source
                    #'image_index': int(_file[-7:-4])
                }
                list_image.append(file_json)


    #get_detected_value


    work_json_3=get_detected_value(p_base_dir, p_date, 30, list_image)



    for _json in work_json_3:
        if 'image_text' in _json:
            file_json = {
                'video_hash_md5':_json['video_hash_md5'],
                'video_image_hash_md5':_json['video_image_hash_md5'],
                'video_image_name':_json['video_image_name'],
                'image_text': _json['image_text'],
                'folder_name': folder_source
                #'image_index': int(_file[-7:-4])
            }
        else:
            file_json = {
                'video_hash_md5':_json['video_hash_md5'],
                'video_image_hash_md5':_json['video_image_hash_md5'],
                'video_image_name':_json['video_image_name'],
                'folder_name': folder_source
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
    return_json['hash_md5']=[]

    for _json in results:
        work_hash={}
        work_hash['video_hash_md5']=_json[1]['video_hash_md5']
        work_hash['video_image_hash_md5']=_json[1]['video_image_hash_md5']
        work_hash['video_image_name']=_json[1]['video_image_name']
        if 'image_text' in _json[1]:
            work_hash['image_text']=_json[1]['image_text']

        return_json['hash_md5'].append(work_hash)


    with open (file_dest_test,'w') as _file:
        json.dump(return_json, _file)





def run_update_content_audio_text(p_base_dir,p_date):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)


    file_source_1 = os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')
    file_source_2= os.path.join(folder_date, 'video_hash_' + str(p_date) + '.json')
    file_source_3= os.path.join(folder_date, 'video_url_' + str(p_date) + '.json')
    file_dest= os.path.join(folder_date, 'ads_creatives_audit_content_' + str(p_date) + '.json')


    with open (file_source_1,'r') as _file:
        work_json_1 = json.load(_file)

    with open (file_source_2,'r') as _file:
        work_json_2 = json.load(_file)

    with open (file_source_3,'r') as _file:
        work_json_3 = json.load(_file)

    with open (file_dest,'r') as _file:
        dest_json = json.load(_file)




    #loop1
    for _json_1 in work_json_1['hash_md5']:
        if 'image_text' in _json_1:
            found_1=-1

            #loop2
            for _j2,_json_2 in enumerate(work_json_2['hash_md5']):
                if _json_1['video_hash_md5']==_json_2['hash_md5']:
                    found_1=_j2
                    break

            if found_1 >=0:
                #loop3
                for _file in work_json_2['hash_md5'][found_1]['file_name']:
                    found_2=-1
                    #loop4
                    for _j3,_json_3 in enumerate(work_json_3['my_json']):
                        if _file==_json_3['file_name']:

                            #update dest
                            index_json=work_json_3['my_json'][_j3]['index_json']
                            index_video=work_json_3['my_json'][_j3]['index_video']

                            dest_json['my_json'][index_json]['audit_content']['video_ids'][index_video]['image_text']=_json_1['image_text']

    with open (file_dest,'w') as _file:
            json.dump(dest_json, _file)



def detect_video_image_text_date(p_base_dir, p_date ,p_process_num):
    run_detect_video_image_text(p_base_dir,p_date,p_process_num)
    #run_update_video_image_text(p_base_dir,p_date)


def detect_video_image_text_period(p_base_dir, p_from_date , p_to_date ,p_process_num):

    start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        detect_video_image_text_date (p_base_dir, date,p_process_num)
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
    detect_video_image_text_period( base_dir, from_date, to_date, process_num)
