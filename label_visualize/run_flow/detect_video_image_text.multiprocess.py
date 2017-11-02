
"""
    Project : Online marketing tool - Audit content - Audit audio
    Company : VNG Corporation

    Description: Call gcloud speech API to get text for audio

    Examples of Usage:
        python detect_audio.py 2016-10-01 2017-06-29
"""




import argparse
import io
import sys
import os
import json
import subprocess
from datetime import datetime , timedelta, date
from multiprocessing import Process, Manager
import itertools
import time

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



def do_work(list_index,in_queue, out_list):


    while True:
        item = in_queue.get()
        #line_no, line = item
        _i,_value = item
        print ('_i:',_i,' _value:',_value)



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

            if _file['video_name'] == _value['file_name'] :

                # link = 'gs://python_video/' + folder + '/' + file_['name']
                file_name=_file['full_name']
                #print('Process:',file_name)

                #not image_texts exist->init {}
                if 'image_texts' not in _value:
                    print('init')
                    _value['image_texts'] = []

                    text={}
                    text['name']=_file['name']
                    #file_text['text']=detect_text(file_name)
                    text['texts']=detect_text(file_name)
                    text['api_call']=1
                    _value['image_texts'].append(text)
                #exist image_texts -> update or append
                else:
                    #loop 3
                    found=-1
                    for _j, _value_j in enumerate(_value['image_texts']):
                        if _value_j['name']==_file['name']:
                            found=_j
                            break

                    #exist -> update
                    if found>=0:
                        count=_value['image_texts'][found].get('api_call',0)
                        if count==0:
                            print('update')
                            _value['image_texts'][found]['texts']=detect_text(file_name)
                            _value['image_texts'][found]['api_call']=count+1
                    # append
                    else:
                        print('append')
                        text={}
                        text['name']=_file['name']
                        #file_text['text']=detect_text(file_name)
                        text['texts']=detect_text(file_name)
                        text['api_call']=1
                        _value['image_texts'].append(text)

        #print(result)
        result = (_i, _value)

        out_list.append(result)

def get_image_text(p_folder, p_path_folder_work, p_work_json, p_process_num):


    list_index = []
    list_file = next(os.walk(p_path_folder_work))[2]

    for _file in list_file:
        if len(_file) >24:
            #print(_file)
            #print( _file[:-8])
            file_json = {
                'name': _file,
                'video_name': _file[:-8],
                'full_name': p_path_folder_work + '/' + _file
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



def get_30_date(p_path_full_data, p_date, p_work_json):
    list_neighbor = []
    list_folder = next(os.walk(g_path))[1]

    delta = 60
    vdate = datetime.strptime(p_date, '%Y-%m-%d').date()

    list_work_json_before = []
    list_name = []
    json_count = 0

    #================ lay data truoc 30 ngay =============
    for _i in range(int (delta)):
        single_date = vdate - timedelta(_i)
        folder = os.path.join(p_path_full_data, single_date.strftime('%Y-%m-%d'))
        file_name = folder + '/' + 'video_url_'+ single_date.strftime('%Y-%m-%d') + '.json'
        if os.path.exists(file_name):
            with open (file_name,'r') as _file_json:
                data = json.load(_file_json)
                for _value in data['my_json']:
                    # print(value)
                    if _value.get('video_images',[]) and (_value['file_name'] not in list_name):
                        list_name.append(_value['file_name'])
                        list_work_json_before.append(_value)

    #============ Update data neu da ton tai=============
    for _value in p_work_json['my_json']:
        for _json in list_work_json_before:
            if (_value['file_name'] == _json['file_name']) and (_json.get('video_images',[])):
                _value['video_images'] = _json.get('video_images',[])
                json_count += 1
    print ("======================================================================")
    # print (video_json)
    print ("Total "+ str(len(p_work_json['my_json'])))
    print ("Finded " + str(json_count))
    print ("======================================================================")
    return (list_work_json_before, p_work_json)

def get_video_image_text(p_path, p_from_date = '2016-10-01', p_to_date = '2016-10-01',p_process_num=1):
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
            path_folder_work = os.path.join(path_folder, 'video_images')
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
                    list_json_before, work_json = get_30_date(p_path, _folder, work_json)
                    work_json = get_image_text(_folder, path_folder_work, work_json, p_process_num)
                    # print (video_json)
                    with open (path_file_work,'w') as _f:
                        json.dump(work_json, _f)
                print ("========================= Add label to data json =========================")

                #cap nhat audit_content

                if os.path.exists(path_file) and os.path.exists(path_file_work):
                    with open(path_file, 'r') as _f:
                        data = json.load(_f)
                    with open(path_file_work, 'r') as _f:
                        data_work = json.load(_f)
                    for _value in data_work['my_json']:
                        i = _value['index_json']
                        j = _value['index_video']

                        if 'video_ids' in data['my_json'][i]['audit_content']:
                            #if 'image_texts' not in data['my_json'][i]['audit_content']['video_ids'][j] and 'image_texts' in  _value:
                            if  'image_texts' in  _value:
                                data['my_json'][i]['audit_content']['video_ids'][j]['image_texts'] = _value['image_texts']

                    with open (path_file,'w') as _f:
                        json.dump(data, _f)

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
    get_video_image_text(g_path, date, to_date, process_num)
