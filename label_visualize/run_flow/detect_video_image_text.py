
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



def get_text(p_file_work):
    #============== Get sample rate ==================
    cmd = "ffprobe " + p_file_work + " -show_entries" + " stream=sample_rate"
    out = subprocess.check_output(cmd)
    #print(out)
    if (isinstance(out, bytes)):
        out = str(out)
        sample_rate = int(out[(out.find('=') + 1) : (out.rfind('[') - 4)])
    elif (isinstance(out, str)):
        sample_rate = int(out[(out.find('=') + 1) : (out.rfind('['))])
    print(sample_rate)

    #============== Get text of audio ===================
    text = transcribe_file(p_file_work, sample_rate)

    return text


def get_image_text(p_folder, p_path_folder_work, p_work_json):


    list_index = []
    list_file = next(os.walk(p_path_folder_work))[2]

    list_file_work=[]

    for _file in list_file:
        print(_file)
        file_json = {
            'name': _file,
            'video_index': int(_file[11:-12]),
            'image_index': int(_file[-7:-4])
        }
        list_index.append(file_json)
        list_file_work.append(_file)

    print   (list_index)

    for _i, _value in enumerate(p_work_json['my_json']):

        if 'image_texts' not in _value:
            _value['image_texts'] = []
            #_value['audio_text']['api_call'] = 0
            #_value['audio_text']['transcript'] = ''
            #_value['audio_text']['confidence'] = 0

        #if 'api_call' not in _value['audio_text']:
        #    _value['audio_text']['api_call']=0


        if not _value['image_texts']:
            #print(value['audio_text'])
        #if not value['audio_text']['transcript']:
            for _file in list_index:
                if _file['video_index'] == _i:

                    # link = 'gs://python_video/' + folder + '/' + file_['name']
                    file_name = p_path_folder_work + '/' + _file['name']
                    print(file_name)

                    #init
                    file_text={}

                    file_text['name']=_file['name']
                    file_text['api_call']=0
                    file_text['text']=detect_text(file_name)


                    _value['image_texts'].append(file_text)


                    #count = _value['video_images'].get('api_call',0)
                    #if count == 0:
                    #    _value['audio_text'] = get_text(file_name)
                    #    _value['audio_text']['api_call'] = count+1
                    #        print('Result')
                    #        print(_value['audio_text'])
                    # value['audio_text'] = {}
                    # print ("Done")

    return p_work_json

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

def get_video_image_text(p_path, p_from_date = '2016-10-01', p_to_date = '2016-10-01'):
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
                    work_json = get_image_text(_folder, path_folder_work, work_json)
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
                            data['my_json'][i]['audit_content']['video_ids'][j]['audio_text'] = _value['audio_text']

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
    script, date, to_date = argv
    get_video_image_text(g_path, date, to_date)
