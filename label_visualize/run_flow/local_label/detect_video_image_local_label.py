
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

from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)

from google.cloud import vision
from google.cloud.vision import types


import argparse
import sys

import numpy as np
import tensorflow as tf

def load_graph(model_file):
    graph = tf.Graph()
    graph_def = tf.GraphDef()

    with open(model_file, "rb") as f:
      graph_def.ParseFromString(f.read())
    with graph.as_default():
      tf.import_graph_def(graph_def)

    return graph

def read_tensor_from_image_file(file_name, input_height=299, input_width=299,
				input_mean=0, input_std=255):
    input_name = "file_reader"
    output_name = "normalized"
    file_reader = tf.read_file(file_name, input_name)
    if file_name.endswith(".png"):
        image_reader = tf.image.decode_png(file_reader, channels = 3,
                                       name='png_reader')
    elif file_name.endswith(".gif"):
        image_reader = tf.squeeze(tf.image.decode_gif(file_reader,
                                                  name='gif_reader'))
    elif file_name.endswith(".bmp"):
        image_reader = tf.image.decode_bmp(file_reader, name='bmp_reader')
    else:
        image_reader = tf.image.decode_jpeg(file_reader, channels = 3,
                                        name='jpeg_reader')
    float_caster = tf.cast(image_reader, tf.float32)
    dims_expander = tf.expand_dims(float_caster, 0);
    resized = tf.image.resize_bilinear(dims_expander, [input_height, input_width])
    normalized = tf.divide(tf.subtract(resized, [input_mean]), [input_std])
    sess = tf.Session()
    result = sess.run(normalized)

    return result

def load_labels(label_file):
    label = []
    proto_as_ascii_lines = tf.gfile.GFile(label_file).readlines()
    for l in proto_as_ascii_lines:
        label.append(l.rstrip())
    return label

def detect_local_label(p_path):

    file_name = p_path
    model_file = \
        "inception_v3_2016_08_28_frozen.pb"
    label_file = "imagenet_slim_labels.txt"
    input_height = 299
    input_width = 299
    input_mean = 0
    input_std = 255
    input_layer = "input"
    output_layer = "InceptionV3/Predictions/Reshape_1"

    graph = load_graph(model_file)
    t = read_tensor_from_image_file(file_name,
                                  input_height=input_height,
                                  input_width=input_width,
                                  input_mean=input_mean,
                                  input_std=input_std)

    input_name = "import/" + input_layer
    output_name = "import/" + output_layer
    input_operation = graph.get_operation_by_name(input_name);
    output_operation = graph.get_operation_by_name(output_name);

    with tf.Session(graph=graph) as sess:
        results = sess.run(output_operation.outputs[0],
                      {input_operation.outputs[0]: t})
    results = np.squeeze(results)

    #top_k = results.argsort()[-5:][::-1]
    if len(results) >=10:
        top_k = results.argsort()[-10:][::-1]
    else:
        top_k = results.argsort()[-len(results):][::-1]

    labels = load_labels(label_file)

    return_label=[]
    for i in top_k:
        #print(labels[i], results[i])
        return_label.append(labels[i])

    return return_label






def get_image_local_label(p_folder, p_path_folder_work, p_work_json):


    list_index = []
    list_file = next(os.walk(p_path_folder_work))[2]

    for _file in list_file:
        #print(_file)
        if len(_file) >24:
            file_json = {
                'name': _file,
                'video_name': _file[:-8],
                'full_name': p_path_folder_work + '/' + _file
                #'image_index': int(_file[-7:-4])
            }
            list_index.append(file_json)

    #print   (list_index)



    #loop 1
    for _i, _value in enumerate(p_work_json['my_json']):

        #loop 2
        for _file in list_index:

            if _file['video_name'] == _value['file_name'] :

                # link = 'gs://python_video/' + folder + '/' + file_['name']
                file_name = p_path_folder_work + '/' + _file['name']
                print('Process:',file_name)

                #not image_texts exist->init {}
                if 'image_local_labels' not in _value:
                    print('init')
                    _value['image_local_labels'] = []

                    text={}
                    text['name']=_file['name']
                    #file_text['text']=detect_text(file_name)
                    text['labels']=detect_local_label(file_name)
                    text['api_call']=1
                    _value['image_local_labels'].append(text)
                #exist image_texts -> update or append
                else:
                    #loop 3
                    found=-1
                    for _j, _value_j in enumerate(_value['image_local_labels']):
                        if _value_j['name']==_file['name']:
                            found=_j
                            break

                    #exist -> update
                    if found>=0:
                        count=_value['image_local_labels'][found].get('api_call',0)
                        if count==0:
                            print('update')
                            _value['image_local_labels'][found]['labels']=detect_local_label(file_name)
                            _value['image_local_labels'][found]['api_call']=count+1
                    # append
                    else:
                        print('append')
                        text={}
                        text['name']=_file['name']
                        #file_text['text']=detect_text(file_name)
                        text['labels']=detect_local_label(file_name)
                        text['api_call']=1
                        _value['image_local_labels'].append(text)





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

def get_video_image_local_label(p_path, p_from_date = '2016-10-01', p_to_date = '2016-10-01'):
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
                    work_json = get_image_local_label(_folder, path_folder_work, work_json)
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
                                data['my_json'][i]['audit_content']['video_ids'][j]['image_local_labels'] = _value['image_local_labels']

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
    get_video_image_local_label(g_path, date, to_date)
