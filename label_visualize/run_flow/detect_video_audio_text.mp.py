
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

from VoiceActivityDetector import VoiceActivityDetector

from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)


def transcribe_audio(p_speech_file, p_sample_rate):
    """Transcribe the given audio file asynchronously."""
    from google.cloud import speech
    from google.cloud.speech import enums
    from google.cloud.speech import types
    client = speech.SpeechClient()

    # [START migration_async_request]
    with io.open(p_speech_file, 'rb') as audio_file:
        content = audio_file.read()

    audio = types.RecognitionAudio(content=content)
    config = types.RecognitionConfig(
        encoding=enums.RecognitionConfig.AudioEncoding.LINEAR16,
        sample_rate_hertz=int(p_sample_rate),
        language_code='vi-VN')

    # [START migration_async_response]
    operation = client.long_running_recognize(config, audio)
    # [END migration_async_request]

    print('Waiting for operation to complete...')
    response = operation.result(timeout=90)


    text = {}
    text['transcript']=''
    text['confidence']=''
    # Print the first alternative of all the consecutive results.
    for result in response.results:
        print('Transcript: {}'.format(result.alternatives[0].transcript))
        print('Confidence: {}'.format(result.alternatives[0].confidence))
        text['transcript'] = result.alternatives[0].transcript
        text['confidence'] = result.alternatives[0].confidence
        #print(text)
    # [END migration_async_response]

    return text



def detect_audio_text(p_file_work):

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
    text = transcribe_audio(p_file_work, sample_rate)

    return text

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



def get_dictict_value(p_base_dir,p_date,p_delta=60):
    list_neighbor = []
    list_folder = next(os.walk(path))[1]

    start_date = datetime.strptime(p_date, '%Y-%m-%d').date()

    list_work_json_before = []
    list_name = []
    json_count = 0

    #================ lay data truoc 30 ngay =============
    for _i in range(int (p_delta)):
        date = start_date - timedelta(_i)
        folder = os.path.join(p_base_dir, date.strftime('%Y-%m-%d'))
        file_name = folder + '/' + 'video_hash_'+ date.strftime('%Y-%m-%d') + '.json'

        if os.path.exists(file_name):
            with open (file_name,'r') as _file_json:
                data = json.load(_file_json)
                for _value in data['my_json']:
                    if ( 'audio_text' in _value ) and (_value['file_name'] not in list_name):
                        list_name.append(_value['file_name'])
                        list_work_json_before.append(_value)

    #============ Update data neu da ton tai=============
    for _value in p_work_json['my_json']:
        for json_ in list_work_json_before:
            if (_value['file_name'] == json_['file_name']) and  ( 'audio_text' in _value ) :
                #_value['audio_text']['transcript'] = json_['audio_text']['transcript']
                #_value['audio_text']['confidence'] = json_['audio_text']['confidence']
                #_value['audio_text']['api_call'] = json_['audio_text']['api_call']
                _value['audio_text'] = json_['audio_text']
                json_count += 1
    print ("======================================================================")
    # print (video_json)
    print ("Total "+ str(len(p_work_json['my_json'])))
    print ("Finded " + str(json_count))
    print ("======================================================================")
    return (list_work_json_before, p_work_json)


def run_detect_video_audio_text(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_work = os.path.join(folder_date, 'videos')

    file_work = os.path.join(folder, 'video_hash_' + str(p_date) + '.json')

    #get distinct value 30 day


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


def detect_video_audio_text_date(p_base_dir, p_date = '2016-10-01',p_process_num):
    run_detect_video_audio_text(p_base_dir,p_date,p_process_num)
    update_content_video_audio_text(p_base_dir,p_date,p_process_num)


def detect_video_audio_text_period(p_base_dir, p_from_date = '2016-10-01', p_to_date = '2016-10-01',p_process_num):
	start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
	end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

	while(start <= end):
		date = os.path.join(p_base_dir, start.strftime('%Y-%m-%d'))
        detect_video_audio_text_date (p_base_dir, date,p_process_num)
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
    detect_video_audio_text_period(base_dir, from_date, to_date, process_num)
