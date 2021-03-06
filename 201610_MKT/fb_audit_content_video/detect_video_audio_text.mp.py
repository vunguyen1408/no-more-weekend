
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

from VoiceActivityDetector import VoiceActivityDetector


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
        #encoding=enums.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=int(p_sample_rate),
        language_code='vi-VN')

    # [START migration_async_response]
    try:
        operation = client.long_running_recognize(config, audio)
        print('Waiting for operation to complete...')
        response = operation.result(timeout=90)

        text = {}

        # Print the first alternative of all the consecutive results.
        for result in response.results:
            print('Transcript: {}'.format(result.alternatives[0].transcript))
            print('Confidence: {}'.format(result.alternatives[0].confidence))
            text['transcript'] = result.alternatives[0].transcript
            text['confidence'] = result.alternatives[0].confidence
            #print(text)
        # [END migration_async_response]

        return text

    except Exception as ex:
        print("Failed with error %r", ex)

    # [END migration_async_request]






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



        if 'audio_text' not in line  :

            file_source_wav = os.path.join(line['folder_name'], line['video_hash_md5']+'.16.wav')
            file_source_flac = os.path.join(line['folder_name'], line['video_hash_md5']+'.16.flac')


            if os.path.exists(file_source_wav) and os.path.getsize(file_source_wav)  >0:
                v = VoiceActivityDetector(file_source_wav)
                raw_detection = v.detect_speech()
                speech_labels = v.convert_windows_to_readible_labels(raw_detection)


                line['audio_text'] = {}
                text={}
                if (len(speech_labels) > 3):
                    #print('init')
                    text['transcript']=detect_audio_text(file_source_flac)

                text['api_call']=1
                line['audio_text']=text

        else:
            #print('exist')
            #exist -> update

            api_count=line['audio_text'].get('api_call',0)
            #speech_count=line['audio_text'].get('speech_found',-1)


            #2 condion
            if api_count==0 :
                #print('update')

                file_source_wav = os.path.join(line['folder_name'], line['video_hash_md5']+'.16.wav')
                file_source_flac = os.path.join(line['folder_name'], line['video_hash_md5']+'.16.flac')

                if os.path.exists(file_source_wav) and os.path.getsize(file_source_wav)  >0:


                    v = VoiceActivityDetector(file_source_wav)
                    raw_detection = v.detect_speech()
                    speech_labels = v.convert_windows_to_readible_labels(raw_detection)

                    text={}
                    if (len(speech_labels) > 3):
                        text['transcript']=detect_audio_text(file_source_flac)
                        #print('Call API + update')

                    text['api_call']=api_count+1
                    line['audio_text']=text

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
        file_source = os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')

        if os.path.exists(file_source) and os.path.getsize(file_source) >0 :
            with open (file_source,'r') as _file_json:
                data = json.load(_file_json)

            #loop source
            for _source_json in data['hash_md5']:

                    #loop dest
                    for _dest_json in p_work_json['hash_md5']:

                        if _source_json['video_hash_md5']==_dest_json['video_hash_md5']:
                            if 'audio_text' in _source_json  :
                                #make sure data empty
                                if not _source_json['audio_text'].get('transcript',''):
                                    #update
                                    _dest_json['audio_text']=_source_json['audio_text']
                            break


        start += timedelta(1)

    return p_work_json




def run_detect_video_audio_text(p_base_dir,p_date,p_process_num):

    #list file
    list_file = []

    folder_date=os.path.join(p_base_dir, p_date)
    folder_source = os.path.join(folder_date, 'video_audios')
    folder_dest = os.path.join(folder_date, 'video_audios')
    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)

    file_source = os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')
    file_dest= os.path.join(folder_date, 'audio_hash_' + str(p_date) + '.json')

    #print(file_source)
    if os.path.exists(file_source):
        with open (file_source,'r') as _file:
            work_json = json.load(_file)
    else:
        print('exit')
        return


    #get_detected_value


    work_json=get_detected_value(p_base_dir, p_date, 30, work_json)





    for _json in work_json['hash_md5']:
        if 'audio_text' in _json:
            file_json = {
                'video_hash_md5':_json['video_hash_md5'],
                'audio_hash_md5':_json['audio_hash_md5'],
                'audio_text': _json['audio_text'],
                'folder_name': folder_source
                #'image_index': int(_file[-7:-4])
            }
        else:
            file_json = {
                'video_hash_md5':_json['video_hash_md5'],
                'audio_hash_md5':_json['audio_hash_md5'],
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
        work_hash['audio_hash_md5']=_json[1]['audio_hash_md5']
        if 'audio_text' in _json[1]:
            work_hash['audio_text']=_json[1]['audio_text']

        return_json['hash_md5'].append(work_hash)

    with open (file_dest,'w') as _file:
            json.dump(return_json, _file)





def run_update_audio_text(p_base_dir,p_date):

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
        if 'audio_text' in _json_1:
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

                            dest_json['my_json'][index_json]['audit_content']['video_ids'][index_video]['audio_text']=_json_1['audio_text']

    with open (file_dest,'w') as _file:
            json.dump(dest_json, _file)



def detect_video_audio_text_date(p_base_dir, p_date ,p_process_num):
    run_detect_video_audio_text(p_base_dir,p_date,p_process_num)
    run_update_audio_text(p_base_dir,p_date)


def detect_video_audio_text_period(p_base_dir, p_from_date , p_to_date ,p_process_num):

    start = datetime.strptime(p_from_date, '%Y-%m-%d').date()
    end = datetime.strptime(p_to_date, '%Y-%m-%d').date()

    while(start <= end):

        date = start.strftime('%Y-%m-%d')
        detect_video_audio_text_date (p_base_dir, date,p_process_num)
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
    detect_video_audio_text_period( base_dir, from_date, to_date, process_num)
