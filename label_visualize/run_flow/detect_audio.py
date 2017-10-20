
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


def transcribe_file(speech_file, p_sample_rate):
    """Transcribe the given audio file asynchronously."""
    from google.cloud import speech
    from google.cloud.speech import enums
    from google.cloud.speech import types
    client = speech.SpeechClient()

    # [START migration_async_request]
    with io.open(speech_file, 'rb') as audio_file:
        content = audio_file.read()

    audio = types.RecognitionAudio(content=content)
    config = types.RecognitionConfig(
        encoding=enums.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=int(p_sample_rate),
        language_code='vi-VN')

    # [START migration_async_response]
    operation = client.long_running_recognize(config, audio)
    # [END migration_async_request]

    print('Waiting for operation to complete...')
    response = operation.result(timeout=300)

    # Print the first alternative of all the consecutive results.
    text = {}

    for result in response.results:
        print('Transcript: {}'.format(result.alternatives[0].transcript))
        print('Confidence: {}'.format(result.alternatives[0].confidence))

        text['transcript'] = result.alternatives[0].transcript
        text['confidence'] = result.alternatives[0].confidence
        print(text)

    return text



def analyze_labels(file_audio):
    #============== Get sample rate ==================
    cmd = "ffprobe " + file_audio + " -show_entries" + " stream=sample_rate"
    out = subprocess.check_output(cmd)
    print(out)
    if (isinstance(out, bytes)):
        out = str(out)
        sample_rate = int(out[(out.find('=') + 1) : (out.rfind('[') - 4)])
    elif (isinstance(out, str)):
        sample_rate = int(out[(out.find('=') + 1) : (out.rfind('['))])
    # print(sample_rate)

    #============== Get text of audio ===================
    text = transcribe_file(file_audio, sample_rate)

    return text


def get_label_videos(folder, path_folder_audios, video_json):

    list_index = []
    list_file = next(os.walk(path_folder_audios))[2]

    for file_ in list_file:
        file_json = {
            'name': file_,
            'index': int(file_[11:-5])
        }
        list_index.append(file_json)

    for i, value in enumerate(video_json['my_json']):
        if 'audio_text' not in value:
            value['audio_text'] = {}
            value['audio_text']['transcript'] = ''
            value['audio_text']['confidence'] = 0

        if 'transcript' in value['audio_text'] :
            print ("Found")
            print(value['audio_text']['transcript'])

        print ("cont")

        #if not (value['audio_text']['transcript'] != ""):
        if not value['audio_text']['transcript'] :
            for file_ in list_index:
                if file_['index'] == i:
                    # link = 'gs://python_video/' + folder + '/' + file_['name']
                    file_name = path_folder_audios + '/' + file_['name']
                    print(file_name)

                    # list_label = analyze_labels(link)
                    # value['video_label'] = list(list_label)

                    #leth 2017.10.20
                    #check file size > 0
                    file_stat = os.stat(file_name)
                    print (file_stat.st_size)
                    if file_stat.st_size > 0:
                        value['audio_text'] = analyze_labels(file_name)
                    # value['audio_text'] = {}
                    # print ("Done")

    return video_json

def get_30_date(path_full_data, date, video_json):
    list_neighbor = []
    list_folder = next(os.walk(path))[1]

    delta = 60
    vdate = datetime.strptime(date, '%Y-%m-%d').date()

    list_video_json_before = []
    list_name = []
    json_count = 0

    #================ lay data truoc 30 ngay =============
    for i in range(int (delta)):
        single_date = vdate - timedelta(i)
        folder = os.path.join(path_full_data, single_date.strftime('%Y-%m-%d'))
        file_name = folder + '/' + 'audio_url_'+ single_date.strftime('%Y-%m-%d') + '.json'
        if os.path.exists(file_name):
            with open (file_name,'r') as file_json:
                data = json.load(file_json)
                for value in data['my_json']:
                    # print(value)
                    if (value['audio_text']['transcript'] != '') and (value['file_name'] not in list_name):
                        list_name.append(value['file_name'])
                        list_video_json_before.append(value)

    #============ Update data neu da ton tai=============
    for value in video_json['my_json']:
        for json_ in list_video_json_before:
            if (value['file_name'] == json_['file_name']) and (json_['audio_text']['transcript'] != ''):
                value['audio_text']['transcript'] = json_['audio_text']['transcript']
                value['audio_text']['confidence'] = json_['audio_text']['confidence']
                json_count += 1
    print ("======================================================================")
    # print (video_json)
    print ("Total "+ str(len(video_json['my_json'])))
    print ("Finded " + str(json_count))
    print ("======================================================================")
    return (list_video_json_before, video_json)

def add_label_video_to_data(path, date_ = '2016-10-01', to_date_ = '2016-10-01'):
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    list_folder = next(os.walk(path))[1]

    #========================== Auto run ===================

    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    for folder in list_folder:
        d = datetime.strptime(folder, '%Y-%m-%d').date()

        if d <= to_date and d >= date:
            # print (d)

            #==============================================
            path_folder = os.path.join(path, folder)
            path_folder_audios = os.path.join(path_folder, 'audios')
            path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
            path_file_video = os.path.join(path_folder, 'video_url_' + str(folder) + '.json')
            # print (path_file)
            # print (path_file_video)
            if os.path.exists(path_file) and os.path.exists(path_file_video):

                #cap nhat video_json
                with open (path_file_video,'r') as file_json:
                    video_json = json.load(file_json)
                    # video_json = get_label_videos(folder, path_folder_audios, video_json)
                    list_video_json_before, video_json = get_30_date(path, folder, video_json)
                    video_json = get_label_videos(folder, path_folder_audios, video_json)
                    # print (video_json)
                    with open (path_file_video,'w') as f:
                        json.dump(video_json, f)
                print ("========================= Add label to data json =========================")

                #cap nhat audit_content

                if os.path.exists(path_file) and os.path.exists(path_file_video):
                    with open(path_file, 'r') as f:
                        data = json.load(f)
                    with open(path_file_videos, 'r') as f:
                        data_video = json.load(f)
                    for vaule in data_video['my_json']:
                        i = vaule['index_json']
                        j = vaule['index_video']
                        if 'video_ids' in data['my_json'][i]['audit_content']:
                            data['my_json'][i]['audit_content']['video_ids'][j]['audio_text'] = vaule['audio_text']

                    with open (path_file,'w') as f:
                        json.dump(data, f)


# path_folder_videos = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-02/videos'
# path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# path = 'D:/DATA/NEW_DATA_10-2016_05-2017/FULL_DATA_10-2016_06-2017/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# date_ = '2016-11-26'
# to_date_ = '2016-12-10'

if __name__ == '__main__':
    from sys import argv
    path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, date, to_date = argv
    add_label_video_to_data(path, date, to_date)
