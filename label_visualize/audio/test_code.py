import os
import json
import subprocess
from datetime import datetime , timedelta, date

import argparse
import io



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
    response = operation.result(timeout=90)

    # Print the first alternative of all the consecutive results.
    for result in response.results:
        print('Transcript: {}'.format(result.alternatives[0].transcript))
        print('Confidence: {}'.format(result.alternatives[0].confidence))

    return result.alternatives[0].transcript, result.alternatives[0].confidence



def get_text_from_video(file_video, file_audio, file_history):
	#================== Convert video ====================
	subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_audio])


	#================= Standardlize Audio ==============
	subprocess.call(["sox", file_audio, "--channels=1", "--bits=16", file_audio[0:-5] + '.16.flac'])


	#============== Get sample rate ==================
	cmd = "ffprobe " + file_audio[0:-5] + '.16.flac' + " -show_entries" + " stream=sample_rate"
	out = subprocess.check_output(cmd) 
	sample_rate = int(out[out.find('=') + 1:out.rfind('[')])

	#============== Get text of audio ===================
	transcript, confidence = transcribe_file(file_audio[0:-5] + '.16.flac', sample_rate)
	print(file_audio[0:-5] + '.16.flac')
	print('Transcript:', transcript)
	print('Confidence:', confidence)

	# ============= Get Time of audio ====================
	cmd = "ffprobe " + file_audio[0:-5] + '.16.flac' + " -show_entries" + " stream=duration"
	out = subprocess.check_output(cmd) 
	duration = float(out[out.find('=') + 1:out.rfind('[')])

	if os.path.exists(file_history):
		with open(file_history, 'r') as fi:
			list_history = json.load(fi)
	else:
		list_history = {
			'Total_time': 0,
			'Detail': []
		}

	json_ = {
		'Date': datetime.datetime.now(),
		'Video': file_video,
		'Audio': file_audio,
		'Transcript': transcript,
		'Confidence': confidence,
		'Time': duration
	}

	list_history['Total_time'] += json_['Time']
	list_history['Detail'].append(json_)

	with open(file_history, 'w') as fo:
		json.dump(file_history, fo)





file_video = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/audio/2016-10-01_98.mp4'
file_audio = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/audio/2016-10-01_98.flac'
file_history = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/audio/history.json'

get_text_from_video(file_video, file_audio, file_history)








# print('11111111111111111111111111111')
# subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_audio])
# print()

# print('22222222222222222222222222222')
# print(file_video + '.16.flac')
# subprocess.call(["sox", file_audio, "--channels=1", "--bits=16", file_audio[0:-5] + '.16.flac'])
# print()

# print('33333333333333333333333333333')
# print(file_audio[0:-5] + '.16.flac')
# out = subprocess.call(["ffprobe", file_audio[0:-5] + '.16.flac', "-show_streams| grep", "stream=sample_rate"]) 
# cmd = "ffprobe " + file_audio[0:-5] + '.16.flac'
# out = subprocess.Popen(["ffprobe", file_audio[0:-5] + '.16.flac'])
# stdout, stderr = out.communicate()
# print("====================================")
# a = out.stdout.close()
# out = subprocess.getoutput(cmd)

# cmd = "ffprobe " + file_audio[0:-5] + '.16.flac' + " -show_entries" + " stream=duration" #sample_rate"#["ffprobe", file_audio[0:-5] + '.16.flac', "-show_entries", "stream=sample_rate"]
# out = subprocess.check_output(cmd) 

# sample_rate = int(out[out.find('=') + 1:out.rfind('[')])
# print('+++++++++++++++++++++++++++++++++')
# print(sample_rate)
# print('+++++++++++++++++++++++++++++++++')

# ffprobe 2016-11-01_243_0.16.flac  -show_streams| grep  sample_rate
# sample_rate=48000 file_audio[0:-4] | grep , shell=True
