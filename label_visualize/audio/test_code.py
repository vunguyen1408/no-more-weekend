import os
import json
import subprocess
from datetime import datetime , timedelta, date



file_video = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/audio/2016-10-01_98.mp4'
file_audio = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/audio/2016-10-01_98.flac'
# print('11111111111111111111111111111')
# subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_audio])
# print()

# print('22222222222222222222222222222')
# print(file_video + '.16.flac')
# subprocess.call(["sox", file_audio, "--channels=1", "--bits=16", file_audio[0:-5] + '.16.flac'])
# print()

print('33333333333333333333333333333')
print(file_audio[0:-5] + '.16.flac')
# out = subprocess.call(["ffprobe", file_audio[0:-5] + '.16.flac', "-show_streams| grep", "stream=sample_rate"]) 
# cmd = "ffprobe " + file_audio[0:-5] + '.16.flac'
# out = subprocess.Popen(["ffprobe", file_audio[0:-5] + '.16.flac'])
# stdout, stderr = out.communicate()
# print("====================================")
# a = out.stdout.close()
# out = subprocess.getoutput(cmd)

cmd = "ffprobe " + file_audio[0:-5] + '.16.flac' + " -show_entries" + " stream=sample_rate"#["ffprobe", file_audio[0:-5] + '.16.flac', "-show_entries", "stream=sample_rate"]
out = subprocess.check_output(cmd) 

print(out)
print('+++++++++++++++++++++++++++++++++')
print(out['sample_rate'])
# print(out[0])
print('+++++++++++++++++++++++++++++++++')
print(type(out))

# ffprobe 2016-11-01_243_0.16.flac  -show_streams| grep  sample_rate
# sample_rate=48000 file_audio[0:-4] | grep , shell=True