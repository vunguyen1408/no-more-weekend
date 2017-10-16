import os
import json
import subprocess
from datetime import datetime , timedelta, date



file_video = 'C:/Users/CPU10912-local/Desktop/2016-10-01_98.mp4'
file_audio = 'C:/Users/CPU10912-local/Desktop/2016-10-01_98.flac'
print('11111111111111111111111111111')
subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_audio])
print()

print('22222222222222222222222222222')
print(file_video + '.16.flac')
subprocess.call(["sox", file_audio, "--channels=1", "--bits=16", file_video + '.16.flac'])
print()

print('33333333333333333333333333333')
subprocess.call(["ffprobe", file_audio[0:-4] + '.16.flac', "-show_streams|", "grep", "sample_rate"])
print()

# ffprobe 2016-11-01_243_0.16.flac  -show_streams| grep  sample_rate
# sample_rate=48000