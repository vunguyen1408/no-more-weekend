import os
import json
import subprocess
from datetime import datetime , timedelta, date


def convertVideoToAudio(path_data, start_date, end_date):
	start = datetime.strptime(start_date, '%Y-%m-%d').date()
	end = datetime.strptime(end_date, '%Y-%m-%d').date()

	
	while(start <= end):
		path_date = os.path.join(path_data, start.strftime('%Y-%m-%d'))
		path_video = os.path.join(path_date, 'videos')
		if os.path.exists(path_video):
			path_audio = os.path.join(path_date, 'audios')
			if not os.path.exists(path_audio):
				os.makedirs(path_audio)

			list_video = next(os.walk(path_video))[2]
			for video in list_video:
				file_video = os.path.join(path_video, video)
				file_name = video[0:video.rfind('.') ] + '.flac'
				file_audio = os.path.join(path_audio, file_name)
				if not os.path.exists(file_audio):				
					subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_audio])					
					print(file_audio)
				if os.path.exists(file_audio):
					subprocess.call(["sox", file_audio, "--channels=1", "--bits=16", file_audio])
					print("Standar.............", file_audio)

		start += timedelta(1)


# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# convertVideoToAudio(path_data, '2016-10-01', '2017-06-29')


if __name__ == '__main__':
    from sys import argv
    path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, date, to_date = argv
    convertVideoToAudio(path_data, date, to_date)










