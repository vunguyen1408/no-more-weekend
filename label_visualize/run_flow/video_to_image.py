"""
	Project : Online marketing tool - Audit content - Audit audio
	Company : VNG Corporation

	Description: Convert video to audio

    Examples of Usage:
    	python video_to_audio.py 2016-10-01 2017-06-29
"""


import os
import json
import subprocess
from datetime import datetime , timedelta, date


def convertVideoToImage(path_data, start_date, end_date):
	start = datetime.strptime(start_date, '%Y-%m-%d').date()
	end = datetime.strptime(end_date, '%Y-%m-%d').date()


	while(start <= end):
		path_date = os.path.join(path_data, start.strftime('%Y-%m-%d'))
		path_video = os.path.join(path_date, 'videos')
		if os.path.exists(path_video):
			path_image = os.path.join(path_date, 'video_images')
			if not os.path.exists(path_image):
				os.makedirs(path_image)

			list_video = next(os.walk(path_video))[2]
			for video in list_video:

				file_video = os.path.join(path_video, video)
				#file_name = video[0:video.rfind('.') ]+'_%03d' + '.png'
				file_name = video+'.%03d' + '.png'
				file_image = os.path.join(path_image, file_name)
				if not os.path.exists(file_image):
					#ffmpeg -i input.flv -vf "select='eq(pict_type,PICT_TYPE_I)'" -vsync vfr thumb%04d.png
					subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])
					#print(file_image)


		start += timedelta(1)


# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# convertVideoToAudio(path_data, '2016-10-01', '2017-06-29')


if __name__ == '__main__':
    from sys import argv
    path_data = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, date, to_date = argv
    convertVideoToImage(path_data, date, to_date)
