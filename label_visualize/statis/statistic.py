import os, os.path
import pandas as pd
import json
from dateutil.relativedelta import *
from datetime import datetime ,timedelta, date
import pandas as pd
import subprocess





def getLength(input_video):
	result = subprocess.Popen('ffprobe -i ' + input_video + ' -show_entries format=duration -v quiet -of csv="p=0"', stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
	output = result.communicate()
	return float(output[0])



def Daily(path_data, date):
	
	number_image = 0
	file_image = path_data + '/' + date + '/image_url_' + date + '.json'
	# ------------------- Number image -----------------------
	if os.path.exists(file_image):

		with open (file_image,'r') as f:
			image = json.load(f)
		number_image = len(image['my_json'])

	number_video = 0
	path_video_folder = path_data + '/' + date + '/videos'
	# ------------------ Time video ----------------------
	if os.path.exists(path_video_folder):
		dir_ = next(os.walk(path_video_folder))[0]
		list_file = next(os.walk(path_video_folder))[2]
		for file in list_file:
			file = dir_ + '/' + file
			number_video += getLength(file)

	return (number_image, number_video)


def count_image(path_data, date_, to_date_):

	date_ = datetime.strptime(date_, '%Y-%m-%d').date()
	to_date_ = datetime.strptime(to_date_, '%Y-%m-%d').date()
	n = int((to_date_ - date_).days)
	with open('statictis.txt', 'a') as f:
		for i in range(n + 1):
			single_date = date_ + timedelta(i)

			d = single_date.strftime('%Y-%m-%d')
			number_image, number_video = Daily(path_data, str(d))
			print (d , number_image, number_video)
		f.write(str(d) + ' ' + str(number_image) + ' ' + str(number_video) )


month = '2016-10-01'
to_month = '2017-05-30'
path_audit_content = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'

count_image(path_audit_content, month, to_month)