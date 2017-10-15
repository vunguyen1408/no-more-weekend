# Tong ket danh sach cac label cua file Json
import os, os.path
import json
from datetime import datetime , timedelta, date
import time

def create_content_file(path_folder, path_file, folder):
	with open(path_file, 'r') as f:
		data = json.load(f)
	list_result = []
	for i, value in enumerate(data['my_json']):
		page_id = ""
		if 'object_story_spec' in value:
			if 'page_id' in value['object_story_spec']:
				page_id = value['object_story_spec']['page_id']
		#================ Get list video
		list_video = []
		if 'video_ids' in value['audit_content']:
			list_video = value['audit_content']['video_ids']
		#=============== Parse link video
		for j, video_id in enumerate(list_video):
			if page_id != "" and video_id != "":
				id_v = video_id['video_id']
				# video_url = get_link_video(page_id, id_v)
				video_json = {
						'index_json': i,
						'index_video': j,
						'page_id': page_id,
						'video_id': id_v,
						# 'video_url': video_url,
						'video_label': []
				}
				list_result.append(video_json)
	list_json = {}
	list_json['my_json'] = list_result
	file_name = os.path.join(path_folder, ('video_url_' + str(folder) + '.json'))
	with open (file_name,'w') as f:
		json.dump(list_json, f)

def create_content_date(path, date_ = '2016-10-11', to_date_ = '2017-05-01'):
	# Lấy danh sách path của các file json cần tổng hợp data
	list_file = []
	date = datetime.strptime(date_, '%Y-%m-%d').date()
	to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
	list_folder = next(os.walk(path))[1]
	for folder in list_folder:
		f_date = datetime.strptime(folder, '%Y-%m-%d').date()
		print (folder)
		if f_date <= to_date and f_date >= date:
			path_folder = os.path.join(path, folder)
			file_name = "ads_creatives_audit_content_"+ folder +".json"
			path_file = os.path.join(path_folder, file_name)
			if os.path.exists(path_file):
				# time.sleep(5)
				create_content_file(path_folder, path_file, folder)
				print (folder)
				print ("------------------------------------")

#path = 'C:\\Users\\CPU10145-local\\Desktop\\Python Envirement\\Data\\Date'
#path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path = 'D:/WorkSpace/GITHUB/DATA/DATA/DWHVNG/APEX\MARKETING_TOOL_02_JSON'
date = '2017-05-02'
to_date = '2017-07-01'
path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
create_content_date(path, date, to_date)
