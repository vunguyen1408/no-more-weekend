import sys
import os
import json
import cx_Oracle
"""
    Project : Online marketing tool - Audit content
    Company : VNG Corporation

    Description: Insert data audit of Ads to Database
    
    Examples of Usage:
        python insert_audit_ads_to_data.py 2016-10-01 2017-06-29
"""


from datetime import datetime , timedelta, date
import time

def check_file_exist(photo_link, path_down_load_file):
    import io
    import os

    try:
        from urllib.request import urlretrieve  # Python 3
        from urllib.error import HTTPError,ContentTooShortError
    except ImportError:
        from urllib import urlretrieve  # Python 2



    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2
    from os.path import splitext, basename, join
    picture_page = photo_link
    disassembled = urlparse(picture_page)
    filename, file_ext = splitext(basename(disassembled.path))
    filename = filename + file_ext
    fullfilename = os.path.join(path_down_load_file, filename)

    if not os.path.exists(fullfilename):

	    #download
	    try:
	        urlretrieve(photo_link, fullfilename)

	    except HTTPError as err:
	        return '0'
	        print(err.code)
	    except ContentTooShortError as err:
	        #retry 1 times
	        try:
	            urlretrieve(photo_link, fullfilename)
	        except ContentTooShortError as err:
	            print(err.code)
	            return '0'
    return fullfilename

#-------------- Do data audit ------------------
def InsertContentAds(cursor, ads, d):
	statement = 'insert into STG_AUDIT_CONTENT ( \
	AD_ID, PRODUCT_ID, CONTENT, TYPE, PREDICT_PERCENT, \
	INDEX_CONTENT, SNAPSHOT_DATE, INSERT_DATE, FLAG) \
	values (:1, :2, :3, :4, :5, :6, :7, :8, :9)'
	print (ads['list_product'])
	print (ads['ad_id'])
	path_down_load_file = '/u01/oracle/oradata/APEX/MARKETING_TOOL_03/temp'
	# now = datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date()
	# print (ads['audit_content'])
	if ads['list_product'] != [] and 'audit_content' in ads:
		#-------- Insert image ---------------
		list_image = ads['audit_content']['image_urls']
		if list_image != []:
			for i, image in enumerate(list_image):
				if 'percent_predict' in image:
					try:
						file_name = check_file_exist(image['image_url'], path_down_load_file)
					except ContentTooShortError as e:
						file_name = '0'
					if file_name == '0':
						flag = 0
					else:
						flag = 1
					cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], image['image_url'], 'image_url', image['percent_predict'], i,  \
					datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), flag))

		list_thumbnail = ads['audit_content']['thumbnail_urls']
		if list_thumbnail != []:
			for i, thumbnail in enumerate(list_thumbnail):
				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], thumbnail['thumbnail_url'], 'thumbnail_url', 0, i,  \
				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), 0))

		links = ads['audit_content']['links']
		if links != []:
			for i, link in enumerate(links):
				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], link['link'], 'link', 0, i,  \
				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), 0))

		messages = ads['audit_content']['messages']
		if messages != []:
			for i, message in enumerate(messages):
				try:
					cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], message['message'], 'message', 0, i,  \
					datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), 0))
				except:
					try :
						cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], message['message'].encode('utf-8'), 'message', 0, i,  \
						datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), 0))
					except:
						print ("Qua dai")

		video_ids = ads['audit_content']['video_ids']
		if video_ids != [] and 'object_story_spec' in ads:
			for i, video_id in enumerate(video_ids):
				link = 'https://www.facebook.com/' + str(ads['object_story_spec']['page_id']) + '/videos/' + str(video_id['video_id'])
				cursor.execute(statement, (ads['ad_id'], ads['list_product'][0], link, 'video_id', 0, i,  \
				datetime.strptime(d, '%Y-%m-%d'), datetime.strptime((time.strftime('%Y-%m-%d')), '%Y-%m-%d').date(), 0))

def add_label_video_to_data(connect, path, date_, to_date_):
	# Lấy danh sách path của các file json cần tổng hợp data
	list_folder = next(os.walk(path))[1]

	#========================== Auto run ===================
	conn = cx_Oracle.connect(connect, encoding = "UTF-8", nencoding = "UTF-8")
	cursor = conn.cursor()
	date = datetime.strptime(date_, '%Y-%m-%d').date()
	to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
	for folder in list_folder:
		print (folder)
		d = datetime.strptime(folder, '%Y-%m-%d').date()
		if d <= to_date and d >= date:
			path_folder = os.path.join(path, folder)
			path_file = os.path.join(path_folder, 'ads_creatives_audit_content_' + str(folder) + '.json')
			print (path_file)
			print ("--------------------")
			if os.path.exists(path_file):
				with open(path_file, 'r') as f:
					data = json.load(f)
					for ads in data['my_json']:
						print ('ads====================')
						InsertContentAds(cursor, ads, str(d))
		conn.commit()
	cursor.close()

if __name__ == '__main__':
    from sys import argv
    path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'  
    connect = 'MARKETING_TOOL_01/MARKETING_TOOL_01_9999@10.60.1.42:1521/APEX42DEV'  
    script, date, to_date = argv
    add_label_video_to_data(connect, path, date, to_date)