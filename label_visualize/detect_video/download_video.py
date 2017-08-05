# Tong ket danh sach cac label cua file Json
import os, os.path
import json
from datetime import datetime , timedelta, date
import time
import urllib.request
import re
import sys

def parse_file_name(url):
    from urllib.parse import urlparse
    parse = urlparse(url)
    temp = str(parse.path)
    file_name = temp.split('/')[-1]
    return file_name

def get_url_video(page_id, video_id):
    from urllib.request import urlopen
    video_url = ""
    url = 'https://www.facebook.com/' + str(page_id) + '/videos/' + str(video_id) + '/'
    try:
        data = urlopen(url).read()
        data = str(data)
        index = data.find('hd_src:"')
        if index >= 0:
            video_url = ((data[index:]).split('"'))[1]
    except:
        print ('error')
        print (url)
    return video_url

def get_info(video_url):
    from urllib.request import urlopen

    info = {'size': 0, 'format': '.mp4'}
    try:
        url_h = urlopen(video_url)
        meta  = url_h.info()
        size  = meta['Content-Length']
        content_type = meta['content-type']
        format = re.search('video/(\w+)', content_type).group(1)
        info['size'] = size
        info['format'] = format
    except urllib.request.URLError:
        print (video_url)
        print ('error : get format video')
    return info

def down_load_file(url, filename, size):
    if os.path.exists(filename):
        print ("Co video---")
        return True
    else:
        from urllib.request import urlopen

        try:
            file = open(filename, 'wb')
        except IOError:
            print ('cannot access file ' + filename)
            size = int(size)
        try:
            h_url = urllib.request.urlopen(url)
        except urllib.request.URLError:
            print ('error : cannot open url')
        # try:
        flag = False
        while True:
            try:
                info = h_url.read(1024 * 8)
                flag = True
            except Exception:
                try:
                    file = open(filename, 'wb')
                except IOError:
                    print ('cannot access file ' + filename)

                try:
                    print ("ket noi lai..............")
                    h_url = urllib.request.urlopen(url)
                    flag = False
                except urllib.request.URLError:
                    print ('error : cannot open url')
            if flag:
                if len(info) < 1 :
                    break
                file.write(info)
        print ("========= Down load complete ============")
    

def down_load_file_folder(path_folder, path_file, folder):
    with open(path_file, 'r') as f:
        data = json.load(f)
    folder_video = os.path.join(path_folder, 'videos')

    if not os.path.exists(folder_video):
        os.makedirs(folder_video)
    list_result = []
    list_download = []
    down = True
    for i1, value1 in enumerate(data['my_json']):
        down = True
        url1 = get_url_video(value1['page_id'], value1['video_id'])
        if url1 != "":
            file_name1 = parse_file_name(url1)
            data['my_json'][i1]['file_name'] = file_name1
            for i2 in range(0, i1):
                value2 = data['my_json'][i2]
                file_name2 = value2['file_name']
                if file_name1 == file_name2:
                    if i2 in list_download:
                        down = False
                        break
            if down:
                #=================== Download
                info = get_info(url1)
                if (int(info['size'])) > 0:
                    # print (info)
                    file_name = os.path.join(folder_video, (str(folder) + '_' + str(i1) + '.' + info['format']))
                    if not os.path.exists(file_name):
                        down_load_file(url1, file_name, info['size'])
                        list_download.append(i1)
                        # time.sleep(5)
                    else:
                        print ("Co video---")
        else:
            print ("url hong.....")
            data['my_json'][i1]['file_name'] = ""
    with open (path_file,'w') as f:
        json.dump(data, f)


def create_content_date(path, date_ = '2016-10-01', to_date_ = '2017-05-01'):
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    list_folder = next(os.walk(path))[1]
    for folder in list_folder:
        f_date = datetime.strptime(folder, '%Y-%m-%d').date()
        if f_date <= to_date and f_date >= date:
            print (folder)
            path_folder = os.path.join(path, folder)
            file_name = "video_url_"+ folder +".json"
            path_file = os.path.join(path_folder, file_name)
            if os.path.exists(path_file):
                # time.sleep(5)
                down_load_file_folder(path_folder, path_file, folder)
            print("---------------------------------------------------------------")


# path = 'D:/WorkSpace/GITHUB/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
date = '2016-10-01'
to_date = '2016-10-01'
create_content_date(path, date, to_date)


