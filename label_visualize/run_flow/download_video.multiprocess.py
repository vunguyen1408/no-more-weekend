"""
    Project : Online marketing tool - Audit content - Audit video
    Company : VNG Corporation

    Description: Download video for every date

    Examples of Usage:
        python download_video.py 2016-10-01 2017-06-29
"""

# Tong ket danh sach cac label cua file Json
import os, os.path
import json
from datetime import datetime , timedelta, date
import time
import urllib.request
import re
import sys

#import Download_Parallel


from multiprocessing import Process, Manager
import itertools
from hash_md5 import hash_md5


def do_work(folder_video,in_queue, out_list):


    while True:
        item = in_queue.get()
        #line_no, line = item
        i1,value1 = item
        #print (item)

        # exit signal
        #if line == None:
        #print(line)
        if 'None->Exit' in value1 :
            #print('exit')
            return

        # work
        time.sleep(.1)
        #loop 2



        down = True
        file_name1=""
        url1=""
        url1 = get_url_video(value1['page_id'], value1['video_id'])
        if url1 != "":
            file_name1 = parse_file_name(url1)
            value1['file_name'] = file_name1
            value1['video_renamed']=1 #after 2017-11-01

            #=================== Download
            file_name=os.path.join(folder_video, file_name1)
            if not os.path.exists(file_name):
                info = get_info(url1)
                if info.get('size',0):
                    #print(file_name)
                    download_file(url1, file_name, info['size'])
                    if os.path.exists(file_name) and  os.path.getsize(file_name) >0:
                        value1['hash_md5']=hash_md5(file_name)

                    #Download_Parallel.DownloadFile_Parall(url1, file_name, 4)
                    #obj = SmartDL(url1, file_name)
                    #obj.start()
                #
            else:
                if 'hash_md5' not in value1:
                    if os.path.exists(file_name) and  os.path.getsize(file_name) >0:
                        value1['hash_md5']=hash_md5(file_name)
                    




        #print(result)
        result = (i1,value1)

        out_list.append(result)






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
        #print (url)
    #print('video_url',video_url)
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
    #print('info',info)
    return info

def download_file(url, filename, size):
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
                    #print ("ket noi lai..............")
                    h_url = urllib.request.urlopen(url)
                    flag = False
                except urllib.request.URLError:
                    print ('error : cannot open url')
            if flag:
                if len(info) < 1 :
                    break
                file.write(info)
        print ("========= Down load complete ============")


def check_and_download_file(path_folder, path_file, folder, p_process_num):
    from pySmartDL import SmartDL

    with open(path_file, 'r') as f:
        work_json = json.load(f)

    folder_video = os.path.join(path_folder, 'videos')

    if not os.path.exists(folder_video):
        os.makedirs(folder_video)

    #multiprocessing
    num_workers = int(p_process_num)

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=do_work, args=(folder_video, work, results))
        p.start()
        pool.append(p)

    # produce data
    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
    #print(type(work_json))
    iters = itertools.chain(work_json['my_json'], ({'None->Exit'},)*num_workers)
    for num_and_line in enumerate(iters):
        work.put(num_and_line)

    for p in pool:
        p.join()

    return_json={}
    return_json['my_json']=[]

    for _json in results:
        return_json['my_json'].append(_json[1])


    with open (path_file,'w') as f:
        json.dump(return_json, f)





def create_hash_file(path_folder, path_file, folder):
	with open(path_file, 'r') as f:
		data = json.load(f)


	list_result = []

	for _json in  data['my_json']:


		if 'hash_md5' in _json:

			#print(_json['hash_md5'])

			temp_hash=_json['hash_md5']
			temp_file=_json['file_name']

			find_hash = -1
			for _i,_hash in enumerate(list_result):
				#print(_i)
				if _hash['hash_md5']==temp_hash:

					#print(list_result[_i]['file_name'])
					find_file=-1
					for _file  in list_result[_i].get('file_name',[]):
						if _file==temp_file:
							find_file=1
							break

					if find_file < 0:
                    #append
						list_result[_i]['file_name'].append(temp_file)
					find_hash=_i
					break

			if find_hash <0:
				new_hash={}
				new_hash['hash_md5']=temp_hash
				new_hash['file_name']=[]
				new_hash['file_name'].append(temp_file)
				list_result.append(new_hash)



	list_json = {}
	list_json['hash_md5'] = list_result
	file_name = os.path.join(path_folder, ('video_hash_' + str(folder) + '.json'))

	with open (file_name,'w') as f:
		json.dump(list_json, f)



def download_video(path, date_, to_date_, process_num=1):
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
                check_and_download_file(path_folder, path_file, folder,process_num)
                create_hash_file(path_folder, path_file, folder)
            print("---------------------------------------------------------------")


# # path = 'D:/WorkSpace/GITHUB/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# # path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# date = '2016-12-11'
# to_date = '2016-12-11'
# create_content_date(path, date, to_date)


if __name__ == '__main__':
    from sys import argv



    script, start_date, end_date, process_num = argv
    path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    download_video(path, start_date, end_date,process_num)
