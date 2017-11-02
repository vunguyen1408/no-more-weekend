"""
	Project : Online marketing tool - Audit content - Audit audio
	Company : VNG Corporation

	Description: Convert video to audio

    Examples of Usage:
    	python video_to_audio.multiprocess.py 2016-10-01 2017-06-29
"""


import os
import json
import subprocess
import glob,itertools,time
from datetime import datetime , timedelta, date
from multiprocessing import Process, Manager


def do_work(path_video,path_work, in_queue, out_list):

    while True:
        item = in_queue.get()
        #line_no, line = item
        line_no, video = item

        # exit signal

        if 'None->Exit' in video :
            #print('exit')
            return

        # work




        time.sleep(.1)
        file_video = os.path.join(path_video, video)
        #file_name = video[0:video.rfind('.') ]+'_%03d' + '.png'
        file_name_check = video+ '.flac'
        file_work_check = os.path.join(path_work, file_name_check)
        file_name = video+ '.flac'
        file_work = os.path.join(path_work, file_name)

        #if not glob.glob(file_work_check):
        #    subprocess.call(["ffmpeg", "-i", file_video,"-vf", "select='eq(pict_type,PICT_TYPE_I)'","-vsync","vfr",file_image ])

        if not os.path.exists(file_work):
            subprocess.call(["ffmpeg", "-i", file_video,"-c:a", "flac", file_work])

        if os.path.exists(file_work):
            subprocess.call(["sox", file_work, "--channels=1", "--bits=16", file_work])


        result = (line_no, video)


        out_list.append(result)



def convertVideoToAudio(path_data, start_date, end_date, p_process_num):
	start = datetime.strptime(start_date, '%Y-%m-%d').date()
	end = datetime.strptime(end_date, '%Y-%m-%d').date()


	while(start <= end):
		path_date = os.path.join(path_data, start.strftime('%Y-%m-%d'))
		path_video = os.path.join(path_date, 'videos')

		if os.path.exists(path_video):
			path_work = os.path.join(path_date, 'video_audios')
			if not os.path.exists(path_work):
				os.makedirs(path_work)

			list_video = next(os.walk(path_video))[2]

			#multiprocessing
			num_workers = int(p_process_num)

			manager = Manager()
			results = manager.list()
			work = manager.Queue(num_workers)

			# start for workers
			pool = []
			for i in range(num_workers):
			    p = Process(target=do_work, args=(path_video,path_work, work, results))
			    p.start()
			    pool.append(p)

		    # produce data
		    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
		    #print(type(work_json))
			iters = itertools.chain(list_video, ({'None->Exit'},)*num_workers)
			for num_and_line in enumerate(iters):
			    work.put(num_and_line)

			for p in pool:
			    p.join()

			##


		start += timedelta(1)


# path_data = '/u01/app/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
# convertVideoToAudio(path_data, '2016-10-01', '2017-06-29')


if __name__ == '__main__':
    from sys import argv
    path_data = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON'
    script, date, to_date ,process_num = argv
    convertVideoToAudio(path_data, date, to_date, process_num)
