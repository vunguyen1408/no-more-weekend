import os, os.path
import pandas as pd
import json
import subprocess



# def getLength(filename):
#   result = subprocess.Popen("ffprobe " + filename)
#   return result

filename = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2016-10-01/videos/2016-10-01_9.mp4'


def getLength(input_video):
	result = subprocess.check_output(['ffprobe', '-i', input_video, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=%s' % ("p=0")])
	return float(result)

print (getLength(filename))



