#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    Project : Online marketing tool - Audit content
    Company : VNG Corporation

    Description: Parse data to file json audit content
    
    Examples of Usage:
        python parse_json_to_audit_content.py 2016-10-01 2017-06-29
"""
import argparse
import base64
import os


def _finditem(obj, key):
    if key in obj:
        return obj[key]
    for k, v in obj.items():
        if isinstance(v,dict):
            item = _finditem(v, key)
            if item is not None:
                return item

def _finditem2(obj, key):
    list_item=[]
    if key in obj:
        list_item.append( obj[key] )
    for k, v in obj.items():
        if isinstance(v,dict):
            item = _finditem(v, key)
            if item is not None:
                list_item.append(item)
    return list(set(list_item))

def get_json3(list_file):
    import csv
    import json
    import pandas as pd
    import re
    from pprint import pprint
    import time

    frame = pd.DataFrame()
    list_json = []

    for files_ in list_file:
        with open(files_) as rawfile:
            for row in rawfile:
                data = row.split(", ",1) # split by first comma

                #get ads_id
                pattern =  "(\d+)"
                matchObject = re.search(pattern, data[0], flags=0)
                ads_id=matchObject.group()

                #get json
                json_data = json.loads(data[1])
                json_data[0]['ad_id']=ads_id # bo sung ads_id vao json
                #print(json_data[0]["ad_id"])
                list_json.append(json_data[0])


    return list_json
    #print(list_[0]["body"])
    #print(list_json[0]["body"],list_json[0]["image_url"])


def parse_ads_creatives_csv_to_json(pdate, base_dir, base_dir_json):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json
    import time

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env


    wrk_dir=os.path.join(base_dir, pdate)
    json_dir=os.path.join(base_dir_json, pdate)

    if not os.path.exists(json_dir):
        os.makedirs(json_dir)

    ads_creatives_file_name = "ads_creatives_"+pdate+".json"
    ads_creatives_file= os.path.join(json_dir, ads_creatives_file_name)
    list_file = []
    list_json = []


    for root, dirs, files in os.walk(wrk_dir):
        for f in files:
            fullpath = os.path.join(root, f)

            #if os.path.splitext(fullpath)[1] == '.txt':
            if 'creatives.txt' in fullpath:
                print (fullpath)
                #get_json(fullpath)
                list_file.append(fullpath)

    #print(list_files)
    list_json = get_json3(list_file)
    final_json={}
    final_json['my_json']=list_json
    with open (ads_creatives_file,'w') as f:
        json.dump(final_json,f)

def parse_ads_creatives_json_audit_content(pdate, base_dir):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json
    import time


    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    wrk_dir=os.path.join(base_dir, pdate)

    ads_creatives_file_name = "ads_creatives_"+pdate+".json"
    ads_creatives_audit_content_file_name = "ads_creatives_audit_content_"+pdate+".json"

    ads_creatives_audit_content_file= os.path.join(wrk_dir, ads_creatives_audit_content_file_name)

    list_file = []
    list_json = []

    #audit content object
    list_audit_context = ['image_url','thumbnail_url','link','video_id','message']

    #find all file
    for root, dirs, files in os.walk(wrk_dir):
        for f in files:
            fullpath = os.path.join(root, f)

            #if os.path.splitext(fullpath)[1] == '.txt':
            if ads_creatives_file_name in fullpath:
                #print (fullpath)
                #get_json(fullpath)
                list_file.append(fullpath)

    #get all data
    for file_ in list_file:
        with open (file_,'r') as file_json:
            reader=json.load(file_json)
            for row in reader['my_json']:
                list_json.append(row)
                #print(row["image_url"])


    #get audit content
    position=0
    for i in list_json:
        #print(json["image_url"])
        #print(type(i[]))
        audit_content={}
        #get content
        for j in list_audit_context:

            audit_content_object=j+'s' #name
            #print(audit_content_object)
            audit_content[audit_content_object]=[]


            #for k in _finditem2(i[0],j):
            for k in _finditem2(i,j):
                #add dict content
                content = {}
                content[j] = k
                #print(k)

                audit_content[audit_content_object].append(content)

        #print(type(list_json[position][0]))
        #print(type(list_json[i]))
        #list_json[position][0]['audit_content']=audit_content
        list_json[position]['audit_content']=audit_content
        position+=1


    final_json={}
    final_json['my_json']=list_json
    #print(list_json[0][0]['audit_content']['image_urls'])
    with open (ads_creatives_audit_content_file,'w') as f:
        json.dump(final_json,f)




# Lua chon chay tung ngay

def main(date_, to_date_):
    """Run a label request """
    from datetime import datetime , timedelta, date
    path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02'
    path_json ="/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/"
    list_folder = next(os.walk(path))[1]
    # date_ = '2017-05-06'
    # to_date_ = '2017-06-29'
    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    for folder in list_folder:
        # ---------- Check cac folder khong phai date ------------------
        if folder[:4].isdigit():
            d = datetime.strptime(folder, '%Y-%m-%d').date()
            if d <= to_date and d >= date:
                try:
                    parse_ads_creatives_csv_to_json(folder, path, path_json)
                    #analyze_ads_creatives_json(vdate)
                    parse_ads_creatives_json_audit_content(folder, path_json)
                    # label_ads_creatives_json_audit_content(folder, path_json)
                except:
                    print ("Date error: %s" %folder)

if __name__ == '__main__':
    from sys import argv
    script, start_date, end_date = argv
    main(start_date, end_date)
