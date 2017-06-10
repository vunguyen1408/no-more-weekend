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

import argparse
import base64
import os

#base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"

def label(photo_link):

    # [START vision_quickstart]
    import io
    import os

    #import argparse
    #import base64

    #import googleapiclient.discovery

#
    try:
        from urllib.request import urlretrieve  # Python 3
    except ImportError:
        from urllib import urlretrieve  # Python 2
#import urllib.request


    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2


    from os.path import splitext, basename, join


    # Imports the Google Cloud client library
    from google.cloud import vision

    pdate=vdate

    #return
    list_label=[]

    # Instantiates a client
    vision_client = vision.Client()

    picture_page = photo_link
    disassembled = urlparse(picture_page)
    filename, file_ext = splitext(basename(disassembled.path))

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    base_dir="/u01/oracle/oradata/APEX/MARKETING_TOOL_02/"+pdate+"/images"

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    fullfilename = join(base_dir, filename+file_ext)
    #fullfilename = join("resources", filename+file_ext)
    urlretrieve(photo_link, fullfilename)
    photo_file=fullfilename
    print(photo_file)

    # The name of the image file to annotate
    #file_name = os.path.join(
    #    os.path.dirname(__file__),
    #    'resources/wakeupcat.jpg')

    # Loads the image into memory
    with io.open(fullfilename, 'rb') as image_file:
        content = image_file.read()
        image = vision_client.image(
            content=content)

    # Performs label detection on the image file
    labels = image.detect_labels()

    print('Labels:')
    for label in labels:
        print(label.description)
        list_label.append(label.description)
    # [END vision_quickstart]
    return         list_label



def get_json(files):
    import csv
    import json
    from pprint import pprint
    import pandas as pd

    with open(files) as csvfile:
        reader = csv.reader(csvfile , delimiter='/,/ /[', quoting=csv.QUOTE_NONE)
        for row in reader:
            print(row[1].strip("]"))
            #json.dumps( [ row for row in reader ] )
            #pprint ( json.dumps(row[1].strip("]"), indent=4) )
            #d = json.load(row[1].strip("]"))
            json_str = json.dumps(row[1].strip("]"))
            #pprint(json_str)
            with open('data.json', 'w') as json_file:
                json.dump(json_str, json_file)


def get_json2(files):
    import csv
    import json
    from pprint import pprint

    with open(files) as json_data:
        d = json.load(json_data)
        print(d)


def get_json3(list_file):
    import csv
    import json
    import pandas as pd
    import re
    from pprint import pprint

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
                list_json.append(json_data)



    #print(list_[0]["body"])
    #print(list_json[0]["body"],list_json[0]["image_url"])
    return list_json;

def parse_ads_creatives_csv_to_json(pdate):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    base_dir="/u01/oracle/oradata/APEX/MARKETING_TOOL_02/"
    wrk_dir=os.path.join(base_dir, pdate)

    ads_creatives_file_name = "ads_creatives_"+pdate+".json"
    ads_creatives_file= os.path.join(wrk_dir, ads_creatives_file_name)
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

    with open (ads_creatives_file,'w') as f:
        json.dump(list_json,f)

def parse_ads_creatives_json_to_label(pdate):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    base_dir="/u01/oracle/oradata/APEX/MARKETING_TOOL_02/"
    wrk_dir=os.path.join(base_dir, pdate)

    ads_creatives_file_name = "ads_creatives_"+pdate+".json"
    image_url_file_name = "image_url_"+pdate+".json"
    image_url_file= os.path.join(wrk_dir, image_url_file_name)

    list_file = []
    list_json = []
    list_image_json = []

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
            for row in reader:
                list_json.append(row)
                #print(row["image_url"])


    # de han che so luong call api, cac url can duoc kiem tra da co chua
    # trong file image_url_lablel_yyyymmdd.json
    if os.path.exists( image_url_file ) and os.stat(image_url_file).st_size  > 0  :
        try:
            with open (image_url_file,'r') as file_json:
                reader=json.load(file_json)
                for row in reader:
                    list_image_json.append(row)
        except IOError as e:
            # you can print the error here, e.g.
            print(str(e))



    for i in list_json:
        #print(json["image_url"])
        print(i)
        print(i[0])
        if i[0]["object_type"] == 'SHARE':

            #check duplicate trong list
            # if exist then check null value
            #
            exists = False
            x=0
            y=-1 #null_position
            for image in list_image_json:
                #print(type(image))
                #if image["image_url"] ==  i["image_url"] and image["image_label"] !="":
                if image["image_url"] ==  i[0]["image_url"]:
                    exists = True
                    if image["image_label"]=="":
                        y=x
                    break
                x+=1

            print(exists)
            print (y)

            #get label
            if exists == False:
                image_label=label(i[0]["image_url"])
                #image_label=""

                #create dict
                image_url_json={
                                 "image_url"    : i[0]["image_url"]
                                ,"image_label"  : image_label
                                }
                #append
                list_image_json.append(image_url_json)
            else:
            # exist = True
                if y >=0 :
                    # update value
                    image_label=label(i[0]["image_url"])
                    list_image_json[y]["image_label"]=image_label



            #label(image_url)

        elif i["object_type"] == 'VIDEO':
            print(i["object_story_spec"]["video_data"]["image_url"])
        #'object_type' == 'SHARE'; id, call_to_action_type, object_type, object_story_spec.link_data.link, image_url, object_story_spec.link_data.message
        #'object_type' == 'VIDEO'; id, call_to_action_type, object_type, object_story_spec.video_data.call_to_action.value.link, object_story_spec.video_data.image_url, object_story_spec.link_data.object_story_spec.video_data.description


    #print (type(list_image_json))
    #print (list(set(list_image_json)))
    with open (image_url_file,'w') as f:
        json.dump(list_image_json,f)


        #print (json_)
       #print(json_data)
    #print(json_data[0]["image_url"])
    #print(type(df))
    #print(list_[0]["body"])
    #print(list_json[0]["body"],list_json[0]["image_url"])
    #frame = pd.concat(list_)
    #'object_type' == 'SHARE'; id, call_to_action_type, object_type, object_story_spec.link_data.link, image_url, object_story_spec.link_data.message
    #'object_type' == 'VIDEO'; id, call_to_action_type, object_type, object_story_spec.video_data.call_to_action.value.link, object_story_spec.video_data.image_url, object_story_spec.link_data.object_story_spec.video_data.description










if __name__ == '__main__':
    #parser = argparse.ArgumentParser()
    #parser.add_argument('image_link', help='The image link you\'d like to label.')
    #args = parser.parse_args()
    #main(args.image_file)
    #label(args.image_link)
    vdate="2017-05-01"
    parse_ads_creatives_csv_to_json(vdate)
    parse_ads_creatives_json_to_label(vdate)
