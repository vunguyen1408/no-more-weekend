# Tong ket danh sach cac label cua file Json
import os, os.path
import json
import csv
from datetime import datetime , timedelta, date


def get_content_label_file(path_file):

    number = 0
    list_result = []
    list_row_unique = []
    with open(path_file, 'r') as f:
        data = json.load(f)
    for value in data['sample_json']:
        label_image = value['video_label']
        if len(label_image) > 0:
            number += 1
            if label_image not in list_row_unique:
                list_row_unique.append(list(label_image))
                temp = list(label_image)
                temp.append(1)
                list_result.append(temp)
            else:
                index = list_row_unique.index(label_image)
                list_result[index][len(list_result[index]) - 1] += 1
    print ("Get data completed...!")
    return (list_result, list_row_unique, number)


# path_file = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data product/sample_content.json'
# list_result, list_row_unique = get_content_label_list_file(path_file)
# for i in list_result:
#     print (i)
