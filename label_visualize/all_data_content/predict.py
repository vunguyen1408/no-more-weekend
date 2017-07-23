import caculator_percent as cp
import os, os.path
#from os.path import splitext, basename, join
import json
import csv
import numpy as np
from itertools import combinations


#[0 'label',1 'label_count',2 'percent_label',3 'previous_p_label',4 'previous_next_label',5'sum_p_label',
#6 'image_count',7 'percent_image',8 'previous_p_image',9 'previous_next_image',10 'sum_p_label',11 'number_edge']
def label_bigger_percent(percent, list_in):
    list_bigger = []
    for label in list_in:
        if label[5] <= percent:
            list_bigger.append(label[0])
    return list_bigger


def label_relationship_bigger_percent(list_bigger, lable_unique, arr_relationship):
    # Duyet trong all label
    label_relationship = []
    for label in lable_unique:
        # Neu khong thuoc list > percent
        if label not in list_bigger:
            # Get index
            i = lable_unique.index(label)
            # Duyet tai dong index
            for j in range(len(lable_unique)):
                # Neu co lien ket
                if arr_relationship[i][j] != 0:
                    # Label lien ket do co thuoc list label > percent
                    if lable_unique[j] in list_bigger:
                        label_relationship.append(label)
                        break
    return label_relationship



def get_label_bigger_and_not_exist(path_in):
    if os.path.exists(path_in):
        list_result, arr_relationship, lable_unique = cp.getData(path_in)
        list_percent_label = cp.caculater_percent_label(list_result, arr_relationship)
        list_bigger = label_bigger_percent(50, list_percent_label)
        label_relationship = label_relationship_bigger_percent(list_bigger, lable_unique, arr_relationship)

        return (list_bigger, label_relationship)





def get_content_label_list_file(list_path_file, list_bigger, label_relationship):
    image = []
    image_not_in_dataset = []
    for file_ in list_path_file:
        with open(file_, 'r') as f:
            data = json.load(f)
        for value in data['my_json']:
            flag = False
            list_image_label = value['image_label']
            if len(list_image_label) == 0:
                image.append(list_image_label)
            for label in list_image_label:
                if label not in list_bigger:
                    # flag = True
                    if label not in label_relationship:
                        flag = True
            # if flag == False:
            if flag == True and list_image_label != []:
                image_not_in_dataset.append(value['image_url'])
                # print (value['image_url'])
                # print (value['image_label'])
                # print ("=====================================================================")
    print (len(image))
    return image_not_in_dataset


def down_load_file(photo_link, path_down_load_file):
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
            print(err.code)
        except ContentTooShortError as err:
            #retry 1 times
            try:
                urlretrieve(photo_link, fullfilename)
            except ContentTooShortError as err:
                print(err.code)


def image_not_in_dataset(path_content, path_full_data, path_down_load_file):
    list_bigger, label_relationship = get_label_bigger_and_not_exist(path_content)

    list_file = []
    list_folder = next(os.walk(path_full_data))[1]
    for folder in list_folder:
        path_folder = os.path.join(path_full_data, folder)
        file_name = "image_url_"+ folder +".json"
        path_file = os.path.join(path_folder, file_name)
        if os.path.exists(path_file):
            list_file.append(path_file)
            # print(path_file)
    image_not_in_dataset = get_content_label_list_file(list_file, list_bigger, label_relationship)

    print (len(image_not_in_dataset))
    for link in image_not_in_dataset:
        down_load_file(link, path_down_load_file)




path_content = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_content_local.csv'
path_full_data = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path_down_load_file = 'C:/Users/CPU10145-local/Desktop/image'

image_not_in_dataset(path_content, path_full_data, path_down_load_file)
