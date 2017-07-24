import caculator_product as cd
import os, os.path
#from os.path import splitext, basename, join
import json
import csv
import numpy as np
from itertools import combinations

#================================== Create predict ===============================================
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

def create_dataset_predict(path_in, product, percent, date_, to_date_):
    if os.path.exists(path_in):
        list_result, arr_relationship, lable_unique = cd.getData(path_in, product, date_, to_date_)
        list_percent_label = cd.caculator_percent_label(list_result, arr_relationship)
        # Lấy tập label thuộc percent % 
        list_bigger = label_bigger_percent(percent, list_percent_label)
        # Lay tập label có quan hệ với tập percent %
        label_relationship = label_relationship_bigger_percent(list_bigger, lable_unique, arr_relationship)
        return (list_bigger, label_relationship)


#==================================== Predict for each json ============================================
def check(list_label, list_bigger, label_relationship):
    flag = True
    for label in list_label:
        if label not in list_bigger:
            if label not in label_relationship:
                flag = False
                break
    return flag

def predict(path_full_data, file_no_label, product, date_ = '2017-10-01', to_date_ = '0001-01-01'):
    percent = 80
    list_bigger, label_relationship = create_dataset_predict(path_full_data, product, percent, date_, to_date_)
    with open(file_no_label, 'r') as f:
        data = json.load(f)
        for value in data['my_json']:
            list_image_urls = value['audit_content']['image_urls']
            print (value['list_product'])
            print (value['audit_content'])
            for i in range(len(list_image_urls)):
                if 'image_label' in list_image_urls[i]:
                    list_label = list_image_urls[i]['image_label']
                    if list_label == []:
                        print ("Khon thuoc product %s" %product)
                        break
                    result = check(list_label, list_bigger, label_relationship)
                    print (list_image_urls[i]['image_url'])
                    if result:
                        print ("Thuoc product %s" %product)
                    else:
                        print ("Khon thuoc product %s" %product)
            print ("=====================================================================================")


#==================================== Predict on all data ===============================================
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
                    if label not in label_relationship:
                        flag = True
            if flag == True and list_image_label != []:
                image_not_in_dataset.append(value['image_url'])
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

def image_not_in_dataset(path_content, path_full_data, path_down_load_file, product, date_, to_date_):
    percent = 80
    list_bigger, label_relationship = create_dataset_predict(path_content, product, percent, date_, to_date_)
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
    # for link in image_not_in_dataset:
    #     down_load_file(link, path_down_load_file)



#======================================= RUN TEST ===============================================
# path_content = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_content_local.csv'
path_full_data = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path_down_load_file = 'C:/Users/CPU10145-local/Desktop/image'


# path_content = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path_full_data = 'E:/VNG/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
date_ = '2017-10-01'
to_date_ = '0001-01-01'
product = "242"
file_no_label = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON/2016-10-16/ads_creatives_audit_content_2016-10-16.json'
file = 'C:/Users/CPU10145-local/Desktop/test.json'
predict(path_full_data, file_no_label, product, date_ = '2017-10-01', to_date_ = '0001-01-01')

