import caculator as cd
import os, os.path
#from os.path import splitext, basename, join
import json
import csv
import numpy as np
from itertools import combinations


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
    return fullfilename
#================================== Create predict ===============================================
#[0 'label',1 'label_count',2 'percent_label',3 'previous_p_label',4 'previous_next_label',5'sum_p_label',
#6 'image_count',7 'percent_image',8 'previous_p_image',9 'previous_next_image',10 'sum_p_label',11 'number_edge']
def label_bigger_percent(percent, list_in):
    list_bigger = []
    for label in list_in:
        if label[5] <= percent:
            list_bigger.append(label[0])
    return list_bigger

def label_relationship_bigger_percent(list_bigger, lable_unique, arr_relationship, number_relationship):
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
                if number_relationship != -1:
                    if int(arr_relationship[i][j]) >= number_relationship:
                        # Label lien ket do co thuoc list label > percent
                        if lable_unique[j] in list_bigger:
                            label_relationship.append(label)
                            break
    return label_relationship

def read_list_percent_and_array_relationship(path_percent, path_relationship):
    list_percent = []
    lable_unique = []
    with open (path_percent, 'r') as csvfile:
        reader=csv.reader(csvfile)
        for i, row in enumerate(reader):
            list_percent.append([row[0], int(row[1]), row[2], row[3], row[4], float(row[5]), row[6],
                                 row[7], row[8], row[9], float(row[10]), row[11]])
            lable_unique.append(row[0])

    size = len(list_percent)
    arr_relationship = np.zeros((size, size), dtype=np.int)
    with open (path_relationship, 'r') as csvfile:
        reader=csv.reader(csvfile)
        for i, row in enumerate(reader):
            for j, value in enumerate(row):
                arr_relationship[i][j] = int(value)
    return (list_percent, arr_relationship, lable_unique)


def create_dataset_predict(path_percent, path_relationship, percent, number_relationship):
    if os.path.exists(path_percent) and os.path.exists(path_relationship):
        list_percent, arr_relationship, lable_unique = read_list_percent_and_array_relationship(path_percent, path_relationship)
        # Lấy tập label thuộc percent %
        list_bigger = label_bigger_percent(percent, list_percent)
        # Lay tập label có quan hệ với tập percent %
        label_relationship = label_relationship_bigger_percent(list_bigger, lable_unique, arr_relationship, number_relationship)
        return (list_bigger, label_relationship)


#==================================== Predict for each json ============================================
def check(list_label, list_bigger, label_relationship):
    if list_label == []:
        return False

    flag = True
    for label in list_label:
        if label not in list_bigger:
            if label not in label_relationship:
                flag = False
                break
    return flag

def check_percent(list_label, list_bigger, label_relationship, percent_face):
   #from itertools import combinations
   list_new_list_label = []
   num_remain = int(float(len(list_label) * percent_face / 100))
   list_new_list_label = list(combinations(list_label, num_remain))
   for label in list_new_list_label:
       label = list(label)
       flag = check(label, list_bigger, label_relationship)
       if flag == True:
           return True
   return False
