# Tong ket danh sach cac label cua file Json
import os, os.path
import json
"""
    Read each file json of each product. Genarate:
    + list_row_unique: list label unique
    + list_result : list of list label -- frequence
"""
def get_content_label_file(path_file):

    list_result = []
    list_row_unique = []
    with open(path_file, 'r') as f:
        data = json.load(f)
    for value in data['sample_json']:
        label_image = value['image_label']
        if len(label_image) > 0:
            if label_image not in list_row_unique:
                list_row_unique.append(list(label_image))
                temp = list(label_image)
                temp.append(1)
                list_result.append(temp)
            else:
                index = list_row_unique.index(label_image)
                list_result[index][len(list_result[index]) - 1] += 1
    print ("Get data completed...!")
    return (list_result, list_row_unique)




























