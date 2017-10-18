# package
import get_content_crawler as get_content_crawler

import os, os.path
#from os.path import splitext, basename, join
import csv
import numpy as np
import json
from itertools import combinations
import pandas as pd


# Lấy list label chứa (label, label_count, image_count) và (array)
def getData(path_file_content):
    edge_data = []
    lable_unique = []
    list_json = []
    list_result, label_image, number = get_content_crawler.get_content_label_file(path_file_content)

    for row in list_result:
        # Xóa phần tử cuối cùng của dòng (phần tử frequence)
        row_content = list(row[:len(row) - 1])
        list_json.append(row)
        # Tạo lable_unique là list chứa tất cả các là duy nhất
        for value in row_content:
            if value not in lable_unique:
                lable_unique.append(value)
        if len(row_content) > 1:
            # Tạo tất cả các cạnh trên một dòng
            edge_row = list(combinations(row_content, 2))
            edge_data.append(edge_row)

    # Tính số lần xuất hiện của một label trong các dòng và trong all ảnh
    label_count = [0] * len(lable_unique)
    image_count = [0] * len(lable_unique)
    for i in range(len(lable_unique)):
        for row in list_json:
            if lable_unique[i] in row:
                label_count[i] = label_count[i] + 1
                image_count[i] = image_count[i] + (int)(row[len(row) - 1])
    # Tạo ma trận thể hiện quan hệ của
    size = len(lable_unique)
    arr_relationship = np.zeros((size, size), dtype=np.int)
    for row in edge_data:
        for edge in row:
            x = lable_unique.index(edge[0])
            y = lable_unique.index(edge[1])
            arr_relationship[x][y] += 1
            arr_relationship[y][x] += 1

    list_result = []
    for i in range(size):
        list_result.append([0] * 12)
        list_result[i][0] = str(lable_unique[i])
        list_result[i][1] = label_count[i]
        list_result[i][6] = image_count[i]
    # print (list_result[i])
    return (list_result, arr_relationship, lable_unique, number)


def caculater_percent(list_result):
    size = len(list_result)
    list_percent_label = sorted(list_result, key=lambda list_result:list_result[2], reverse=True)
    if len(list_percent_label) > 0:
        for i in range(1, size - 1):
            list_percent_label[i][3] = list_percent_label[i - 1][2]
            list_percent_label[i][4] = list_percent_label[i + 1][2]
            # list_percent_label[i][5] = list_percent_label[i][2] + list_percent_label[i][3]

            list_percent_label[i][8] = list_percent_label[i - 1][7]
            list_percent_label[i][9] = list_percent_label[i + 1][7]
            # list_percent_label[i][10] = list_percent_label[i][7] + list_percent_label[i][8]

        # Set các trường hợp Null
        list_percent_label[0][3] = 'null'
        list_percent_label[size - 1][3] = list_percent_label[size - 2][2]

        list_percent_label[0][4] = list_percent_label[1][2]
        list_percent_label[size - 1][4] = 'null'

        list_percent_label[0][8] = 'null'
        list_percent_label[size - 1][8] = list_percent_label[size - 2][7]

        list_percent_label[0][9] = list_percent_label[1][7]
        list_percent_label[size - 1][9] = 'null'

        list_percent_label[0][5] = list_percent_label[0][2]
        list_percent_label[0][10] = list_percent_label[0][7]
        for i in range(1, size):
            list_percent_label[i][5] = list_percent_label[i - 1][5] + list_percent_label[i][2]
            list_percent_label[i][10] = list_percent_label[i - 1][10] + list_percent_label[i][7]
    return list_percent_label


#[0 'label',1 'label_count',2 'percent_label',3 'previous_p_label',4 'previous_next_label',5'sum_p_label',
#6 'image_count',7 'percent_image',8 'previous_p_image',9 'previous_next_image',10 'sum_p_label',11 'number_edge']
def caculator_percent_label(list_result, arr_relationship):
    size = len(list_result)
    list_percent_label = list(list_result)
    total_label = sum(row[1] for row in list_result)
    total_image = sum(row[6] for row in list_result)
    friend_edge = (arr_relationship != 0).sum(0)
    for i in range(size):
        list_result[i][2] = round((list_result[i][1] / total_label) * 100, 15)
        list_result[i][7] = round((list_result[i][6] / total_image)* 100, 15)
        list_result[i][11] = friend_edge[i]

    list_percent_label = caculater_percent(list_result)
    return list_percent_label

def percent_product(path_file_content):
    if os.path.exists(path_file_content):
        list_result, arr_relationship, lable_unique, number = getData(path_file_content)
        list_percent_label = caculator_percent_label(list_result, arr_relationship)
        # df = create_dataframe(list_percent_label)
        return (list_percent_label, number, arr_relationship)


def create_excel_all_product(path_in_json, path_out):
    list_folder = next(os.walk(path_in_json))[1]
    list_data_frame = []
    for folder in list_folder:
        file_name_json = str(folder) + '.json'
        path_json = os.path.join(path_in_json, folder)
        file_json_content = os.path.join(path_json, file_name_json)
        product_id = folder
        print (file_json_content)
        if os.path.exists(file_json_content):
            list_percent_label, number, arr_relationship = percent_product(file_json_content)
            #================ Write file percent ==================
            file_percent = os.path.join(path_out, product_id + '/' + product_id + '_image.csv')
            with open(file_percent, 'w', newline="") as f:
                wr = csv.writer(f, quoting=csv.QUOTE_ALL)
                wr.writerows(list_percent_label)
            #===========================================================

            #================ Write file relationship ==================
            file_name = os.path.join(path_out, product_id + '/' + product_id + '_image_relationship.csv')
            with open(file_name, 'w', newline="") as f:
                wr = csv.writer(f, quoting=csv.QUOTE_ALL)
                wr.writerows(arr_relationship)
            #===========================================================

path_file_content = '/u01/oracle/oradata/APEX/MARKETING_TOOL_03/Json_data_crawler'
# path_file_content = 'C:/Users/CPU10145-local/Desktop/Server/MARKETING_TOOL_03/Json_data_crawler'

create_excel_all_product(path_file_content, path_file_content)
