# package
import get_content_product as content_product

import os, os.path
#from os.path import splitext, basename, join
import csv
import numpy as np
import json
from itertools import combinations
import pandas as pd


# Lấy list label chứa (label, label_count, image_count) và (array)
def getData(path_file_content, product, date_, to_date_):
    edge_data = []
    lable_unique = []
    list_json = []
    list_result, label_image = content_product.get_content_label_date(path_file_content, product, date_, to_date_)

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
    arr_relationship = np.zeros((size, size))
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
    return (list_result, arr_relationship, lable_unique)


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

def create_dataframe(list_percent_label):
    df = pd.DataFrame({
        'label': [i[0] for i in list_percent_label], 
        'label_count': [i[1] for i in list_percent_label],
        'percent_label': [i[2] for i in list_percent_label],
        'previous_p_label': [i[3] for i in list_percent_label],
        'previous_next_label': [i[4] for i in list_percent_label],
        'sum_p_label': [i[5] for i in list_percent_label],
        'image_count': [i[6] for i in list_percent_label],
        'percent_image': [i[7] for i in list_percent_label],
        'previous_p_image': [i[8] for i in list_percent_label],
        'previous_next_image': [i[9] for i in list_percent_label],
        'sum_p_label': [i[10] for i in list_percent_label],
        'number_edge': [i[11] for i in list_percent_label],
        })
    df = df[['label', 'label_count', 'percent_label', 'previous_p_label', 'previous_next_label','sum_p_label',
        'image_count', 'percent_image', 'previous_p_image', 'previous_next_image', 'sum_p_label', 'number_edge']]
    return df

def percent_product(path_file_content, path_out, product, date_, to_date_):
    if os.path.exists(path_file_content):
        list_result, arr_relationship, lable_unique = getData(path_file_content, product, date_, to_date_)
        list_percent_label = caculator_percent_label(list_result, arr_relationship)
        df = create_dataframe(list_percent_label)
        return (df, len(lable_unique))

def group_by_product(path_audit_content):
    list_folder = next(os.walk(path_audit_content))[1]
    list_unique_product = []
    for folder in list_folder:
        folder_audit = os.path.join(path_audit_content, folder)
        audit_content = "ads_creatives_audit_content_"+ folder +".json"
        path_file_audit_content = os.path.join(folder_audit, audit_content)
        if os.path.exists(path_file_audit_content):
            with open(path_file_audit_content, 'r') as f_json:
                data_json = json.load(f_json)
            for j in data_json['my_json']:
                list_product = j['list_product']
                for p in list_product:
                    if p not in list_unique_product:
                        list_unique_product.append(p)
    return list_unique_product

def percent_each_product(path_file_content, path_out):
    date_ = '2017-10-01'
    to_date_ = '0001-01-01'
    list_unique_product = group_by_product(path_file_content)

    file_name_out = 'caculator_percent.xlsx'
    file_out = os.path.join(path_out, file_name_out)
    writer = pd.ExcelWriter(file_out, engine='xlsxwriter')
    list_data_frame = []
    list_number_label = []
    for p in list_unique_product:
        df, size = percent_product(path_file_content, path_out, p, date_, to_date_)
        list_data_frame.append(df)
        list_number_label.append(size)

    df0 = pd.DataFrame({
            'product_id': list_unique_product,
            'number_label': list_number_label
        })
    df0 = df0[['product_id', 'number_label']]
    df0.to_excel(writer, sheet_name='Summary', index=False)

    for i, df in enumerate(list_data_frame):
        df.to_excel(writer, sheet_name=str(list_unique_product[i]), index=False)
    writer.save()

path_file_content = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path_out = 'C:/Users/CPU10145-local/Desktop/report'


# path_file_content = 'E:/vng/data/data/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# path_out = 'C:/Users/ltduo/Desktop'

percent_each_product(path_file_content, path_out)