# Tong ket danh sach cac label cua file Json
import os, os.path
import json
import csv
from datetime import datetime , timedelta, date


def get_content_label_list_file(list_path_file, product):
    """
    Lấy nội dung các list label của từng file trong list file
    Đầu vào:
        + list_path_file : list chứa đường dẫn các file
    Trả về:
        + list_result : list các row, mỗi row là một list các label, có thuộc tính tần suất
        + label_image : list các row, mỗi row là một list các label
    """
    list_result = []
    list_row_unique = []
    count = 0
    for file_ in list_path_file:
        with open(file_, 'r') as f:
            data = json.load(f)
        for value in data['my_json']:
            if product in value['list_product']:
                list_image_urls = value['audit_content']['image_urls']
                for i in range(len(list_image_urls)):
                    if 'image_label' in list_image_urls[i]:
                        label_image = list_image_urls[i]['image_label']
                        if len(label_image) > 0:
                            count = count + 1
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


#path = 'C:\\Users\\CPU10145-local\\Desktop\\Python Envirement\\Data\\Date'
# path = 'C:\\Users\\CPU10145-local\\Desktop\\Python Envirement\\DATA NEW\\DATA\\DWHVNG\\APEX\\MARKETING_TOOL_02_JSON'
# date_ = '2017-10-01'
# to_date_ = '0001-01-01'
# product = "242"

def get_content_label_date(path, product, date_ = None, to_date_ = '0001-01-01'):
    """
    Lấy nội dung các list label của từng ngày được chọn.
    Đầu vào:
        + path : Đường dẫn đến thư mục MARKETING_TOOL_02_JSON
        + date_ : lấy đến ngày date_, mặc định là ngày hiện tại
        + date_ : lấy kể từ ngày to_date_, mặc định là '0001-01-01'
    Trả về:
        + list_result : list các row, mỗi row là một list các label, có thuộc tính tần suất
        + label_image : list các row, mỗi row là một list các label
    """
    # Lấy danh sách path của các file json cần tổng hợp data
    list_file = []
    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    list_folder = next(os.walk(path))[1]
    for folder in list_folder:
        f_date = datetime.strptime(folder, '%Y-%m-%d').date()
        if f_date <= date and f_date >= to_date:
            # print (folder)
            path_folder = os.path.join(path, folder)
            file_name = "ads_creatives_audit_content_"+ folder +".json"
            path_file = os.path.join(path_folder, file_name)
            if os.path.exists(path_file):
                list_file.append(path_file)

    # Tổng hợp data của các file json có trong list_file
    list_result, label_image = get_content_label_list_file(list_file, product)
    return (list_result, label_image)

# # Print file
# list_result, label_image = get_content_label_date(path, date_, to_date_)


# file_out ='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data new out.csv'
# with open(file_out, 'w', newline="") as f:
#     wr = csv.writer(f, quoting=csv.QUOTE_ALL)
#     wr.writerows(list_result)






























