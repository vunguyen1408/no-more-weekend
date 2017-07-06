import os, os.path
#from os.path import splitext, basename, join
import io
import json
import time
import csv
import re

def tranform_content_to_Spark(file_in, file_out):
    list_json = []
    with open (file_in, 'r') as csvfile:
        #reader=json.load(file_json)
        #reader=csv.reader(csvfile , delimiter=',', quoting=csv.QUOTE_NONE)
        reader=csv.reader(csvfile , delimiter=',',quotechar='"')

        for row in reader:
            # Deleted the first two charater and the end charater of the first emlement
            if row[0] != '[]':
                row[0] = re.sub('[^a-zA-Z0-9 \']', '', row[0])
                # Deleted the end two charater emlement in index len(row) - 2
                #print (row[len(row) - 2])
                row[len(row) - 2] = re.sub('[^a-zA-Z0-9 \']', '', row[len(row) - 2])
                #print (row[len(row) - 2])
                list_json.append(row)




    list_json = list_json[1:]
    # for row in list_json:
    #     print (row)

    with open(file_out, 'w', newline="") as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerows(list_json)

# Run test
# file_in='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_2017-06-23 04-32-17 PM.csv'
# file_out ='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_2017-06-23 04_32_17 PM out.csv'
#
# tranform_content_to_Spark(file_in, file_out)
