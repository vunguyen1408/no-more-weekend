import os, os.path
#from os.path import splitext, basename, join
import io
import json
import time
import csv

print("a")

file_='/home/leth/Downloads/data_2017-06-23 04_32_17 PM.csv'
print(file_)
with open (file_,'r') as csvfile:
    #reader=json.load(file_json)
    #reader=csv.reader(csvfile , delimiter=',', quoting=csv.QUOTE_NONE)
    reader=csv.reader(csvfile , delimiter=',',quotechar='"')
    for row in reader:
        #list_json.append(row)
        print(row[0])
