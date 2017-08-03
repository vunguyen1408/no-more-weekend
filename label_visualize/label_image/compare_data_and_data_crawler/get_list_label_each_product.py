import os, os.path
import io
import json

def get_list_label_each_product(path_file):
	# Đưa vào đường dẫn của file product. 
	#Lấy ra list toàn bộ label và list unique label
	
	list_total_labels = []
	list_label_unique = []
	with open(path_file, 'r') as f:
        data = json.load(f)

	for value in data['sample_json']:
		list_label = value['image_label']
		if len(list_label) > 0:
			if list_label not in list_label_unique:
				list_label_unique.append(list(list_label))
			temp = list(list_label)
			temp.append(1)
			list_total_labels.append(temp)
            		
	
	print ("Get data completed...!")
	return (list_total_labels, list_label_unique)	