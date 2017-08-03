import os, os.path
import io
import json


def compare_list_label(list_label_image, list_label_product):
	result_compare = True
	for lst_image in list_label_product:
		if (set(lst_image) == set(list_label_image)):
			return True		
	return False

def get_json_from_folder(path_json, path_product, product):	

	list_label_product, list_unique_label_product = get_list_label_each_product(path_file)

	number_image_json = 0
	number_image_predict = 0
	number_image_miss = 0
	list_image_predict = []
	for root, dirs, files in os.walk(path_json):					
		for file in files:			
			if (file.find("ads_creatives_audit_content_") == 0):
				# Mở file từng file đọc data
				path_dir = os.path.join(path_json, root)
				path_file = os.path.join(path_dir, file)
				#print(path_file)
				with open(path_file, 'r') as f:
					data = json.load(f)

				# Check xem ảnh còn tồn tại hay không?
				for i, value in enumerate(data['my_json']):
					if product in value['list_product']:
						list_image_urls = value['audit_content']['image_urls']
						for j, image in enumerate(list_image_urls):
							if 'image_label' in image:
								json_ = {}
								label_image = image['image_label']
								if len(label_image) > 0:
									number_image_json += 1
									flag = compare_list_label(label_image, list_unique_label_product)
									json_ = {
										'image_url': image['image_url'],
										'image_label': image['image_label'],
										'index_in_json': i,
										'index_in_image_label': j,
										'predict': flag
									}
									list_image_predict.append(json_)

									if flag == True:
										number_image_predict += 1
									else: 
										number_image_miss += 1

	# print('Tong image trong json: ', number_image_json)
	# print('Tong image du doan dung trong json: ', number_image_predict)
	# print('Tong image bi miss trong json: ', number_image_miss)
	# print(list_image_predict[0])
	return (list_image_predict, number_image_json, number_image_predict, number_image_miss)


# list_product_name = ['http___kiemvu.360game.vn', 'http___ntgh.360game.vn', 'https___tvc.360game.vn']
# list_product_code = ['206', '257', '208']
# for i in range(len(list_product_name)):
# 	path_folder = 'D:/WorkSpace/CODE/Json_and_excel/Json_and_report'
# 	path_product = os.path.join(path_folder, list_product_name[i])

# 	path_json = 'D:/WorkSpace/CODE/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
# 	get_json_from_folder(path_folder, list_total_labels, list_product_code[i])
# 	print('=======================================================')


