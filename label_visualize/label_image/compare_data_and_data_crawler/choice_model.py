import compare_label as cp
import os, os.path
import pandas as pd
import json




my_json = {'json': [
			{'name': 'http___kiemvu.360game.vn', 'product_id': '206'},
			{'name': 'http___ntgh.360game.vn', 'product_id': '257'},
			{'name': 'https___tvc.360game.vn', 'product_id': '208'},
			]}
# print (number_image_json)
# print (number_image_predict)
# print (number_image_miss)
# Check xem mot anh cua mot json co ton tai trong mot list_json hay khong
def check_exists(json_, list_json):
	flag = False
	try:
		from urllib.parse import urlparse  # Python 3
	except ImportError:
		from urlparse import urlparse  # Python 2

	disassembled1 = urlparse(json_["image_url"])
	for item in list_json:
		disassembled2 = urlparse(item["image_url"])
		if disassembled1.netloc == disassembled2.netloc and disassembled1.path == disassembled2.path  and disassembled1.params == disassembled2.params :
			flag = True
			break
	return flag


def parse_result(list_image_predict):
	list_in = []
	list_out = []
	number_image_predict = 0
	number_image_miss = 0
	for i, item in enumerate(list_image_predict):
		if item['predict'] == True:
			number_image_predict += 1
			if not (check_exists(item, list_in)):
				list_in.append(item)
		else:
			number_image_miss += 1
			if not (check_exists(item, list_out)):
				list_out.append(item)
	return (list_in, list_out, number_image_predict, number_image_miss)


def create_dataframe(result_product):
    df = pd.DataFrame({
        'Web_percent': [i[0] for i in result_product], 
        'Face_percent': [i[1] for i in result_product],
        'Total': [i[2] for i in result_product],
        'Image_in': [i[3] for i in result_product],
        'Image_out': [i[4] for i in result_product],
        'Total_unique': [i[5] for i in result_product],
        'Image_unique_in': [i[6] for i in result_product],
        'Image_unique_out': [i[7] for i in result_product],
        'Image_unique_in/Total unique': [i[8] for i in result_product],
        })
    df = df[['Web_percent', 'Face_percent', 'Total', 'Image_in', 'Image_out','Total_unique', 'Image_unique_in', 'Image_unique_out', 'Image_unique_in/Total unique']]
    return df

def compare_all_product(path_full_data, path_content_crawler, path_result, file_excel):
	# Create list percent
	list_percent = []
	for i in range(100, 30, -5):
		list_percent.append(i)

	list_folder = next(os.walk(path_content_crawler))[1]
	list_df = []
	for folder in list_folder:
		print (folder)
		file_name_json = str(folder) + '.json'
		path_json = os.path.join(path_content_crawler, folder)
		file_json_content = os.path.join(path_json, file_name_json)
		
		if os.path.exists(file_json_content):
			product_id = ''
			for value in my_json['json']:
				if value['name'] == folder:		
					product_id = value['product_id']
			print (product_id)
			result_product = []
			path_folder_result = os.path.join(path_result, product_id)
			for percent_web in list_percent:
				#================================== Make folder result
				path_folder_result_web = os.path.join(path_folder_result, (str(percent_web) + '_web'))
				if not os.path.exists(path_folder_result_web):
					os.makedirs(path_folder_result_web)
            	#==================================
				for percent_face in list_percent:
					list_image_predict = cp.predict_for_all_data(path_full_data, file_json_content, product_id, percent_face, percent_web)
					list_in, list_out, number_image_predict, number_image_miss = parse_result(list_image_predict)

					#============================================= make file 
					path_result_in = os.path.join(path_folder_result_web, (str(percent_face) + '_percent_face_list_in.json'))
					path_result_out = os.path.join(path_folder_result_web, (str(percent_face) + '_percent_face_list_out.json'))

					list_in_json = {}
					list_in_json['my_json'] = list_in
					list_out_json = {}
					list_out_json['my_json'] = list_out
					with open (path_result_in,'w') as f:
						json.dump(list_in_json, f)
					with open (path_result_out,'w') as f:
						json.dump(list_out_json, f)
					#===============================================
					total = len(list_image_predict)
					total_unique = len(list_in) + len(list_out)
					percent = float((len(list_in)  * 100.0) / total_unique)
					result_product.append([percent_web, percent_face, total, number_image_predict, number_image_miss, total_unique, len(list_in), len(list_out), percent])
			df = create_dataframe(result_product)
			list_df.append([product_id, df])
			print ("=============================================")
	writer = pd.ExcelWriter(file_excel, engine='xlsxwriter')
	for i in list_df:
		i[1].to_excel(writer, sheet_name=str(i[0]), index=False)
	writer.save()



path_full_data = 'C:/Users/CPU10145-local/Desktop/Python Envirement/DATA NEW/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON'
path_content_crawler = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data_product/Json_and_report'
path_result = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data_product/Result'
file_excel = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data_product/final_choice_model.xlsx'

compare_all_product(path_full_data, path_content_crawler, path_result, file_excel)
