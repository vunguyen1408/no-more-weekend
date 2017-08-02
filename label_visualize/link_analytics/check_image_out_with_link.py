import os, os.path
import io
import json
import pandas as pd


def get_correct_link(path_json_link, product):
	path_folder = os.path.join(path_json_link, product)
	file_name = 'correct_link_for_' + product + '.json'
	path_file = os.path.join(path_folder, file_name)
	with open(path_file, 'r') as f:
		data = json.load(f)

	list_link = []
	for value in data['list_correct_link']:				
		list_link.append(value['link'])
	
	return list_link


def match_link_to_list_link(link, list_link):	
	for i in list_link:
		if (link.find(i) >= 0):
			return True
	return False	



def compare_link(path_in_folder_image, path_in_link, path_out_image):	

	for root, dirs, files in os.walk(path_in_folder_image):		
		for file in files:			
			if (file.find("percent_face_list_out.json") > 0):

				# ==========Open file to read link of image
				path_dir = os.path.join(path_in_folder_image, root)				
				path_file = os.path.join(path_dir, file)				
				#print(path_file)
				with open(path_file, 'r') as f:
					data = json.load(f)

				#======== Get product
				product = path_file[len(path_in_folder_image)+1 : len(path_in_folder_image)+4]
				list_image_in = []
				list_image_out = []
				# Compare with correct link of the same product
				list_link_product = get_correct_link(path_in_link, product)
				for value in data['my_json']:
					list_link_image = value['list_links']
					for link in list_link_image:						
						if (match_link_to_list_link(link['link'], list_link_product) == True):
							list_image_in.append(value)							
						else:
							list_image_out.append(value)							

				# Save with the same file				
				path_product = os.path.join(path_out_image, product)
				if not os.path.exists(path_product):
					os.makedirs(path_product)   
		
				index_1 = root.find(product)				
				percent_web = root[index_1 + 4:]				
				path_percent_web = os.path.join(path_product, percent_web)
				if not os.path.exists(path_percent_web):
					os.makedirs(path_percent_web)   

				# folder = path_file[index_1:index_2 - 1]
				# path_folder = os.path.join(path_out_image, folder)

				file_name_in = file[:-5] + '_after_check_link_in.json' 
				path_file_in = os.path.join(path_percent_web, file_name_in)	
				image_in = {'my_json': [list_image_in]}
				with open (path_file_in,'w') as f:
						json.dump(image_in, f)


				file_name_out = file[:-5] + '_after_check_link_out.json' 
				path_file_out = os.path.join(path_percent_web, file_name_out)
				image_out = {'my_json': [list_image_out]}
				with open (path_file_out,'w') as f:
						json.dump(image_out, f)

	print("Save data success!...")

path_in_folder_image = 'D:/WorkSpace/GITHUB/New_result/Result'
path_in_link = 'D:/WorkSpace/GITHUB/New_result/Link'
path_out_image = 'D:/WorkSpace/GITHUB/New_result/Result/check_link'


compare_link(path_in_folder_image, path_in_link, path_out_image)