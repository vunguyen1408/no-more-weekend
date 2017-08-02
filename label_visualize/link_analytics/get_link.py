import os, os.path
import io
import json
import pandas as pd


def check_exists(url1, url2):
	flag = False
	try:
		from urllib.parse import urlparse  # Python 3
	except ImportError:
		from urlparse import urlparse  # Python 2

	disassembled1 = urlparse(url1)	
	disassembled2 = urlparse(url2)
	if disassembled1.netloc == disassembled2.netloc:
		flag = True		
	return flag



def get_json_from_folder(path_folder, product):	
	try:
		from urllib.parse import urlparse  # Python 3
	except ImportError:
		from urlparse import urlparse	
	#===============Get link ==========
	url_google = 'http://play.google.com'
	url_apple = 'http://itunes.apple.com'
	
	list_json = []
	list_url_unique = []
	list_fre = []
	list_short_link = []
	#list_image_each_link = [] ##
	for root, dirs, files in os.walk(path_folder):					
		for file in files:			
			if (file.find("ads_creatives_audit_content_") == 0):
				# Mở file từng file đọc data
				path_dir = os.path.join(path_folder, root)
				path_file = os.path.join(path_dir, file)
				#print(path_file)
				with open(path_file, 'r') as f:
					data = json.load(f)

				# ===================================
				for value in data['my_json']:					
					if product in value['list_product']:
						list_link = value['audit_content']['links']							
						for link in list_link:
							list_image = []	
							#==============Is google or apple===========																							
							if (check_exists(link['link'], url_google) == True) or (check_exists(link['link'], url_apple) == True):
								if link['link'] not in list_url_unique:
									list_url_unique.append(link['link'])			
									list_fre.append(1)
									
								else:
									index = list_url_unique.index(link['link'])
									list_fre[index] += 1

							#==============Not google and apple===========
							elif (check_exists(link['link'], url_google) == False) and (check_exists(link['link'], url_apple) == False):
								url_netloc = urlparse(link['link']).netloc

								if(url_netloc != 'goo.gl') and (url_netloc not in list_url_unique):
									list_url_unique.append(url_netloc)
									list_fre.append(1)									
								elif (url_netloc != 'goo.gl') and (url_netloc in list_url_unique):
									index = list_url_unique.index(url_netloc)
									list_fre[index] += 1

								#===========Check short link============
								if (url_netloc == 'goo.gl') and (link['link'] not in list_url_unique):									
										#list_short_link.append(link['link'])
										list_url_unique.append(link['link'])
										list_fre.append(1)
								elif (url_netloc == 'goo.gl') and (link['link'] in list_url_unique):
							 		index = list_url_unique.index(link['link'])
							 		list_fre[index] += 1

							
	

	for i in range(len(list_fre)):	
		json_ = {	
			'link': list_url_unique[i],
			'frequence': list_fre[i]
		}
		list_json.append(json_)		
	return list_json






def correct_link_for_each_product(path_data_in, path_out_file_json, path_out_file_excel):
	my_json = {'json': [
				{'name': 'http___kiemvu.360game.vn', 'product_id': '206'},
				{'name': 'http___ntgh.360game.vn', 'product_id': '257'},
				{'name': 'https___tvc.360game.vn', 'product_id': '208'},
				]}
	list_df = []
	for value in my_json['json']:
		list_link = []
		list_json = get_json_from_folder(path_data_in, value['product_id'])

		#Check list json
		# Nếu substring:
		i = 0
		j = 0
		while i < (len(list_json)):
			while j < (len(list_json)):				
				if (i != j) and (list_json[i]['link'] in list_json[j]['link']):					
					list_json[i]['frequence'] += list_json[j]['frequence']
					list_json.remove(list_json[j])
					j = j - 1				
				j += 1
			i+=1


		# Add link from list_json
		for json_ in list_json:
			list_link.append(json_)
		
		correct_link_each_product = {
			'product': value['product_id'],
			'list_correct_link': list_link
		}

		# Xuất correct link cho mỗi sp ra file json
		path_result = os.path.join(path_out_file_json, value['product_id'])
		if not os.path.exists(path_result):
			os.makedirs(path_result)   
		
		file_name = 'correct_link_for_' + str(value['product_id']) + '.json'
		
		path_file_result_link = os.path.join(path_result, file_name)
		with open (path_file_result_link,'w') as f:
			json.dump(correct_link_each_product, f)

		# Xuất excel cho mỗi sản phẩm
		result_product = []
		for i in range (len(list_json)):
			result_product.append([list_json[i]['link'], list_json[i]['frequence']])
		df = pd.DataFrame({
				'Link': [i[0] for i in result_product],
				'Frequence': [i[1] for i in result_product]
			})
		df = df[['Link', 'Frequence']]
		list_df.append([value['product_id'], df])
		
	print ("=============================================")
	writer = pd.ExcelWriter(path_out_file_excel, engine='xlsxwriter')	
	for i in list_df:
		i[1].to_excel(writer, sheet_name=str(i[0]), index=False)
	writer.save()

	print('Save data success!...')


path_data_in = "D:/WorkSpace/CODE/DATA/DATA/DWHVNG/APEX/MARKETING_TOOL_02_JSON"
path_out_file_json = 'D:/WorkSpace/GITHUB/New_result/Link'
path_out_file_excel = 'D:/WorkSpace/GITHUB/New_result/link_for_each_product.xlsx'

correct_link_for_each_product(path_data_in, path_out_file_json, path_out_file_excel)


