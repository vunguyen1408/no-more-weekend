import json
import logging
import sys
from googleads import adwords
from datetime import datetime


def TSVtoJson(report_string, date):
	from collections import defaultdict
	#========= Get key for json ================
	list_pre = ['enabled', 'paused', 'removed']
	list_key = []
	fi = report_string.split('\n')

	line = fi[0]    
	ele = line.split('\t')
	
	for i in range(len(ele)):
		list_key.append(ele[i])


	#======== Convert a line to dictionary
	list_json = []

	for line in fi:
		ele = line.split('\t')

		dict_campaign = {}
		if (ele[0] not in list_key) and (len(ele) > 1 ) and (ele[0] != 'Total'):
			for i in range(len(list_key)):          
				if (list_key[i] == 'Cost') or ((list_key[i].find('Avg') >= 0) and (list_key[i] != 'Avg. position')):   # Cost                      
					ele[i] = float(float(ele[i]) / 1000000)
				elif (ele[i].isdigit()):     # Integer        
					ele[i] = int(ele[i])
				elif (ele[i].find('%') == len(ele[i]) - 1) and (ele[i].replace("%", "").replace(".", "").isdigit()):         # Percent  
					ele[i] = float(ele[i].replace("%", ""))
				elif (ele[i].replace(".", "").isdigit()):    # Float        
					ele[i] = float(ele[i])          
				elif (ele[i] == ' --'):              # Empty        
					ele[i] = ""         
				elif (ele[i].find('[') > 0 and ele[i].find(']') > 0):# and ele[i].find(',') > 0):                 
					list_ = ele[i].split(',')
					for u in range(len(list_)):             
						s = list_[u]
						for v in range(len(s)):                   
							if (s[v].isalpha() == False):
								list_[u] = s.replace(s[v], '')

					list_[0] = list_[0][1:len(list_[0])]
					list_[-1] = list_[-1][0:len(list_[0]) -1]   
					ele[i] = list_

				dict_campaign[list_key[i]] = ele[i]

			dict_campaign['Mapping'] = False
			dict_campaign['Date'] = date
			if ((dict_campaign['Cost'] > 0)):
				list_json.append(dict_campaign)
	return list_json 


def DownloadAdsOfAdGroup(adwords_client, customerId, startDate, endDate):

	adwords_client.SetClientCustomerId(customerId)
	print (customerId)
	report_downloader = adwords_client.GetReportDownloader(version='v201708')

	report = {
		'reportName': 'Custom date AD_PERFORMANCE_REPORT',
		'dateRangeType': 'CUSTOM_DATE',
		'reportType': 'AD_PERFORMANCE_REPORT',
		'downloadFormat': 'TSV',
		'selector': {
				'dateRange':{'min':startDate,'max':endDate},
				'fields': [
						# Attribute
						'AdGroupId',
						'AdGroupName',
						'AdGroupStatus',
						'AdType',

						'CampaignId',
						'CampaignName',
						'CampaignStatus',						

						# 'CreativeFinalUrls',
						# 'CreativeDestinationUrl',

						'Id',
						'ImageAdUrl',
						'DisplayUrl',
						'ImageCreativeName',
						'Description',
						# 'DevicePreference',                    
						# 'ExternalCustomerId',          
						# 'Headline',

						# Metric
						'Clicks',
						# 'InvalidClicks',
						'Conversions',
						'Engagements',
						'Impressions',
						
						# 'Interactions',
						# 'InteractionTypes',						
						# 'InteractionRate',

						'VideoViews',
						# 'VideoViewRate',
						# 'VideoQuartile25Rate',
						# 'VideoQuartile50Rate',
						# 'VideoQuartile75Rate',
						# 'VideoQuartile100Rate',

						'Cost',
						# 'Ctr',
						# 'AverageCpc',
						# 'AverageCpm',
						# 'AverageCpe',
						# 'AverageCpv',
						# 'AverageCost',					
						# 'AveragePosition',

				]
			}
		}

	result = report_downloader.DownloadReportAsString(
		report, skip_report_header=True, skip_column_header=False,
		skip_report_summary=False, include_zero_impressions=True)
	# print(result)
	return result



def DownloadVideo(adwords_client, customerId, startDate, endDate):

	adwords_client.SetClientCustomerId(customerId)
	print (customerId)
	report_downloader = adwords_client.GetReportDownloader(version='v201708')

	report = {
		'reportName': 'Custom date VIDEO_PERFORMANCE_REPORT',
		'dateRangeType': 'CUSTOM_DATE',
		'reportType': 'VIDEO_PERFORMANCE_REPORT',
		'downloadFormat': 'TSV',

		'selector': {
			'dateRange':{'min':startDate,'max':endDate},
			'fields': [

			    # Attribute
				'AdGroupId',
				'AdGroupName',   
				'AdGroupStatus',     

				'CampaignId',
				'CampaignName',
				'CampaignStatus',
				'CreativeId',
				'CreativeStatus',

				'VideoChannelId',
				'VideoDuration',
				'VideoId',
				'VideoTitle',

	          	# Metric
				'Clicks',				
				'Conversions',
				'Engagements',
				'Impressions',
				
				'VideoViews',
				'VideoViewRate',
				'VideoQuartile25Rate',
				'VideoQuartile50Rate',
				'VideoQuartile75Rate',
				'VideoQuartile100Rate',

				'Cost',
				'Ctr',				
				'AverageCpm',				
				'AverageCpv',			
          
          ]
      }
  }

	result = report_downloader.DownloadReportAsString(
		report, skip_report_header=True, skip_column_header=False,
		skip_report_summary=False, include_zero_impressions=True)
	# print(result)
	return result



def DownloadKeyword(adwords_client, customerId, startDate, endDate):

	adwords_client.SetClientCustomerId(customerId)
	print (customerId)
	report_downloader = adwords_client.GetReportDownloader(version='v201708')

	report = {
		'reportName': 'Custom date KEYWORDS_PERFORMANCE_REPORT',
		'dateRangeType': 'CUSTOM_DATE',
		'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
		'downloadFormat': 'TSV',

		'selector': {
			'dateRange':{'min':startDate,'max':endDate},
			'fields': [
			    
				'AdGroupId',
				'AdGroupName',   
				'AdGroupStatus',     

				'CampaignId',
				'CampaignName',
				'CampaignStatus',

				'FinalUrls',
				'Criteria',
				'Id',
				'IsNegative',
				'KeywordMatchType',
				'LabelIds',
				'Labels',
				'UrlCustomParameters',

				# Metric
				'Clicks',				
				'Conversions',
				'Engagements',
				'Impressions',
				
				'VideoViews',
				'VideoViewRate',
				'VideoQuartile25Rate',
				'VideoQuartile50Rate',
				'VideoQuartile75Rate',
				'VideoQuartile100Rate',

				'Cost',
				'Ctr',				
				'AverageCost',
				'AverageCpc',
				'AverageCpe',
				'AverageCpm',
				'AverageCpv',
				'AveragePageviews',
				'AveragePosition',
				'AverageTimeOnSite'	          
				]
			}
		}

	result = report_downloader.DownloadReportAsString(
		report, skip_report_header=True, skip_column_header=False,
		skip_report_summary=False, include_zero_impressions=True)
	print(result)
	return result	



adwords_client = adwords.AdWordsClient.LoadFromStorage('D:/WorkSpace/Adwords/Finanlly/AdWords/adwords_python3/googleads.yaml')
customerId = '5008396449'
startDate = '2017-10-02' 
endDate = '2017-10-02' 

#===================== Downloads Ads ======================
# path_file_ads = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/ads.json'

# result_ads = DownloadAdsOfAdGroup(adwords_client, customerId, startDate, endDate)
# result_json = TSVtoJson(result_ads, startDate)
# with open (path_file_ads, 'w') as f:
# 	json.dump(result_json, f)

#===================== Downloads Video ======================
path_file_video = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/video.json'

result_ads = DownloadVideo(adwords_client, customerId, startDate, endDate)
result_json = TSVtoJson(result_ads, startDate)
with open (path_file_video, 'w') as f:
	json.dump(result_json, f)
print("Save videos into file ............")

#===================== Downloads Keywords ======================
path_file_keyword = 'C:/Users/CPU10912-local/Desktop/Adword/DATA/ACCOUNT_ID/keyword.json'

result_ads = DownloadKeyword(adwords_client, customerId, startDate, endDate)
result_json = TSVtoJson(result_ads, startDate)
with open (path_file_keyword, 'w') as f:
	json.dump(result_json, f)

print("Save keywords into file ............")






