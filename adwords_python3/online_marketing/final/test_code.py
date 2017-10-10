from googleads import adwords
adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')
campaign_service = adwords_client.GetService('CampaignService', version='v201708')
