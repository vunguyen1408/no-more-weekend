from googleads import adwords


adwords_client = adwords.AdWordsClient.LoadFromStorage('/home/marketingtool/Workspace/Python/no-more-weekend/adwords_python3/online_marketing/googleads.yaml')

acccount_id = '6493618146'
adwords_client.SetClientCustomerId(str(acccount_id))
campaign_service = adwords_client.GetService('CampaignService', version='v201708')
