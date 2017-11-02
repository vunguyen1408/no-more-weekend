import json
import os 

def RecomputeTotalPlan(plan, list_campaign):

	"""
		Hàm tính lại total cho một plan (trừ đi các campaign được nhả)
	"""
	sum_plan = plan['TOTAL_CAMPAIGN'].copy()
	for campaign_in_plan in plan['CAMPAIGN']:
		for campaign in list_campaign:
			
			if (str(campaign_in_plan['CAMPAIGN_ID']) == str(campaign['Campaign ID'])) \
			and (campaign_in_plan['Date'] == campaign['Date']):
				# --------------- Tính total ------------------
				sum_plan['CLICKS'] -= float(campaign['Clicks'])
				sum_plan['IMPRESSIONS'] -= float(campaign['Impressions'])
				sum_plan['CTR'] -= float(campaign['CTR'])
				sum_plan['AVG_CPC'] -= float(campaign['Avg. CPC'])
				sum_plan['AVG_CPM'] -= float(campaign['Avg. CPM'])
				sum_plan['COST'] -= float(campaign['Cost'])
				sum_plan['CONVERSIONS'] -= float(campaign['Conversions'])
				sum_plan['INVALID_CLICKS'] -= float(campaign['Invalid clicks'])
				sum_plan['AVG_POSITION'] -= float(campaign['Avg. position'])
				sum_plan['ENGAGEMENTS'] -= float(campaign['Engagements'])
				sum_plan['AVG_CPE'] -= float(campaign['Avg. CPE'])
				sum_plan['AVG_CPV'] -= float(campaign['Avg. CPV'])
				sum_plan['INTERACTIONS'] -= float(campaign['Interactions'])
				sum_plan['VIEWS'] -= float(campaign['Views'])
				# if 'INSTALL_CAMP' not in campaign:
				# 	campaign['INSTALL_CAMP'] = 0
				# 	sum_plan['INSTALL_CAMP'] -= float(campaign['INSTALL_CAMP'])
	print (plan['TOTAL_CAMPAIGN'])
	print()		
	plan['TOTAL_CAMPAIGN'] = sum_plan.copy()
	print (plan['TOTAL_CAMPAIGN'])
	return plan



list_campaign = [
{
      "Campaign state": "enabled",
      "Campaign": "JXM | 1610105 1611013 1611026 1611062 1703048 1704016 1706008 1707026 1709039 | Search Google Play Store",
      "Advertising Channel": "SEARCH",
      "Advertising Sub Channel": "Search Mobile App",
      "Campaign ID": 682545537,
      "Campaign serving status": "eligible",
      "Clicks": 2990,
      "Impressions": 16945,
      "Unique cookies": "",
      "CTR": 17.65,
      "Avg. CPC": 128268,
      "Avg. CPM": 22633225,
      "Cost": 383.52,
      "Conversions": 1113,
      "Bid Strategy Type": "Target CPA",
      "Invalid clicks": 454,
      "Avg. position": 1.4,
      "Engagements": 0,
      "Avg. CPE": 0,
      "View rate": 0,
      "Views": 0,
      "Avg. CPV": 0,
      "Avg. Cost": 128268,
      "Interaction Types": [
        "Clicks"
      ],
      "Interactions": 2990,
      "Interaction Rate": 17.65,
      "Video played to 25%": 0,
      "Video played to 50%": 0,
      "Video played to 75%": 0,
      "Video played to 100%": 0,
      "Start date": "2016-10-16",
      "End date": "",
      "Mapping": False,
      "Date": "2017-06-29",
      "INSTALL": 30000,
      "Plan": None

}, 
{
      "Campaign state": "enabled",
      "Campaign": "JXM | 1610105 1611013 1611026 1611062 1703048 1704016 1706008 1707026 1709039 | Search Google Play Store",
      "Advertising Channel": "SEARCH",
      "Advertising Sub Channel": "Search Mobile App",
      "Campaign ID": 682545537,
      "Campaign serving status": "eligible",
      "Clicks": 2990,
      "Impressions": 16945,
      "Unique cookies": "",
      "CTR": 17.65,
      "Avg. CPC": 128268,
      "Avg. CPM": 22633225,
      "Cost": 383.52,
      "Conversions": 1113,
      "Bid Strategy Type": "Target CPA",
      "Invalid clicks": 454,
      "Avg. position": 1.4,
      "Engagements": 0,
      "Avg. CPE": 0,
      "View rate": 0,
      "Views": 0,
      "Avg. CPV": 0,
      "Avg. Cost": 128268,
      "Interaction Types": [
        "Clicks"
      ],
      "Interactions": 2990,
      "Interaction Rate": 17.65,
      "Video played to 25%": 0,
      "Video played to 50%": 0,
      "Video played to 75%": 0,
      "Video played to 100%": 0,
      "Start date": "2016-10-16",
      "End date": "",
      "Mapping": False,
      "Date": "2017-06-30",
      "INSTALL": 30000,
      "Plan": None

}]


path = 'C:/Users/CPU10912-local/Desktop/total.json'

with open(path, 'r') as fi:
	data = json.load(fi)

RecomputeTotalPlan(data['TOTAL'][0], list_campaign)



















