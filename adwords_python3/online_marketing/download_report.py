#!/usr/bin/env python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This example downloads a criteria performance report.

To get report fields, run get_report_fields.py.

The LoadFromStorage method is pulling credentials and properties from a
"googleads.yaml" file. By default, it looks for this file in your home
directory. For more information, see the "Caching authentication information"
section of our README.

"""


import logging
import sys
from googleads import adwords

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


def main(client):
  report_downloader = client.GetReportDownloader(version='v201708')

  report = {
      'reportName': 'Custom date CAMPAIGN_PERFORMANCE_REPORT',
      'dateRangeType': 'CUSTOM_DATE',
      #'dateRange':{'min':20170601,'max':20170630},
      'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
      'downloadFormat': 'TSV',
      'selector': {
           'dateRange':{'min':'20170601','max':'20170630'},
           'fields': ['CampaignStatus'
           ,'CampaignName'
           #, #budget
           , 'ServingStatus'
           ,'Clicks'
           ,'Impressions'
           ,'ImpressionReach'
           ,'Ctr'
           ,'AverageCpc'
           ,'AverageCpm'
           ,'Cost'
           ,'InvalidClicks'
           ,'AveragePosition'
           ,'Engagements'
           ,'AverageCpe'
           ,'VideoViewRate'
           ,'VideoViews'
           ,'AverageCpv'
           ,'AverageCost'
           ,'InteractionTypes'
           ,'Interactions'
           ,'InteractionRate'
            ,'VideoQuartile25Rate'
            ,'VideoQuartile50Rate'
            ,'VideoQuartile75Rate'
            ,'VideoQuartile100Rate'
            ,'VideoViews'


           #TotalCost
           #'AccountCurrencyCode'
# ,'AccountDescriptiveName'
# ,'AccountTimeZone'
# ,'ActiveViewCpm'
# ,'ActiveViewCtr'
# ,'ActiveViewImpressions','Interactions'
# ,'ActiveViewMeasurability'
# ,'ActiveViewMeasurableCost'
# ,'ActiveViewMeasurableImpressions'
# ,'ActiveViewViewability'
# ,'AdNetworkType1'
# ,'AdNetworkType2'
# ,'AdvertisingChannelSubType'
# ,'AdvertisingChannelType'
# ,'AllConversionRate'
# ,'AllConversions'
# ,'AllConversionValue'
# ,'Amount'
#
#
#
#
#
# ,'AverageFrequency'
# ,'AveragePageviews'
#
# ,'AverageTimeOnSite'
# ,'BaseCampaignId'
# ,'BiddingStrategyId'
# ,'BiddingStrategyName'
# ,'BiddingStrategyType'
# ,'BidType'
#,'BounceRate'
# ,'BudgetId'
# #,'CampaignDesktopBidModifier'
# #,'CampaignGroupId'
# ,'CampaignId'
# #,'CampaignMobileBidModifier'
#
# ,'CampaignStatus'
#,'CampaignTabletBidModifier'
#,'CampaignTrialType'
# ,'ClickAssistedConversions'
# ,'ClickAssistedConversionsOverLastClickConversions'
# ,'ClickAssistedConversionValue'
#
#,'ClickType'
# ,'ContentBudgetLostImpressionShare'
# ,'ContentImpressionShare'
# ,'ContentRankLostImpressionShare'
# #,'ConversionCategoryName'
# ,'ConversionRate'
# ,'Conversions'
#,'ConversionTrackerId'
# #,'ConversionTypeName'
# ,'ConversionValue'
#
# ,'CostPerAllConversion'
# ,'CostPerConversion'
# ,'CostPerCurrentModelAttributedConversion'
# ,'CrossDeviceConversions'
#
# ,'CurrentModelAttributedConversions'
# ,'CurrentModelAttributedConversionValue'
# ,'CustomerDescriptiveName'
# #,'Date'
# #,'DayOfWeek'
# ,'Device'
# ,'EndDate'
# ,'EngagementRate'
#
# #,'EnhancedCpcEnabled'
# #,'EnhancedCpvEnabled'
# #,'ExternalConversionSource'
# #,'ExternalCustomerId'
# ,'GmailForwards'
# ,'GmailSaves'
# ,'GmailSecondaryClicks'
# #,'HourOfDay'
# # ,'ImpressionAssistedConversions'
# # ,'ImpressionAssistedConversionsOverLastClickConversions'
# ,'ImpressionAssistedConversionValue'
#
#
#
#
#
# ,'InvalidClickRate'
#
# ,'IsBudgetExplicitlyShared'
# ,'LabelIds'
# ,'Labels'
# ,'Month'
# ,'MonthOfYear'
# ,'NumOfflineImpressions'
# ,'NumOfflineInteractions'
# ,'OfflineInteractionRate'
# ,'PercentNewVisitors'
#,'Period'
# ,'Quarter'
# ,'RelativeCtr'
# ,'SearchBudgetLostImpressionShare'
# ,'SearchExactMatchImpressionShare'
# ,'SearchImpressionShare'
# ,'SearchRankLostImpressionShare'
# ,'ServingStatus'
# #,'Slot'
# ,'StartDate'
# ,'TrackingUrlTemplate'
# #,'UrlCustomParameters'
# ,'ValuePerAllConversion'
# ,'ValuePerConversion'
# ,'ValuePerCurrentModelAttributedConversion'

# ,'ViewThroughConversions'
# ,'Week'
# ,'Year'
]
      }
  }

  # You can provide a file object to write the output to. For this demonstration
  # we use sys.stdout to write the report to the screen.
  report_downloader.DownloadReport(
      report, sys.stdout, skip_report_header=False, skip_column_header=False,
      skip_report_summary=False, include_zero_impressions=True)


if __name__ == '__main__':
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  adwords_client.SetClientCustomerId('5008396449')
  main(adwords_client)
