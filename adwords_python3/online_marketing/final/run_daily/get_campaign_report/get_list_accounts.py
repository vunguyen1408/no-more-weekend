"""
      Get all accounts using API GG Adwords
"""
from googleads import adwords
import json
import os
import pandas as pd

PAGE_SIZE = 500

def SaveAccountTree(account, accounts, links, level, list_acc, list_mcc, list_mcc_id, list_dept, list_entity, dept = None):
  """Save an account tree.

  Args:
    account: dict The account to display.
    accounts: dict Map from customerId to account.
    links: dict Map from customerId to child links.
    level: int level of the current account in the tree.
    list_acc: list acc output get from API
    list_mcc: list id of mcc 
    list_mcc_id: list id of mcc account
    list_dept: list dept of mcc account
    dept: dept of current account
  """
  
  if account['customerId'] in links:    
    if str(account['customerId']) in list_mcc_id:      
      dept = str(account['customerId'])

    for child_link in links[account['customerId']]:
      child_account = accounts[child_link['clientCustomerId']]

      # if (str(child_account['customerId']) in list_mcc_id):
      #   dept = str(child_account['customerId'])

      child_note = {
                  'customerId': child_account['customerId'],
                  'name': child_account['name'],
                  'level': level,
                  'children': [],
                  'deptId': dept,
                  'dept Name': list_mcc[list_mcc_id.index(dept)],
                  'dept': list_dept[list_mcc_id.index(dept)],
                  'entity':  list_entity[list_mcc_id.index(dept)]
      }

      account['children'].append(child_note) 

      if child_note not in list_acc:
        list_acc.append(child_note)  
           
      SaveAccountTree(child_note, accounts, links, level + 1, list_acc, list_mcc, list_mcc_id, list_dept, list_entity, dept)
  if level == 1:     
    return (account, list_acc)
      


def GetAllAcount(path_config, path_dept):
  # Initialize appropriate service.  
  adwords_client = adwords.AdWordsClient.LoadFromStorage(path_config)
  managed_customer_service = adwords_client.GetService(
      'ManagedCustomerService', version='v201708')
  

  # Construct selector to get all accounts.
  offset = 0
  selector = {
      'fields': ['CustomerId', 'Name'],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
  }
  more_pages = True
  accounts = {}
  child_links = {}
  parent_links = {}
  root_account = None

  while more_pages:
    # Get serviced account graph.
    page = managed_customer_service.get(selector)    

    if 'entries' in page and page['entries']:
      # Create map from customerId to parent and child links.
      if 'links' in page:
        for link in page['links']:
          if link['managerCustomerId'] not in child_links:
            child_links[link['managerCustomerId']] = []
          child_links[link['managerCustomerId']].append(link)
          if link['clientCustomerId'] not in parent_links:
            parent_links[link['clientCustomerId']] = []
          parent_links[link['clientCustomerId']].append(link)
      # Map from customerID to account.
      for account in page['entries']:
        accounts[account['customerId']] = account
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    print (int(page['totalNumEntries']))    
    more_pages = offset < int(page['totalNumEntries'])

  # Find the root account.
  for customer_id in accounts:
    if customer_id not in parent_links:
      root_account = accounts[customer_id]

  # =================Get list dept of all account ========================= 
  
  dept = pd.read_excel(path_dept)

  list_mcc = list(dept['MCC Level 3'])  
  list_mcc_id = list(dept['ID'])
  list_dept = list(dept['Dept'])
  list_entity = list(dept['Entity'])
  for i in range(len(list_mcc_id)):
    list_mcc_id[i] = list_mcc_id[i].replace('-', '')
  list_mcc.append(None)
  list_mcc_id.append(None)
  list_dept.append(None)
  list_entity.append(None)


  #===================Get account and store as tree =====================

  # Display account.
  list_acc = []  
  if root_account: 
    dept = None   
    if (str(root_account['customerId']) in list_mcc_id):      
      dept = str(root_account['customerId'])
    root_note = {
              'customerId': root_account['customerId'],
              'name': root_account['name'],
              'level': 0,
              'children': [],
              'deptId': dept,
              'dept Name': list_mcc[list_mcc_id.index(dept)],
              'dept': list_dept[list_mcc_id.index(dept)],
              'entity':  list_entity[list_mcc_id.index(dept)]
    }
    list_acc.append(root_note)
    root_note = SaveAccountTree(root_note, accounts, child_links, 1, list_acc, list_mcc, list_mcc_id, list_dept, list_entity, root_note['deptId'])
  return (root_note, list_acc)





