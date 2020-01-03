from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from oauth2client import client
from oauth2client import tools
from oauth2client import file
from bs4 import BeautifulSoup
from urlparse import urljoin
import numpy as np
import httplib2
import argparse
import urllib2 
import csv


def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
  """Get a service that communicates to a Google API.
  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.
  Returns:
    A service that is connected to the specified API.
  """

  credentials = ServiceAccountCredentials.from_p12_keyfile(
    service_account_email, key_file_location, scopes=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service

def get_first_profile_id(service):
  # Declaring variable 
  profiles_list = []
  ua_list = []
  url_list = []
  column_url = []
  column_share_k = []
  column_share = []

  # Hostname to complete page url in order to scrap them properly
  base = 'http://www.hostname.com'

  # Accessing property 
  properties = service.management().webproperties().list(
    accountId='~all').execute()

  # Building property list
  for property in properties.get('items', []):
    ua_list.append(property.get('id'))

  #looping through ua list
  for ua in ua_list:
    profiles = service.management().profiles().list(
        accountId='xxxxxxxxx',
        webPropertyId= ua ).execute()

  # Building a list of Profile id
    for profile in profiles.get('items', []):
      profiles_list.append(profile.get('id'))

  # Loop through the profiles_list and get the best pages for each profile 
  for profile in profiles_list:
    response = service.data().ga().get(
      ids='ga:' + profile,
      start_date='1daysAgo',
      end_date='today',
      metrics='ga:sessions',
      dimensions='ga:pagePath',
      sort='-ga:sessions',
      filters='ga:sessions>400').execute()

    url_list.extend(row[0] for row in response.get('rows', []))

    # Building a list of full url (Hostname + Page path)
    fullurl = [urljoin(base, h) for h in url_list]

    # Scraping some data from the url list
    for url in fullurl:  
            
      try:
          page = urllib2.urlopen(url)
      except urllib2.HTTPError as e:
              if e.getcode() == 404: # eheck the return code
                  print url
                  continue
      soup = BeautifulSoup(page, 'html.parser')

      # Take out the <div> of name and get its value
      name_box = soup.find(attrs={'class': 'nb-shares'})
      if name_box is None:
        continue
      share_count = name_box.text.strip() # strip() is used to remove starting and trailing

      # save the data in tuple
      column_url.append(url)
      column_share_k.append(share_count)

      # Format the data scraped
      column_share = [int(1000*float(x.replace('k', ''))) if 'k' in x else int(x) for x in column_share_k]

    #export in csv
    csv_out = open(response.get('profileInfo').get('profileName') + '.csv', 'wb')
    mywriter = csv.writer(csv_out)
    for row in zip(column_url, column_share):
      mywriter.writerow([row])
    csv_out.close()

    #reset list
    fullurl = []
    url_list = []
    share_count = []
    column_share_k = []
    column_url = []
    column_share = []

def main():
  # Define the auth scopes to request.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']

  # Use the developer console and replace the values with your
  # service account email and relative location of your key file.
  service_account_email = 'email@nameproject.iam.gserviceaccount.com'
  key_file_location = 'yourfilename.p12'

  # Authenticate and construct service.
  service = get_service('analytics', 'v3', scope, key_file_location,
    service_account_email)
  profile = get_first_profile_id(service)

if __name__ == '__main__':
  main()