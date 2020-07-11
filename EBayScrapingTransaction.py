from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime 
from datetime import date
from datetime import timedelta
import pydata_google_auth
import pandas as pd

from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import re

#Get your GCP credential
credentials = pydata_google_auth.get_user_credentials(
    ['https://www.googleapis.com/auth/bigquery'],
)

#BigQuery project_ID and table
project_id = 'xxxxx'
dataset_id = 'xxxxx'

#set BigQuery client
try:
    client = bigquery.Client(project=project_id, credentials=credentials)

    datasets = client.list_datasets()

    if datasets:
        for obj in datasets:
            print('-------->')
            print(vars(obj))
    else:
        print("This project does not contain any datasets.")
    
except ConnectionError as e:
  print(e)
  print(e.response.dict())   

#Run BigQuery SQL
query = "your query"
df = client.query(query).to_dataframe()  

#Beautiful Soup aetting
def GetPage(p_url): 
    try:    
      TmpSoup = ''
      headers = {
              "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0 nghttp2/",
              'Accept-Language':'ja,en-us,en;q=0.5'
              }
      # headers={"User-Agent":"Mozilla/5.0"}

      request = urllib.request.Request(url=p_url, headers=headers)
      page = urllib.request.urlopen(request)
      TmpSoup = BeautifulSoup(page,'html.parser')
      return TmpSoup

    except urllib.error.HTTPError as e:
        print('HTTPError ' + str(e.code)) 
        print(p_url)
        return "fail"
    except urllib.error.URLError as e:
        print('URLError ' + str(e.reason))
        print(p_url)
        return "fail"        
    except Exception as e:
        print('Exception ' + str(e))
        print(p_url)
        return "fail"
        
scrapresult = pd.DataFrame(columns = [
        'itemId',
        'title',
        'Price',
        'Quantity',
        'DateOfPurchased',
        'Date'        
        ]
)

#scraping logic
def ScrapEbayTran(p_df):
  try:
    k = -1
    for i, item in p_df.iterrows():
      soup = GetPage(item['URL'])
      if soup != 'fail':
        subsoup = soup.find_all("a",class_="vi-txt-underline")
        if len(subsoup) > 0:
          url2 = subsoup[0].attrs['href']
          soup = GetPage(url2)  
          soup = soup.find_all("td",class_="contentValueFont")
          m = len(soup)
          if m > 0:
            j = 0
            for j in range(0, m):
              if j % 3 == 0:
                k = k + 1
                scrapresult.at[k, 'Price'] = soup[j].text
              elif j % 3 == 1:
                scrapresult.at[k, 'Quantity'] = soup[j].text
              elif j % 3 == 2:    
                scrapresult.at[k, 'DateOfPurchased'] = soup[j].text
              scrapresult.at[k, 'itemId'] = int(item['itemId'])
              scrapresult.at[k, 'title'] = item['title']    
              scrapresult.at[k, 'Date'] = datetime.now()  
    return scrapresult   
  except Exception as e:
      print(e)

#Run Scraping
UL = len(df) 
table_id = 'maximal-coast-264420.Ebay_Keyword_Search_Result_QA.EBayTranScrapingCurrent'
for i in range(0,UL, 100):
    scrapresult = ScrapEbayTran(df[i:i+100:])
    scrapresult['DateOfPurchased2'] = pd.to_datetime(scrapresult['DateOfPurchased']).values.astype('M8[D]')
    scrapresult = scrapresult[(pd.to_datetime(scrapresult['DateOfPurchased2']) == '2020-07-02') | (scrapresult['Price'].str.contains(r'\$')==False)]
    client.insert_rows(client.get_table(table_id),scrapresult[0:10000:].dropna(subset=['itemId']).replace({'none': '', 'nan': 0}).fillna(0).values.tolist())


      
