from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pydata_google_auth
import pandas as pd
import numpy as np
from datetime import datetime 

from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import re

from janome.analyzer import Analyzer
from janome.charfilter import UnicodeNormalizeCharFilter, RegexReplaceCharFilter
from janome.tokenizer import Tokenizer as JanomeTokenizer  # sumyのTokenizerと名前が被るため
from janome.tokenfilter import POSKeepFilter, ExtractAttributeFilter

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

def make_clickable(val):
    return '<a target="_blank" href="{}">{}</a>'.format(val,'link')
    
credentials = pydata_google_auth.get_user_credentials(
    ['https://www.googleapis.com/auth/bigquery'],
)

project_id = 'xxxxx'
dataset_id = 'xxxxx'

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
  
query = 'your query'
df = client.query(query).to_dataframe() 

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
        
def getItemInfo(p_url, p_itemId, p_seqnum): 
    
    #ECSite = 'exclude'
    ECSite = ''
    store = ''
    product = ''
    url = ''
    price = ''
    global GImageSearchLinkDF
    
    subsoup = p_url[:p_url.find('/',10)]

    #japan site only
    if subsoup == "https://item.rakuten.co.jp":
        #soup = GetPage(p_url)
        ECSite = 'item.rakuten'
    elif subsoup == "https://search.rakuten.co.jp":
        #soup = GetPage(p_url)
        ECSite = 'search.rakuten'
    elif subsoup == "https://tower.jp":
        ECSite = subsoup
    elif subsoup == "https://www.hmv.co.jp":
        ECSite = 'hmv.co.jp'        
    elif subsoup == "https://shop.mu-mo.net":
        ECSite = 'shop.mu-mo'                
    elif subsoup == "https://shopping.yahoo.co.jp":
        #soup = GetPage(p_url)
        ECSite = 'shopping.yahoo'
    elif subsoup == "https://auctions.yahoo.co.jp":
        #soup = GetPage(p_url)
        ECSite = 'auctions.yahoo'      
    elif subsoup == "https://lohaco.jp":
        #soup = GetPage(p_url)
        ECSite = 'lohaco.jp'  
    elif subsoup == "https://aucfan.com":
        ECSite = 'aucfan.com'          
    elif subsoup == "https://www.amazon.co.jp":
        #soup = GetPage(p_url)
        ECSite = 'amazon.co.jp'             
    elif subsoup == "https://www.mercari.com":
        #soup = GetPage(p_url)
        ECSite = 'mercari'        
    elif subsoup == "https://kakaku.com":
        if p_url.find('/item/') > 0:
            soup = GetPage(p_url)
            ECSite = 'kakaku.com'
            m = len(soup.find_all("div",class_="itemPrice")) - 1
            j = 0 
            for j in range(0, m):
                price = soup.find_all("div",class_="itemPrice")[j].text
                if price != "":
                    product = soup.find_all("p",class_="itemTtlBox")[j].get_text().replace("\n", "").replace("\r", "").replace("\u3000", " ")
                    store = soup.find_all("p",class_="shopIcon")[j].get_text().replace("\n", "").replace("\r", "").replace("\u3000", " ")
                    url = soup.find_all("div",attrs={'class':'allbtn','id':re.compile('allbtnList?')})[j].find('a').attrs['href']
    elif subsoup in ["https://bandai-hobby.net", "https://sega.jp", "https://www.biccamera.com", "https://www.mechakaitai.com", "https://www.ms-plus.com", "https://www.sony.jp", "https://www.yodobashi.com"]:
        ECSite = subsoup
    else:
        if subsoup[len(subsoup)-5:] == 'co.jp':
          ECSiteDF = subsoup
        
    if ECSite != "exclude" and ECSite != ECSite[:4] != "http" :        
      GImageSearchLinkDF = GImageSearchLinkDF.append(pd.DataFrame.from_dict({"itemId":[int(p_itemId)],"seqnum":[p_seqnum],"ECSite":[ECSite],"store":[store],"product":[product],"price":[price],"url":[url],"Date":[datetime.now().strftime("%Y-%m-%d")]}),ignore_index = True)      

    return ECSite
    
row = 0
GImageSearchDF = pd.DataFrame()
GImageSearchLinkDF = pd.DataFrame()

for row in range(len(df.URL)-1):
  soup = ''
  soup = GetPage(df.URL[row])
  itemId = df.itemId[row]   
  tmp_img = soup.find_all("meta",property="og:image")[0].attrs['content']  
  i = 0
  k = 0
  for i in range(0, 11, 10):  
      url = "https://www.google.co.jp/searchbyimage?hl=ja%safe=off&start=" + str(i) + "&site=search&image_url=" + tmp_img
      soup = GetPage(url)
      if soup != "fail":
        j = 0
        m = len(soup.find_all("div",class_="r")) 
        for j in range(0, m):  
            subsoup = soup.find_all("div",class_="r")[j]  
            title = subsoup.find_all('h3')[0].contents[0]
            url = subsoup.find_all('a')[0].attrs['href']
            k = k + j + 1
            r = getItemInfo(url,itemId, k)
            if r != "exclude" and r != r[:4] != "http" : 
              GImageSearchDF = GImageSearchDF.append(pd.DataFrame.from_dict({"itemId":[int(itemId)],"seqnum":[k],"title":[title],"ImageSearchUrl":[url],"Date":[datetime.now().strftime("%Y-%m-%d")]}),ignore_index = True)  

GImageSearchDF.style.format({'ImageSearchUrl':make_clickable})

