import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://www.gouv.bj/coronavirus/'
req = urllib.requests.urlopen(url)
soup = BeautifulSoup(req.read(), 'html.parser')

stats = soup.find_all('h2',attrs={'class','h1 adapt white regular'})

count = int(stats[0].text) + int(stats[1].text)
date = str(date.today())
new = pd.DataFrame({'Country': 'Benin',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Benin',
                   'Units': 'unclear'")

existing = pd.read_csv('automated_sheets/Benin.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Benin.csv',index=False)
