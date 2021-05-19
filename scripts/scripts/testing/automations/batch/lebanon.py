import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date

url = 'https://corona.ministryinfo.gov.lb'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

count = int(data.find_all('h1',attrs={'class':'s-counter-3 s-counter'}).text)
date = str(date.today())
new = pd.DataFrame({'Country': 'Lebanon',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Lebanon Ministry of Information',
                   'Units': 'number of tests'})

existing = pd.read_csv('automated_sheets/Lebanon.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Lebanon.csv',index=False)
