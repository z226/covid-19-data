import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://covid-19.sr'
req = urllib.request.urlopen(url)
soup = BeautifulSoup(req.read(), 'html.parser')

stats = soup.select('div.stats-number.ult-responsive')
daily = int(stats[-2]['data-counter-value'])

date = str(date.today())
new = pd.DataFrame({'Country': 'Suriname',
                   'Date': [date],
                   'Daily change in cumulative total': daily,
                   'Source URL': url,
                   'Source label': 'Government of Suriname',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/Suriname.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Suriname.csv',index=False)
