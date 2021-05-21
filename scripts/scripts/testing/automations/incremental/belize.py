import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://sib.org.bz/covid-19/by-the-numbers/'
req = urllib.request.urlopen(url)
soup = BeautifulSoup(req.read(), 'html.parser')

stats = soup.select('div.stats-number.ult-responsive')
count = int(stats[0]['data-counter-value'])

date = str(date.today())
new = pd.DataFrame({'Country': 'Belize',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Statistical Institute of Belize',
                   'Units': 'tests performed'})

existing = pd.read_csv('automated_sheets/Belize.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Belize.csv',index=False)
