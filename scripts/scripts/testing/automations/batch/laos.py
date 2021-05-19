import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://www.covid19.gov.la/index.php'
#Only works when making two requests. The first always returns an error.
try:
    data = BeautifulSoup(requests.get(url).text)
except:
    data = BeautifulSoup(requests.get(url).text)

stats = data.find_all('p')
count = int(stats[11].text.split(' ')[0].replace(',',''))

date = str(date.today())
new = pd.DataFrame({'Country': 'Laos',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Laos',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/Laos.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Laos.csv',index=False)
