import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'http://cdcmoh.gov.kh/'
req = requests.get(url)
text = req.text

count = int(text.split('ááá¸ááá¶áááááá¸ááááááááá½áâ ')[1].split(' ')[0])

date = str(date.today())
new = pd.DataFrame({'Country': 'Cambodia',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'CDCMOH',
                   'Units': 'tests performed'})

existing = pd.read_csv('automated_sheets/Cambodia.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Cambodia.csv',index=False)
