import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://covid19.gov.tl/dashboard/'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

stats = soup.find_all('span',attrs={'class':'wdt-column-sum-value'})
count = int(stats[5].text.replace(',',''))
print(count)

date = str(date.today())
new = pd.DataFrame({'Country': 'Timor',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Timor-Leste',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/Timor.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Timor.csv',index=False)
