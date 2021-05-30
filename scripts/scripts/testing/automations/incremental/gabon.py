import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://monitoring-covid19gabon.ga'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

stats = soup.find_all('h3')
count = int(stats[2].text)
print(count)

date = str(date.today())
new = pd.DataFrame({'Country': 'Gabon',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Gabon',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/Gabon.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Gabon.csv',index=False)
