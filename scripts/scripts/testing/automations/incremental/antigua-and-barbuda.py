import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://covid19.gov.ag'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

stats = soup.find_all('p',attrs={'class':'case-Number'})
count = int(stats[3].text)
print(count)

date = str(date.today())
new = pd.DataFrame({'Country': 'Antigua and Barbuda',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Antigua and Barbuda',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/antigua-and-barbuda.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/antigua-and-barbuda.csv',index=False)
