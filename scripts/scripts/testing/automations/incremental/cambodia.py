import os

import pandas as pd
import requests
from datetime import date

url = 'http://cdcmoh.gov.kh/'
req = requests.get(url)
text = req.text

count = int(text.split('ááá¸ááá¶áááááá¸ááááááááá½áâ ')[1].split(' ')[0])

date_str = date.today().strftime("%Y-%m-%d")
df = pd.DataFrame({
    'Country': 'Cambodia',
    'Date': [date_str],
    'Cumulative total': count,
    'Source URL': url,
    'Source label': 'CDCMOH',
    'Units': 'unclear'
})

output_file = 'automated_sheets/Cambodia.csv'
if os.path.isfile(output_file):
    existing = pd.read_csv(output_file)
    df = pd.concat([df, existing]).sort_values('Date', ascending=False)

df.to_csv(output_file, index=False)