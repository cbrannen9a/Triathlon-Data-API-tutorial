import requests
import json
from pprint import pprint

apikey = 'YOUR_API_KEY'
url = 'https://api.triathlon.org/v1/athletes/'
query = '{0}?output=basic'
local_filename = 'test.txt'

params = {}
headers = {'content-type': 'application/json',
           'Accept-Charset': 'UTF-8', 'apikey': apikey}
r = requests.get(url + query.format(75945),
                 headers=headers, params=params, stream=True)

with open(local_filename, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)

data = json.load(open(local_filename))
pprint(data)
