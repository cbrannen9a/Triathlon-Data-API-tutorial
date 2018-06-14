import requests
import json
from pprint import pprint

apikey = 'YOUR_API_KEY'
url = 'https://api.triathlon.org/v1/statistics/results?'
query = "analysis=minimum&target_property=position&filters=program.id,eq,{0}&group_by=athlete.name|athlete.id|program.start_time|program.wetsuit|splits.swim|splits.swim_distance|splits.bike|splits.bike_distance|splits.run|splits.run_distance|splits.t1|splits.t2|temperature.air|temperature.water|format|event.id|event.country|event.venue|finish_time"
local_filename = 'test.txt'

params = {}
headers = {'content-type': 'application/json',
           'Accept-Charset': 'UTF-8', 'apikey': apikey}
r = requests.get(url + query.format('321351'),
                 headers=headers, params=params, stream=True)

with open(local_filename, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)

data = json.load(open(local_filename))
pprint(data)
