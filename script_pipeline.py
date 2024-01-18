import requests
from requests.auth import HTTPBasicAuth


query = 'https://api.meteomatics.com/2024-01-18T00:00:00Z--2024-01-21T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804/html'


r = requests.get(query, auth = HTTPBasicAuth('-', '-'))
#r = requests.get(query)


print(r.status_code)