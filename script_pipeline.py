import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

def atribuindo_senha():
    
    with open('senhas/senhas.csv', 'r') as f:
        df = pd.read_csv(f)
        

query = 'https://api.meteomatics.com/2024-01-18T00:00:00Z--2024-01-21T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804/html'


r = requests.get(query, auth = HTTPBasicAuth('-', '-'))
#r = requests.get(query)


print(r.status_code)


if __name__ == "__main__":
    atribuindo_senha()