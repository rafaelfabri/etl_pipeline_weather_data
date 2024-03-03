import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime

import os

sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema



def senhas():
    
    #senhas = pd.read_csv('/home/rafaelfabrichimidt/Documentos/projetos/artigos/senhas/pipeline_api_weather/senhas.csv')
    
    #credenciais_api = {'login': senhas[senhas['dispositivo'] == 'api_weather']['login'].values[0],
    #                   'senha': senhas[senhas['dispositivo'] == 'api_weather']['senha'].values[0]}
    
    credenciais_api = {'login': os.environ.get('LOGIN_API_WEATHER'),
                       'senha': os.environ.get('SENHA_API_WEATHER')}
    
    credenciais_aws = {'aws_access_key_id': os.environ.get('LOGIN_AWS_S3'),
                       'aws_secret_access_key': os.environ.get('SENHA_AWS_S3')}
        
    
    return credenciais_api, credenciais_aws


def main():
    
    schema = ContratoSchema
            
    
    credenciais_api, credenciais_aws = senhas()

    info_to_request = {'main_link' : 'https://api.meteomatics.com',
                       'intervalo' : 'PT1H',
                       'variaveis' : 't_2m:C,precip_1h:mm,wind_speed_10m:ms',
                       'lat_long'  : '-23.7245,-46.6775',
                       'file'      : 'csv'}
    

    #instanciando API
    instance_api = APICollector(credenciais_api, credenciais_aws, info_to_request)    
    
    instance_api.startETL()
  

if __name__ == "__main__":
    
    main()

