import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime
from dotenv import load_dotenv
import os

load_dotenv('/home/ubuntu/.env')


sys.path.append('/home/ubuntu/etl_pipeline_weather_data/')

#os.path.dirname('rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data/')

#sys.path.append('home/ubuntu/etl_pipeline_weather_data/')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema
from backend.modulos.bucket_s3 import BucketS3




def senhas():
    
    credenciais_api = {'login': os.environ.get('LOGIN_API_WEATHER'),
                       'senha': os.environ.get('SENHA_API_WEATHER')}
    
    credenciais_aws = {'access_key_id': os.environ.get('LOGIN_AWS_S3'),
                       'secret_access_key': os.environ.get('SENHA_AWS_S3')}
        
    
    return credenciais_api, credenciais_aws


def main():
    
    schema = ContratoSchema
            
    
    credenciais_api, credenciais_aws = senhas()

    info_to_request = {'main_link' : 'https://api.meteomatics.com',
                       'intervalo' : 'PT1H',
                       'variaveis' : 't_2m:C,precip_1h:mm,wind_speed_10m:ms',
                       'lat_long'  : '-23.7245,-46.6775',
                       'file'      : 'csv'}
    
    instance_BucketS3 = BucketS3(credenciais_aws, 'weather-data-storage', 'us-east-1')

    #instanciando API
    instance_api = APICollector(credenciais_api, instance_BucketS3, info_to_request)    
    
    instance_api.startETL()
    
    #if df is not None:
        
        
        #cf = cloudFunctions(credenciais_aws)
        
        #cf.storageUploadBucket(df, NOME_ARQUIVO, 'weather-data-storage', 'us-east-1')
    
  

if __name__ == "__main__":
    
    main()

