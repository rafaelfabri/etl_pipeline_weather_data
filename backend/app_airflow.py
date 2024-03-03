

import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime

import os

sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema


class callAPI():


    def senhas(self):
        
        self.credenciais_api = {'login': os.environ.get('LOGIN_API_WEATHER'),
                                'senha': os.environ.get('SENHA_API_WEATHER')}
        
        self.credenciais_aws = {'aws_access_key_id': os.environ.get('LOGIN_AWS_S3'),
                                'aws_secret_access_key': os.environ.get('SENHA_AWS_S3')}
            
        
    
    def infoDataToRequest(self):
        
        main_link = 'https://api.meteomatics.com'
        intervalo = 'PT1H'
        variaveis = 't_2m:C,precip_1h:mm,wind_speed_10m:ms'
        lat_long = '-23.7245,-46.6775'
        file = 'csv'
        
        self.info_to_request = {'main_link' : main_link,
                                'intervalo' : intervalo,
                                'variaveis' : variaveis,
                                'lat_long'  : lat_long,
                                'file'      : file}
        
        print(self.credenciais_api)
    
    def callInstanceAPI(self):
                
        self.instance_api = APICollector(self.credenciais_api, 
                                         self.credenciais_aws, 
                                         self.info_to_request)    
        
    def callStartETL(self):
        
        self.instance_api.startETL()
      
