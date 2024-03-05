

import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime

import os

#sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data')
sys.path.append('~/etl_pipeline_weather_data')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema

from dotenv import load_dotenv

load_dotenv()


class callAPICollector():


    def senhas(self, **context):
        
        credenciais_api = {'login': os.environ.get('LOGIN_API_WEATHER'),
                                'senha': os.environ.get('SENHA_API_WEATHER')}
        
        credenciais_aws = {'aws_access_key_id': os.environ.get('LOGIN_AWS_S3'),
                                'aws_secret_access_key': os.environ.get('SENHA_AWS_S3')}
            
        
        context['task_instance'].xcom_push(key = 'credenciais_api', value = credenciais_api)
        context['task_instance'].xcom_push(key = 'credenciais_aws', value = credenciais_aws)
        
    
    def infoDataToRequest(self, **context):
        
        main_link = 'https://api.meteomatics.com'
        intervalo = 'PT1H'
        variaveis = 't_2m:C,precip_1h:mm,wind_speed_10m:ms'
        lat_long = '-23.7245,-46.6775'
        file = 'csv'
        
        info_to_request = {'main_link' : main_link,
                           'intervalo' : intervalo,
                           'variaveis' : variaveis,
                           'lat_long'  : lat_long,
                           'file'      : file}
    
        context['task_instance'].xcom_push(key = 'info_to_request', value = info_to_request)
    
    
    def callInstanceAPI(self, **context):
        
        credenciais_api = context['task_instance'].xcom_pull(key = 'credenciais_api', task_ids = 'definindo_credenciais')
        credenciais_aws = context['task_instance'].xcom_pull(key = 'credenciais_aws', task_ids = 'definindo_credenciais')
        info_to_request = context['task_instance'].xcom_pull(key = 'info_to_request', task_ids = 'info_request')
        
        instance_api = APICollector(credenciais_api, 
                                    credenciais_aws, 
                                    info_to_request)    
        
        instance_api.startETL()

