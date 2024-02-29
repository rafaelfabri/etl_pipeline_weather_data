import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime
sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema



def senhasAPI():
    
    senhas = pd.read_csv('/home/rafaelfabrichimidt/Documentos/projetos/artigos/senhas/pipeline_api_weather/senhas.csv')
    
    credenciais_api = {'login': senhas[senhas['dispositivo'] == 'api_weather']['login'].values[0],
                       'senha': senhas[senhas['dispositivo'] == 'api_weather']['senha'].values[0]}
    
    return credenciais_api


def main():
    
    schema = ContratoSchema
            
    
    credenciais_api = senhasAPI()


    #instanciando API
    instance_api = APICollector(credenciais_api)    
    
    
    #datas de intervalo de extracao
    comeca = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
    termina = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T23:00:00Z'
    
    
    #criando query para extracao
    query = instance_api.createQuery( main_link    = 'https://api.meteomatics.com', 
                                      data_inicial = comeca,
                                      data_final   = termina,
                                      intervalo    = 'PT1H',
                                      variaveis    = 't_2m:C,precip_1h:mm,wind_speed_10m:ms' ,
                                      lat_long     = '-23.7245,-46.6775',
                                      file         = 'csv')    
    
    

    df = instance_api.startETL(query)
  
    
    print(df)



if __name__ == "__main__":
    
    main()

