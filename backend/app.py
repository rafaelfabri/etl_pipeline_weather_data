import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
import datetime
sys.path.append('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather')
from backend.modulos.api import APICollector
from backend.contrato.schema import ContratoSchema


def main():
    
    schema = ContratoSchema
    print(schema)
        
    api_login, api_senha = senhas()

    main_link, comeca, termina, intervalo, variaveis, lat_long, file = variavies_extracao()


    instance_api = APICollector(schema)
    
    query = instance_api.create_query(main_link = main_link, 
                                      data_inicial = comeca,
                                      data_final = termina,
                                      intervalo = intervalo,
                                      variaveis = variaveis ,
                                      lat_long = lat_long,
                                      file = file)    

    print(query)
    
    df = instance_api.Start(query = query,
                            login = api_login,
                            senha = api_senha)
  
    print(df)


def variavies_extracao():
    
    main_link = 'https://api.meteomatics.com'
    
    #periodo de requisicao - sempre referente a amanha
    comeca = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
    termina = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T23:00:00Z'

    intervalo = 'PT1H'
        
    #variaveis de entracao
    variaveis = 't_2m:C,precip_1h:mm,wind_speed_10m:ms'
    
    #lat_e_long SAO PAULO
    lat_long = '-23.7245,-46.6775'
    file = 'csv'
    
    
    return main_link, comeca, termina, intervalo, variaveis, lat_long, file
    

def senhas():
    senhas = pd.read_csv('/home/rafaelfabrichimidt/Documentos/Projetos/Python/senhas/pipeline_api_weather/senhas.csv')
    api_login = senhas[senhas['dispositivo'] == 'api_weather']['login'].values[0]
    api_senha = senhas[senhas['dispositivo'] == 'api_weather']['senha'].values[0]
    
    return api_login, api_senha



if __name__ == "__main__":
    
    main()

