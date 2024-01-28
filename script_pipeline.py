import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import csv
import datetime 
import boto3



    


def atribuindo_acessos():
    
    try:
        
        with open('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather/senhas/senhas.csv', 'r') as f:
            
            df = pd.read_csv(f)
            
            login_api = df.loc[0, 'login']
            senha_api = df.loc[0, 'senha']
            
            login_aws = df.loc[1, 'login']
            senha_aws = df.loc[1, 'senha']
            
            login_aws_rds = df.loc[2, 'login']
            senha_aws_rds = df.loc[2, 'senha']
            host_aws_rds  = df.loc[2, 'hostname']
            port_aws_rds  = df.loc[2, 'port']
            database_aws_rds = df.loc[2, 'database']
            
            
            
            return login_api, senha_api, login_aws, senha_aws, login_aws_rds, senha_aws_rds, host_aws_rds, port_aws_rds, database_aws_rds

            
    except:
        problema = 'Nao encontrado diretorio senhas ou arquivo senha ou arquivo com formato errado'
        print(problema)
        print('exemplo de arquivo esta no arquivo exemplo_senha.csv')
        
        return ' ', ' '


def criando_query_para_requisicao():
    
    link_site = 'https://api.meteomatics.com'
    
    #periodo de requisicao - sempre referente a ontem
    ontem = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
    hoje = (datetime.datetime.today() + datetime.timedelta(days = 2)).strftime('%Y-%m-%d') + 'T23:00:00Z'
    intervalo = 'PT1H'
    data_e_intervalo_de_dados = '{}--{}:{}'.format(ontem, hoje, intervalo)
    
    
    #variaveis de entracao
    variaveis = 't_2m:C,precip_1h:mm,wind_speed_10m:ms'

    
    #lat_e_long
    lat_long = '-23.7245,-46.6775'
    arquivo = 'csv'
    
    
    query = '{}/{}/{}/{}/{}'.format(link_site, data_e_intervalo_de_dados, variaveis, lat_long, arquivo)
    
    return query
    

def requisicao_dados(login, senha, query):
    
    
    try:
        
        r = requests.get(query, auth = HTTPBasicAuth(login, senha))
        print(query)
        print(r.status_code)
        if r.status_code == 200:
            
            print('Requisicao feita com Sucesso!')
            
            dados_csv = r.content.decode('utf-8')
            
            dados_csv = csv.reader(dados_csv.splitlines(), delimiter = ';')
                        
            lista = []
            
            for row in dados_csv:
                
                lista.append(row)
                        
            df = pd.DataFrame(lista)
                        
            return df
            
        
        else:
            
            print('Resquisicao Negada')
            
            df = pd.DataFrame()
            
            return df

            
    except:
        
        print('link com erro')
        
        df = pd.DataFrame()
            
        return df
        


def aws_s3(login, senha, df):
    
    df.to_csv('test.csv')
    
    NOME_DO_ARQUIVO_LOCAL = 'test.csv'
    NOME_DO_ARQUIVO_NA_AWS = 'test.csv'
    
    
    AWS_ACCESS_KEY = login
    AWS_SECRET_KEY = senha
    AWS_S3_BUCKET_NAME = 'pipeline-weather-data'
    AWS_REGION = 'sa-east-1'
    
    s3_client = boto3.client(service_name = 's3',
                             region_name  = AWS_REGION,
                             aws_access_key_id = AWS_ACCESS_KEY,
                             aws_secret_access_key = AWS_SECRET_KEY)
    
    print(s3_client)
    
    response = s3_client.upload_file(NOME_DO_ARQUIVO_LOCAL, AWS_S3_BUCKET_NAME, NOME_DO_ARQUIVO_NA_AWS)

    
    try:
        
        
        print('file {}'.format(response))
        
    except:
        
        
        print('sem resposta')
    
    
def aws_rds_mysql():
    print('em construcao')


if __name__ == "__main__":   
    
    
    login_api, senha_api, login_aws, senha_aws, login_aws_rds, senha_aws_rds, host_aws_rds, port_aws_rds, database_aws_rds  = atribuindo_acessos()
        
    query = criando_query_para_requisicao()
    

    if login_api != ' ':
    
        df = requisicao_dados(login_api, senha_api, query)
        
        if df.shape[0] > 0:
            
            df.columns = ['DATA', 'TEMPERATURA_CELSIUS', 'PRECIP_MM', 'VELOCIDADE_VENTO_MS']
            
            df.drop(labels = [0], axis = 0, inplace = True)
            
            print(df)
            
            
            #aws_s3(login_aws, senha_aws, df)
        
            #aws_rds_mysql()
        
    else:
        
        print('sem login ou senha')