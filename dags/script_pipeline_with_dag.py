import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import csv
import datetime 
import boto3
import mysql.connector
import os


from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum


    # +---+---+---+---+---+---+---+---+---+   
    # | F | u | n | c | t | i | o | n | s |
    # +---+---+---+---+---+---+---+---+---+      




#1 - FUNCTION
def atribuindo_acessos():
    
        
    try:
             
        
        with open('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather/senhas/senhas.csv', 'r') as f:

            df = pd.read_csv(f)
            
            
            login_api = df.loc[0, 'login']
            senha_api = df.loc[0, 'senha']
        
            login_aws_s3 = df.loc[1, 'login']
            senha_aws_s3 = df.loc[1, 'senha']
        
            login_aws_rds = df.loc[2, 'login']
            senha_aws_rds = df.loc[2, 'senha']
            host_aws_rds  = df.loc[2, 'hostname']
            port_aws_rds  = df.loc[2, 'port']
            database_aws_rds = df.loc[2, 'database']
            
            
            
            return login_api, senha_api, login_aws_s3, senha_aws_s3, login_aws_rds, senha_aws_rds, host_aws_rds, port_aws_rds, database_aws_rds

            
    except:
        problema = 'Nao encontrado diretorio senhas ou arquivo senha ou arquivo com formato errado'
        print(problema)
        print('exemplo de arquivo esta no arquivo exemplo_senha.csv')
        
        return ' ', ' '


#2 - FUNCTION
def criando_query_para_requisicao():
    
    link_site = 'https://api.meteomatics.com'
    
    #periodo de requisicao - sempre referente a amanha
    comeca = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
    termina = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T23:00:00Z'

    intervalo = 'PT1H'
    data_e_intervalo_de_dados = '{}--{}:{}'.format(comeca, termina, intervalo)
        
    #variaveis de entracao
    variaveis = 't_2m:C,precip_1h:mm,wind_speed_10m:ms'
    
    #lat_e_long SAO PAULO
    lat_long = '-23.7245,-46.6775'
    arquivo = 'csv'
    
    
    query = '{}/{}/{}/{}/{}'.format(link_site, data_e_intervalo_de_dados, variaveis, lat_long, arquivo)
    
    return query
    

#3 - FUNCTION
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


#4 - FUNCTION
def transformacao_dados(df):
    
    df.columns = ['DATA', 'TEMPERATURA_CELSIUS', 'PRECIP_MM', 'VELOCIDADE_VENTO_MS']
            
    df.drop(labels = [0], axis = 0, inplace = True)
            
    df['DATA'] = df['DATA'].str[0:16]
    
    df['CIDADE'] = 'SAO PAULO MIRANTE'    
    
    df = df[['CIDADE', 'DATA', 'TEMPERATURA_CELSIUS', 'PRECIP_MM', 'VELOCIDADE_VENTO_MS']]

    df['DATA'] = df['DATA'].str.replace('T', ' ')
    df['DATA'] = pd.to_datetime(df['DATA'], format = '%Y-%m-%d %H:%M')
    df['TEMPERATURA_CELSIUS'] = df['TEMPERATURA_CELSIUS'].astype('float')
    df['PRECIP_MM'] = df['PRECIP_MM'].astype('float')
    df['VELOCIDADE_VENTO_MS'] = df['VELOCIDADE_VENTO_MS'].astype('float')

    return df

#5 - FUNCTION
def aws_s3(LOGIN, SENHA, df):
    
    data = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
            
    nome_arquivo = 'SAO-PAULO-MIRANTE-dt-' + data + '.csv'       
    
    NOME_DO_ARQUIVO_LOCAL = nome_arquivo
    NOME_DO_ARQUIVO_NA_AWS = nome_arquivo
    
    
    df.to_csv(NOME_DO_ARQUIVO_LOCAL, index = False)
    
    
    AWS_ACCESS_KEY = LOGIN
    AWS_SECRET_KEY = SENHA
    AWS_S3_BUCKET_NAME = 'pipeline-weather-data'
    AWS_REGION = 'sa-east-1'
    
    s3_client = boto3.client(service_name = 's3',
                             region_name  = AWS_REGION,
                             aws_access_key_id = AWS_ACCESS_KEY,
                             aws_secret_access_key = AWS_SECRET_KEY) 
    

    
    try:
        
        response = s3_client.upload_file(NOME_DO_ARQUIVO_LOCAL, AWS_S3_BUCKET_NAME, NOME_DO_ARQUIVO_NA_AWS)
        print('file {}'.format(response))
        os.remove(NOME_DO_ARQUIVO_LOCAL)

    except:
        
        
        print('sem resposta')
    

#6 - FUNCTION
def aws_rds_mysql_insert(USER, PASSWORD, HOSTNAME, PORT, DATABASE, df):
       
    
    df_lista = df.values.tolist()
    
    
    conn = mysql.connector.connect(host = HOSTNAME,
                                   user = USER,
                                   password = PASSWORD,
                                   database = DATABASE,
                                   port = PORT)

    cursor = conn.cursor()
    
    query_insert = '''INSERT INTO datawarehouse.pipeline_weather_data 
                      (CIDADE, DATA_HORA, TEMPERATURA_CELSIUS, PRECIP_MM, VELICIDADE_VENTO_MS)
                      VALUES (%s, %s, %s, %s, %s)'''
    
    
    cursor.executemany(query_insert, df_lista)
    
    conn.commit()
    

    conn.close()



def extrai_dados():
    
    login_api, senha_api, login_aws_s3, senha_aws_s3, login_aws_rds, senha_aws_rds, host_aws_rds, port_aws_rds, database_aws_rds  = atribuindo_acessos()
        
    query_api = criando_query_para_requisicao()
    

    if login_api != ' ':
    
        df = requisicao_dados(login_api, senha_api, query_api)
        
        
        if df.shape[0] > 0:
            
            
            # +---+---+---+---+---+---+---+---+---+  
            # | T | r | a | n | s | f | o | r | m |   
            # +---+---+---+---+---+---+---+---+---+         
    
            df = transformacao_dados(df)
                    
            
            # +---+---+---+---+
            # | L | o | a | d |  
            # +---+---+---+---+       
            
            aws_s3(login_aws_s3, senha_aws_s3, df)
        
            aws_rds_mysql_insert(login_aws_rds, senha_aws_rds, host_aws_rds, port_aws_rds, database_aws_rds, df)
        

            
    else:
        
        print('sem login ou senha')



with DAG(
        'pipeline_weather_data',
         start_date = pendulum.datetime(2023, 2, 28, tz='UTC'),
         schedule_interval = '0 1 * * *', # every day 1:00 AM
         catchup=False
) as dag:
    

    tarefa_2 = PythonOperator(python_callable = extrai_dados,
                              task_id='tarefa_2')

    
    tarefa_2 
    














    # +---+---+---+---+ 
    # | M | a | i | n |
    # +---+---+---+---+

#if __name__ == "__main__":   
    
    # +---+---+---+---+---+---+---+ 
    # | E | x | t | r | a | c | t |
    # +---+---+---+---+---+---+---+

#if __name__ == "__main__":   
    
    
    
   

    
