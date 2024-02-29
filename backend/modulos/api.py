import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import csv
import boto3




class APICollector():
    
    def __init__(self, credenciais_api):
        self._credenciais_api = credenciais_api
        self._schema = None
        self._credenciais_aws = None
        
        
        
    def createQuery(self, main_link, data_inicial, data_final, intervalo, variaveis, lat_long, file):
        
        """Metodo utilizado para definir a query/link de requesicao dos dados"""
        
        
        data_e_intervalo_de_dados = '{}--{}:{}'.format(data_inicial, data_final , intervalo)
            
        query = '{}/{}/{}/{}/{}'.format(main_link, data_e_intervalo_de_dados, variaveis, lat_long, file)
        
        print(query)
        
        return query
        
    
    def startETL(self, query):
        
        """startETL é um método que realiza a extração, transformação e carregamento dos dados meteorológicos vindo da API \n
           para isso o startETL utiliza de 4 metodos: 
               requestData
               
               transformCsvToDataframe
               
               transformDataframe
               
               
               """
        
        response = self.requestData(query)
        
        if response is not None:
        
            df = self.transformCsvToDataframe(response)
            
            df = self.transformDataframe(df)
            
            self.loadBucketS3(df)
            
            
            return df
        
            
        
        
        
        else:
            
            return None
    
    def requestData(self, query):
        
        try:
            response = requests.get(query, 
                                    auth = HTTPBasicAuth(self._credenciais_api['login'], 
                                                         self._credenciais_api['senha']))
            
            if response.status_code == 200:
                print('Requisicao feita com Sucesso!')
                return response
            
            else: 
                print('Resquisicao Negada')
                return None
            
        except:
            print('query errada')
            return None
        
    
    def transformCsvToDataframe(self, response):
        
        
        dados_csv = response.content.decode('utf-8')
        
        dados_csv = csv.reader(dados_csv.splitlines(), delimiter = ';')
                    
        lista = []
        
        for row in dados_csv:
            
            lista.append(row)
                    
        df = pd.DataFrame(lista)
                    
        return df
        response = self.response
    
    
    
    def transformDataframe(self, df):
        
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
    
    
    def loadBucketS3(self, df):
        
        senhas = pd.read_csv('/home/rafaelfabrichimidt/Documentos/projetos/artigos/senhas/pipeline_api_weather/senhas.csv')
        
        NOME_ARQUIVO = 'A.parquet'
        
        df.to_parquet(NOME_ARQUIVO)
        
        AWS_ACCESS_KEY = senhas[senhas['dispositivo'] == 'awsS3']['login'].values[0]
        AWS_SECRET_KEY = senhas[senhas['dispositivo'] == 'awsS3']['senha'].values[0]
        AWS_S3_BUCKET_NAME = 'weather-data-storage'
        AWS_REGION = 'us-east-1'
        
        s3_client = boto3.client(service_name = 's3',
                                 region_name = AWS_REGION,
                                 aws_access_key_id = AWS_ACCESS_KEY,
                                 aws_secret_access_key = AWS_SECRET_KEY)
        
        s3_client.upload_file(NOME_ARQUIVO, AWS_S3_BUCKET_NAME, NOME_ARQUIVO)
    
    
    
    def verificacao_contrato():
        
        
        self._schema
    
    