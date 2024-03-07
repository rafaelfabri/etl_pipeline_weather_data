import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import csv
import datetime




class APICollector():
    
    """Coletor de dados API Methoroligcs
       
        A classe APICollector tem como objetivo a extracao de dados, transformacao e carregamento de dados
       
        APICollector possui 3 argumentos obrigatorios: credenciais_api, credenciais_aws, info_to_request
            
        Faca a instancia da APICollector em uma variavel e passe essas tres informacoes

        APICollector(credenciais_api: dict | credenciais_aws: dict  | info_to_request: dict)
        
       """
    
    def __init__(self, credenciais_api, info_to_request, bucketS3):
        self.bucketS3 = bucketS3
        self._credenciais_api = credenciais_api     
        self.query_request = self.createQuery(info_to_request)
        
        

        
    def createQuery(self, info_to_request):
        
        """Metodo utilizado para definir a query/link de requesicao dos dados"""
        
        
        data_e_intervalo_de_dados = '{}--{}:{}'.format(self.extractDate()['start'], 
                                                       self.extractDate()['end'], 
                                                       info_to_request['intervalo'])
            
        query = '{}/{}/{}/{}/{}'.format(info_to_request['main_link'],
                                        data_e_intervalo_de_dados, 
                                        info_to_request['variaveis'], 
                                        info_to_request['lat_long'], 
                                        info_to_request['file'])
        
        print(query)
        
        return query
        
    
    def extractDate(self):
        
        start = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
        end = (datetime.datetime.today() + datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T23:00:00Z'
        
        return {'start' : start, 'end' : end}
    
    
    
    
    def startETL(self):
        
        """startETL é um método que realiza a extração, transformação e carregamento dos dados meteorológicos vindo da API \n
           para isso o startETL utiliza de 4 metodos: 
               
            requestData
            
            transformCsvToDataframe
            
            transformDataframe
            
            loadBucketS3
               
               """
        
        response = self.requestData(self.query_request)
        
        if response is not None:
        
            df = self.transformCsvToDataframe(response)
            
            df = self.transformDataframe(df)
            
            NOME_ARQUIVO = 'SAO PAULO-WEATHER-{}-{}.parquet'.format(self.extractDate()['start'],
                                                                    self.extractDate()['end'])
            
            #self.load(df, NOME_ARQUIVO)
            self.bucketS3.storageUploadBucket(df, NOME_ARQUIVO, 'weather-data-storage', 'us-east-1')            
            print('startETL executado com sucesso')
        else:
            print('response vazio')
    
    
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
    
    
    
    def load(self, df, NOME_ARQUIVO):
                

        
        df.to_parquet(NOME_ARQUIVO)
        
        
        
        
    #    s3_client = boto3.client(service_name = 's3',
    #                             region_name = AWS_REGION,
    #                             aws_access_key_id = AWS_ACCESS_KEY,
    #                             aws_secret_access_key = AWS_SECRET_KEY)
        
    #    s3_client.upload_file(NOME_ARQUIVO, AWS_S3_BUCKET_NAME, NOME_ARQUIVO)
    
    
    
    def verificacao_contrato():
        
        
        return None    
    