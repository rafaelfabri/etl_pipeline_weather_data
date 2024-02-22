import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import csv




class APICollector():
    
    def __init__(self, schema):
        self._schema = schema
        
    def Start(self, query, login, senha):
        
        response = self.request(query, login, senha)
        
        if response is not None:
        
            df = self.transform_request_to_data(response)
            
            
        
            return df
        
        else:
            
            return None
    
    def request(self, query, login, senha):
        
        try:
            response = requests.get(query, 
                                    auth = HTTPBasicAuth(login, senha))
            
            if response.status_code == 200:
                print('Requisicao feita com Sucesso!')
                return response
            
            else: 
                print('Resquisicao Negada')
                return None
            
        except:
            print('query errada')
            return None
        
    
    def transform_request_to_data(self, response):
        
        
        dados_csv = response.content.decode('utf-8')
        
        dados_csv = csv.reader(dados_csv.splitlines(), delimiter = ';')
                    
        lista = []
        
        for row in dados_csv:
            
            lista.append(row)
                    
        df = pd.DataFrame(lista)
                    
        return df
        response = self.response
    
    
    def verificacao_contrato():
        self._schema
    
    
    def create_query(self, 
                     main_link, 
                     data_inicial, 
                     data_final, 
                     intervalo, 
                     variaveis,
                     lat_long, 
                     file):
        
        data_e_intervalo_de_dados = '{}--{}:{}'.format(data_inicial, data_final , intervalo)
            
        
        query = '{}/{}/{}/{}/{}'.format(main_link, data_e_intervalo_de_dados, variaveis, lat_long, file)
        
        return query
                