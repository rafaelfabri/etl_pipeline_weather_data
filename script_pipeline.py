import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import csv
import datetime 



def atribuindo_senha():
    
    try:
        
        with open('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather/senhas/senhas.csv', 'r') as f:
            
            df = pd.read_csv(f)
            
            login = df.loc[0, 'login']
            senha = df.loc[0, 'senha']
            
            return login, senha

            
    except:
        problema = 'Nao encontrado diretorio senhas ou arquivo senha ou arquivo com formato errado'
        print(problema)
        print('exemplo de arquivo esta no arquivo exemplo_senha.csv')
        
        return ' ', ' '

    
    

def requisicao_dados(login, senha):

    query = 'https://api.meteomatics.com/2024-01-20T00:00:00Z--2024-01-23T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804/csv'
    
    
    try:
        
        r = requests.get(query, auth = HTTPBasicAuth(login, senha))
    
        if r.status_code == 200:
            
            print('Success in request!')
            
            data_csv = r.content.decode('utf-8')
            
            data_csv = csv.reader(data_csv.splitlines(), delimiter = ';')
            
            list_of_data = []
            
            for row in data_csv:
                
                list_of_data.append(row)
            
            df = pd.DataFrame(list_of_data)
            
            return df
            
        
        else:
            
            print('Resquest denied')
            
            df = pd.DataFrame()
            
            return df

            
    except:
        
        print('error - link error')
        
        df = pd.DataFrame()
            
        return df
        





if __name__ == "__main__":
    
    
    login, senha = atribuindo_senha()
        

    
    if login != ' ':
    
        df = requisicao_dados(login, senha)
        
        if df.shape[0] > 0:
            
            print(df)
        
        
        
    else:
        
        print('sem senha')