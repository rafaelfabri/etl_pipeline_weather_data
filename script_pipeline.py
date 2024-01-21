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


def criando_query_para_requisicao():
    
    link_site = 'https://api.meteomatics.com'
    
    #periodo de requisicao - sempre referente a ontem
    ontem = (datetime.datetime.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T00:00:00Z'
    hoje = datetime.datetime.today().strftime('%Y-%m-%d') + 'T00:00:00Z'
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
        





if __name__ == "__main__":   
    
    
    login, senha = atribuindo_senha()
        
    query = criando_query_para_requisicao()
    

    if login != ' ':
    
        df = requisicao_dados(login, senha, query)
        
        if df.shape[0] > 0:
            
            df.columns = ['DATA', 'TEMPERATURA_CELSIUS', 'PRECIP_MM', 'VELOCIDADE_VENTO_MS']
            
            df.drop(labels = [0], axis = 0, inplace = True)
            
            print(df)
        
        
    else:
        
        print('sem login ou senha')