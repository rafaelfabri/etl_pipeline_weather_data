import requests
import pandas as pd
from requests.auth import HTTPBasicAuth




def atribuindo_senha():
    
    try:
    
        with open('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather/senhas/senhas.csv') as f:
            
            df = pd.read_csv(f)
            
            
            login = df.loc[0, 'login']
            senha = df.loc[0, 'senha']
            
            print(login)
            print(senha)
            
            return login, senha

            
    except:
        problema = 'Nao encontrado diretorio senhas ou arquivo senha ou arquivo com formato errado'
        print(problema)
        print('exemplo de arquivo esta no arquivo exemplo_senha.csv')
        
        return ' ', ' '

    
    


def requisicao_dados(login, senha):

    query = 'https://api.meteomatics.com/2024-01-18T00:00:00Z--2024-01-21T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804/html'
    
    
    r = requests.get(query, auth = HTTPBasicAuth(login, senha))

    print(r.status_code)



if __name__ == "__main__":
    
    
    login, senha = atribuindo_senha()
        
    print(login)
    
    if login != ' ':
    
        requisicao_dados(login, senha)
        
    else:
        print('sem senha')