import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import sys
sys.path.append('/home/rafaelfabrichimidt/Documentos/Projetos/Python/codigos/pipeline_api_weather')
from backend.modulos import api





def main():
    
    instance_api = api.APICollector()

    print(instance_api)

if __name__ == "__main__":
    
    main()

