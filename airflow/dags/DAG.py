
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
import pendulum

import sys
sys.path.append('/home/ubuntu/etl_pipeline_weather_data/')
#sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data/')

from backend.app_airflow import callAPICollector

#inciando script

with DAG('pipeline_weather_call_backend_',
         start_date = pendulum.datetime(2024, 3, 17, tz = 'UTC'),
         schedule_interval = '*/5 * * * *') as dag:
    
    start_code = EmptyOperator(task_id = 'Inicializacao_Codigo', dag = dag)
    
    atribuicao_senhas = PythonOperator(python_callable = callAPICollector().senhas,
                                       task_id = 'definindo_credenciais',
                                       dag = dag)
    
    criando_info_para_request = PythonOperator(python_callable = callAPICollector().infoDataToRequest,
                                               task_id = 'info_request',
                                               dag = dag)
    
    instanciando_api = PythonOperator(python_callable = callAPICollector().callInstanceAPI,
                                      task_id = 'instanciando_APICollector',
                                      dag = dag)
    
    end_code = EmptyOperator(task_id = 'Encerrando_Codigo')
    
    start_code >> atribuicao_senhas >> criando_info_para_request >> instanciando_api >> end_code
    
