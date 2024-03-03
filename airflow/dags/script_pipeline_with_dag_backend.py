
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
import pendulum

import sys
sys.path.append('/home/rafaelfabrichimidt/Documentos/projetos/python/etl_pipeline_weather_data')
from backend.app_airflow import callAPI



with DAG('pipeline_weather_call_backend',
         start_date = pendulum.datetime(2023, 3, 1, tz = 'UTC'),
         schedule_interval = '0 1 * * *') as dag:
    
    start_code = EmptyOperator(task_id = 'Inicializacao_Codigo', dag = dag)
    
    atribuicao_senhas = PythonOperator(python_callable = callAPI().senhas,
                                       task_id = 'definindo_credenciais',
                                       dag = dag)
    
    instanciando_api = PythonOperator(python_callable = callAPI().callInstanceAPI,
                                      task_id = 'instanciando_APICollector',
                                      dag = dag)

    comecando_etl = PythonOperator(python_callable = callAPI().callStartETL,
                                   task_id = 'executando_ETL',
                                   dag = dag)
    
    end_code = EmptyOperator(task_id = 'Encerrando_Codigo')
    
    start_code >> atribuicao_senhas >> instanciando_api >> comecando_etl >> end_code
    