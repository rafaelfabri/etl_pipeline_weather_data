# Pipeline Extract Weather Data

![PIPELINE](https://github.com/rafaelfabri/etl_pipeline_weather_data/blob/main/dags/fluxo.png)


Esse projeto tem como objetivo extrair dados, todos os dias, de previsão meteorológicas do próximo dia  referente a cidade de São Paulo.

O objetivo foi utilizar Linguagem de Programação Python para extrair esses dados via Request da API https://www.meteomatics.com/ e inseri-los em banco de dados Amazon Relational Database Service (Amazon RDS) e também salvar os arquivos .parquet em um bucket no Amazon Simple Storage Service (Amazon S3).

No final o data pipeline será hospedado na Amazon Elastic Compute Cloud (Amazon EC2) e orquestrado e monitorado em Apache Airflow.

![PIPELINE](https://github.com/rafaelfabri/etl_pipeline_weather_data/blob/main/dags/pipeline.png)


## Passo a Passo para utilizar Airflow  

```
#criar o ambiente
python3.9 -m venv env

#ativar o ambiente 
source env/bin/activate

#instalar airflow
pip install 'apache-airflow==2.7.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.9.txt"

#definir AIRFLOW_HOME 
export AIRFLOW_HOME=~/{caminho}/projeto

#airflow db migrate
airflow db migrate

#criar usuario

airflow users create \
--username admin \ 
--firstname Tony \
--lastname Stark \
--role Admin \
--email "Tony-Stark@gmail.com"

#levantar webserver
airflow webserver -p 8080

#levantar schedule (em outro terminal)
airflow scheduler
```bash
