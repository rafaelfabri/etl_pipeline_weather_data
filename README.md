# Pipeline Extract Weather Data

![hospedagem_cloud](https://github.com/rafaelfabri/etl_pipeline_weather_data/blob/main/imagens/hospedagem_cloud.png)

Esse projeto tem como objetivo a construção de um pipeline de dados para todos os dias extrair dados da previsão meteorológicas do próximo dia referente a cidade de São Paulo.

O objetivo foi utilizar Linguagem de Programação Python para extrair esses dados via Request da API https://www.meteomatics.com/, realizar uma básica transformação de dados e armazena-los. 

A ideia é utilizar a metodologia ETL para a realização deste projeto. Primeiramente, estou construindo localmente e ao final será hospedado na Amazon Elastic Compute Cloud (Amazon EC2), orquestrado e monitorado em Apache Airflow. Foi um grande desafio estar aprendendo como trabalhar com Airflow e os servicos Cloud da Amazon.

Na imagem abaixo está a forma macro das etapas que serão executadas em Python com a extração da fonte de dados, uma básica transformação de dados e por fim o carregamento deste dados.

![PIPELINE](https://github.com/rafaelfabri/etl_pipeline_weather_data/blob/main/imagens/pipeline.png)

O pipeline completo encontra-se na imagem abaixo:

![PIPELINE_COMPLETO](https://github.com/rafaelfabri/etl_pipeline_weather_data/blob/main/imagens/pipeline_completo.png)

O objetivo é deixar esse pipeline rodando todos os dias e para isso será hospedado em uma Instância EC2, poderia ser utilizada uma outra ferramente da AWS como o lambda, mas como o objetivo era praticar um pouco de Airflow escolhi armazenar em uma máquina virtual. O script em Python executará diariamente atravez de uma ativação pelo Airflow e executar um ETL para poder armazena-los em banco de dados como Amazon Relational Database Service (Amazon RDS) e também salvar os arquivos .parquet em um bucket no Amazon Simple Storage Service (Amazon S3).


obs - no codigo deixei a parte com insert na Amazon RDS, porém como é mais caro manter um banco de dados relacional na AWS eu resolvi desliga-lo e deixar o codigo la como exemplo. 


## Configurações Python 

```
#criar o ambiente
python3.9 -m venv env

```bash

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
