import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import csv
import datetime
import boto3




class BucketS3():
    
    def __init__(self, credential):
        
        self._access_key_id = credential['access_key_id']
        self._secret_access_key = credential['secret_access_key']
        
        
    def storageUploadBucket(self, df, NOME_ARQUIVO, BUCKET_NAME, REGION):
        
        ACCESS_KEY = self._access_key_id
        SECRET_KEY = self._secret_access_key

        
        df.to_parquet(NOME_ARQUIVO)
        
        
        s3_client = boto3.client(service_name = 's3',
                                 region_name = REGION,
                                 aws_access_key_id = ACCESS_KEY,
                                 aws_secret_access_key = SECRET_KEY)
        
        s3_client.upload_file(NOME_ARQUIVO, BUCKET_NAME, NOME_ARQUIVO)
    
        print('load')