import requests 
from requests.auth import HTTPBasicAuth
import pandas as pd
import csv
import datetime
import boto3




class BucketS3():
    
    def __init__(self, credential, BUCKET_NAME, REGION):
        
        self._access_key_id = credential['access_key_id']
        self._secret_access_key = credential['secret_access_key']
        self._BUCKET_NAME = BUCKET_NAME
        self._REGION = REGION
        
    def storageUploadBucket(self, df, NOME_ARQUIVO):
        
        _ACCESS_KEY = self._access_key_id
        _SECRET_KEY = self._secret_access_key

        
        df.to_parquet(NOME_ARQUIVO)
        
        
        s3_client = boto3.client(service_name = 's3',
                                 region_name = self._REGION,
                                 aws_access_key_id = _ACCESS_KEY,
                                 aws_secret_access_key = _SECRET_KEY)
        
        s3_client.upload_file(NOME_ARQUIVO, self._BUCKET_NAME, NOME_ARQUIVO)
    
        print('load')