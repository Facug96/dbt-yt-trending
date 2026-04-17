import boto3
import pg8000
import json
import os
from datetime import datetime


DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
S3_BUCKET = os.environ['S3_BUCKET']


VIEWS_S3_KEYS = {
    'latest':'latest/today.json',
    'in':  'latest/in.json',
    'out': 'latest/out.json',
    'both':'latest/both.json',
    'stg_youtube_snapshots':'latest/history.json'
}


conn = pg8000.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432
    )
cursor = conn.cursor()
s3 = boto3.client('s3')
ssm = boto3.client('ssm', region_name='us-east-2')

def get_param(name):
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

DB_HOST     = get_param('/yt-pipeline/DB_HOST')
DB_NAME     = get_param('/yt-pipeline/DB_NAME')
DB_USER     = get_param('/yt-pipeline/DB_USER')
DB_PASSWORD = get_param('/yt-pipeline/DB_PASSWORD')
S3_BUCKET   = get_param('/yt-pipeline/S3_BUCKET')


for view, s3_key in VIEWS_S3_KEYS.items():
    cursor.execute(f'SELECT * FROM "{view}"')
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    data = [dict(zip(columns, row)) for row in rows]
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, default=str)
    )