import os

import boto3
from dotenv import load_dotenv

load_dotenv()
# Singleton instance
dynamodb_resource = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    aws_access_key_id=os.getenv("AWS_DYNAMODB_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_DYNAMODB_SECRET_ACCESS_KEY"),
)

dynamodb_client = boto3.client(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    aws_access_key_id=os.getenv("AWS_DYNAMODB_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_DYNAMODB_SECRET_ACCESS_KEY"),
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_S3_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_S3_REGION', 'ap-south-1')
)

def get_dynamodb_resource():
    return dynamodb_resource


def get_dynamodb_client():
    return dynamodb_client

def get_s3_client():
    return s3_client