import boto3
import os

def delete_file(file_name):
    """Delete file in own server"""
    if os.path.exists(file_name):
        os.remove(file_name)
        return "removed"
    else:
        return "file does not exist"

def download_files(access_key = "ACCESS_KEY", secret_access_key="SECRET_ACESS_KEY", bucket="BUCKET", folder="folder_name/"):
    """Download files of old S3 storage, delete file and upload them to another S3"""
    s3 = boto3.client(
        's3','us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )
    paginator = s3.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket,
                        'Prefix': folder}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for key in page['Contents']:
            if key['Key'].endswith('/'):
                if not os.path.exists('./'+key['Key']):
                    os.makedirs('./'+key['Key'])
            else:
                print('key.name',key['Key'])
                s3.download_file(bucket, key['Key'], key['Key'])
                upload_file(key['Key'])
                delete_file(key['Key'])

def upload_file(file_name, access_key = "ACCESS_KEY", secret_access_key="SECRET_ACESS_KEY", bucket="BUCKET"):
    """Upload file to S3 storage"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
    )
    response = s3_client.upload_file(file_name, bucket, file_name, ExtraArgs={'ACL':'public-read'}) #public-read, anyone can see the file

if __name__ == "__main__":
    download_files()