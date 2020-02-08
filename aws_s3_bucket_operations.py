"""This file automate the S3 bucket operations of the aws
1. create a bucket of s3
2. list the available buckets
3. upload file to s3 bucket
4. delete file from the bucket
5. delete the whole bucket.
6. copy bucket from one to another
7. download the file from bucket.
"""

import boto3
import botocore
import os
import datetime
from botocore.exceptions import ClientError

access_key = ""
secret_key = ""

s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3r = boto3.resource('s3')


def create_bucket(bucket_name, region="us-east-1"):
    """Create an S3 bucket in a specified region
    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    """
    # Create bucket
    try:
        s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_key)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
    except ClientError as e:
        print(e)
        return False
    print(bucket_name, " is created.")
    return True


def list_buckets():
    """it will list all the buckets available"""
    response = s3.list_buckets()
    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(bucket["Name"])


def upload_to_s3(file_from_machine, bucket, file_to_s3):
    """Upload file to bucket"""
    s3.upload_file(file_from_machine, bucket, file_to_s3)
    print(file_to_s3, " : is upoaded to s3")


def delete_file(bucket, file_to_be_deleted):
    """Delete specified file from the bucket"""
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.delete_object(Bucket=bucket, Key=file_to_be_deleted)
    print(file_to_be_deleted, " : is deleted from the bucket")


def delete_whole_bucket(bucket):
    """Delete the whole bucket"""
    bucket = s3r.Bucket(bucket)
    for key in bucket.objects.all():
        key.delete()
    bucket.delete()
    print(bucket, " : is deletd ")


def copy_bucket(source_bucket, destination_bucket):
    """Copy objects from one bucket to another"""
    src = s3r.Bucket(source_bucket)
    dst = s3r.Bucket(destination_bucket)
    for k in src.objects.all():
        copy_source = {'Bucket': source_bucket, 'Key': k.key}
        dst.copy(copy_source, k.key)
        print(k.key,' : is copied')


def download_file(bucket,file_name):
    """Download file form the bucket"""
    with open(file_name, 'wb') as f:
        s3.download_fileobj(bucket, file_name,f)
        print(file_name, ": is downloaded")
# a. Create an S3 bucket name .
# create_bucket("bucket-name",region="eu-west-1")

# b. Puts objects in a previously created bucket.
# upload_to_s3("test.txt","bucket-name","test.txt")

# list_buckets()

# c. Deletes an object in a bucket.
# delete_file("bucket-name","test.txt")

# d. Deletes a bucket.
# delete_whole_bucket("bucket-name")

# f. Downloads an existing object from a bucket.
download_file("","test.txt")

# e. Copies and objects from one bucket to another.
copy_bucket("", "")
print(datetime.datetime.now())

