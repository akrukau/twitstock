import os

import boto

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')


bucket_name = "your-bucket-name"
folder_name = "your-folder-name/"
file_name = "your-file-name"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

bucket = conn.get_bucket(bucket_name)
key = bucket.get_key(folder_name + file_name)

data = key.get_contents_as_string()

print data
