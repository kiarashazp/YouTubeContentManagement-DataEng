import boto3


def connected_to_s3():
    endpoint_url = 'https://s3.ir-thr-at1.arvanstorage.ir'
    access_key = 'ab5fc903-7426-4a49-ae3e-024b53c30d27'
    secret_key = 'f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2'
    bucket_name = 'qbc'
    s3_resource = boto3.client(
        's3', endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key,
    )
    response = s3_resource.list_objects_v2(Bucket=bucket_name)

    return s3_resource, bucket_name, response
