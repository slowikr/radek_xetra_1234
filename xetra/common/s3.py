"""Connector and methods accessing S3"""
import logging
import os
import boto3
class S3BucketConnector():
    """
    CLass for interacting with S3 Buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url:str, bucket: str):
        """
        Constructor for S3BucketConnector
        :param access_key: access key for connecting with S3
        :param secret_key: secret key for connecting with S3
        :param endpoint_url: endpoint url to s3
        :param bucket: S3 Bucket
        """
        self._logger=logging.getLogger(__name__)
        self.endpoint_url=endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3=self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket=self._s3.Bucket(bucket)

    def list_in_prefix(self, prefix: str):
        """
        list of all files with specified prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self):
        """
        Some Text
        """
        return self

    def write_df_to_s3(self):
        """
        Some text
        """
        return self
