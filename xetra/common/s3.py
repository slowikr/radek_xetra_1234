"""Connector and methods accessing S3"""
import logging
import os
from io import StringIO, BytesIO
import boto3
import pandas as pd
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exception import WrongFormatException

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

    def read_csv_to_df(self, key:str, decod='utf-8',dlm=','):
        """
        reading a csv file from the S3 bucket and returning a dataframe

        :param key: key of the file that should be read
        :decod: encoding of the data inside the csv file
        :dlm: seperator of the csv file

        returns:
          data_frame: Pandas DataFrame containing the data of the csv file
        """
        self._logger.info("Reading file %s%s%s",self.endpoint_url, self._bucket.name, key)
        csv_obj=self._bucket.Object(key=key).get()['Body'].read().decode(decod)
        data=StringIO(csv_obj)
        return pd.read_csv(data, delimiter=dlm)

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format:str):
        """
        Converting of DataFrame into Parquet file 
        and saving in S3 bucket (supporting csv or parquet format)
        """
        if data_frame.empty:
            self._logger.info("The dataframe is empyt, nothing to save")
            return None
        if file_format==S3FileTypes.CSV.value:
            out_buffer=StringIO()
            data_frame.to_csv(out_buffer,index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
            return True
        if file_format==S3FileTypes.PARQUET.value:
            out_buffer=BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
            return True
        self._logger.info("The file format %s is not supported", file_format)
        raise WrongFormatException
