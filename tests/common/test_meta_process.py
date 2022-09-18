"""
Testing meta_process methods
"""
import os
import unittest
from datetime import datetime, timedelta
import boto3
from moto import mock_s3
import pandas as pd
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat

class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """

    def setUp(self):
        """
        Setting up env
        """
        # mocking s3 connection start
        self.mock_s3=mock_s3()
        self.mock_s3.start()
        #definiting the class arguments
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        #creating s3 access keys as environment variables
        os.environ[self.s3_access_key]="KEY1"
        os.environ[self.s3_secret_key]="KEY2"
        #creating a bucket on the mock up s3
        self.s3 = boto3.resource(service_name='s3',endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'})
        self.s3_bucket=self.s3.Bucket(self.s3_bucket_name)
        #Creating a testing instance
        self.s3_bucket_conn=S3BucketConnector(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name)

    def tearDown(self):
        """
        Executing after unittests
        """
        #mocking s3 connection stop
        self.mock_s3.stop()

    def test_return_date_list_meta_file_not_exist(self):
        """
        Checking what would be returned
        if asked for the file that doesn't exists
        """
        meta_key="meta_file222.csv"
        start_date="2022-09-01"
        min_date=datetime.strptime(start_date, MetaProcessFormat.META_DATE_FORMAT.value)\
                                   .date()-timedelta(days=1)
        today=datetime.today().date()
        return_min_date_exp="2022-09-01"
        return_min_date_list_exp=[(min_date+timedelta(days=x))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                    for x in range(0,(today-min_date).days+1)]
        return_min_date, return_dates = MetaProcess.return_date_list(self.s3_bucket_conn,meta_key,start_date)
        self.assertEqual(set(return_dates), set(return_min_date_list_exp))
        self.assertEqual(return_min_date_exp,return_min_date)

    def test_return_date_list_meta_file_exist(self):
        """
        Checking if for existing file to process will return expected dates
        """
        #creating test meta_file
        meta_key='meta_file.csv'
        data_frame=pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                        MetaProcessFormat.META_PROCESS_COL.value])
        data_frame[MetaProcessFormat.META_SOURCE_DATE_COL.value]=['2022-09-01','2022-09-02','2022-09-03']
        data_frame[MetaProcessFormat.META_PROCESS_COL.value]=datetime.today().\
            strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        self.s3_bucket_conn.write_df_to_s3(data_frame,meta_key,'csv')
        #expected results
        start_date='2022-09-01'
        today=datetime.today().date()
        exp_return_date='2022-09-04'
        min_date=datetime.strptime('2022-09-03',
                                   MetaProcessFormat.META_DATE_FORMAT.value)\
                                   .date()
        return_min_date_list_exp=[(min_date+timedelta(days=x))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                    for x in range(0,(today-min_date).days+1) if (min_date+timedelta(days=x))>=min_date]
        return_min_date, return_dates = MetaProcess.return_date_list(self.s3_bucket_conn,
                                                                    meta_key,start_date)
        #checking if results meeting expected results
        self.assertEqual(set(return_dates), set(return_min_date_list_exp))
        self.assertEqual(exp_return_date,return_min_date)

if __name__=="__main__":
    unittest.main()
