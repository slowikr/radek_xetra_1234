"""
Test S3BucketCOnnector Methods
"""
import os
import unittest

import pandas as pd
import boto3
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
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

    def test_list_files_in_prefix_ok(self):
        """
        Test the list_files_in_prefix method for getting 2 file keys as list on mocked S3 bucket
        """
        # Expected results
        prefix_exp='prefix/'
        key1_exp=f'{prefix_exp}test1.csv'
        key2_exp=f'{prefix_exp}test2.csv'
        # Test init
        csv_content="""col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result=self.s3_bucket_conn.list_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp,list_result)
        # Cleaunp after tests 
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wronf_prefix(self):
        """
        Test the list_files_in_prefix method when passing nonexisting prefix
        """
        # Expected results
        prefix_exp='no-prefix/'
        # Method execution
        list_result=self.s3_bucket_conn.list_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)

    def test_write_df_to_s3_empty(self):
        """
        Tests the write_df_to_s3 method with
        an empty DataFrame as input
        """
        # Expected results
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be written!'
        # Test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'
        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertEqual(return_exp, result)

    def test_save_to_s3_csv(self):
        """
        Test if CSV files would be saved
        """
        #create dummy Dataframe
        data_frame=pd.DataFrame({'ISIN': ['11111','22222'],
                                'MaxPrice': [22,33]
                                })
        #attempt to save to S3 mock
        success=self.s3_bucket_conn.write_df_to_s3(data_frame,"testfile.csv","csv")
        self.assertTrue(success)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': 'testfile.csv'
                    }
                ]
            }
        )
if __name__=="__main__":
    unittest.main()
