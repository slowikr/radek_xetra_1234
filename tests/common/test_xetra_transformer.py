"""
Testing meta_process methods
"""
import os
import unittest
from unittest.mock import patch
import boto3
from moto import mock_s3
import pandas as pd
from io import BytesIO
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat, S3FileTypes
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig

class TestXetraTransformer(unittest.TestCase):
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
        self.s3_bucket_name_src = "src-bucket"
        self.s3_bucket_name_trg = "trg-bucket"
        self.meta_key="meta_key.csv"
        #creating s3 access keys as environment variables
        os.environ[self.s3_access_key]="KEY1"
        os.environ[self.s3_secret_key]="KEY2"
        #creating a bucket on the mock up s3
        self.s3 = boto3.resource(service_name='s3',endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'})
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'})
        self.src_bucket=self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket=self.s3.Bucket(self.s3_bucket_name_trg)
        #Creating a testing instance
        self.src_bucket_conn=S3BucketConnector(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name_src)
        self.trg_bucket_conn=S3BucketConnector(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name_trg)
        #creating source configuration
        config_dict_src={
            'src_first_extract_date': '2022-09-01',
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_end_price': 'EndPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        #creating target configuration
        config_dict_trg={
            'trg_col_isin': 'ISIN',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_col_prev_clos': 'prev_closing',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': MetaProcessFormat.META_PROCESS_DATE_FORMAT.value,
            'trg_format': S3FileTypes.PARQUET.value
        }
        self.source_config=XetraSourceConfig(**config_dict_src)
        self.target_config=XetraTargetConfig(**config_dict_trg)
        data = [['AT111111', 'SANT', '2022-09-02', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
                ['AT222222', 'SANT', '2022-09-02', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                ['AT111111', 'SANT', '2022-09-03', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                ['AT111111', 'SANT', '2022-09-03', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                ['AT111111', 'SANT', '2022-09-04', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
                ['AT222222', 'SANT', '2022-09-04', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
                ['AT222222', 'SANT', '2022-09-05', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
                ['AT222222', 'SANT', '2022-09-05', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
                ['AT222222', 'SANT', '2022-09-05', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]]
        self.data_frame_src=pd.DataFrame(data, columns=self.source_config.src_columns)
        self.src_bucket_conn.write_df_to_s3(self.data_frame_src.loc[0:1],
        '2022-09-02/2022-09-02_BINS_XETR12.csv','csv')
        self.src_bucket_conn.write_df_to_s3(self.data_frame_src.loc[2:3],
        '2022-09-03/2022-09-03_BINS_XETR15.csv','csv')
        self.src_bucket_conn.write_df_to_s3(self.data_frame_src.loc[4:5],
        '2022-09-04/2022-09-04_BINS_XETR13.csv','csv')
        self.src_bucket_conn.write_df_to_s3(self.data_frame_src.loc[6:8],
        '2022-09-05/2022-09-05_BINS_XETR14.csv','csv')
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%']
        data_report = [['AT111111',	'2022-09-03',	20.21,	21.19,	18.21,	21.34,	1088,	14.85],
	                   ['AT111111',	'2022-09-04',	20.58,	19.27,	18.89,	20.58,	9066,	-9.06],
	                   ['AT222222',	'2022-09-04',	19.27,	21.14,	19.27,	21.14,	1220,	-0.24],
	                   ['AT222222',	'2022-09-05',	23.58,	22.21,	22.21,	25.01,	3586,	5.06]]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        """
        Executing after unittests
        """
        #mocking s3 connection stop
        self.mock_s3.stop()

    def test_extract_no_files(self):
        """
        Tests the extract method when
        there are no files to be extracted
        """
        extract_date = '2200-01-02'
        extract_date_list = []
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.src_bucket_conn, self.trg_bucket_conn,
                         self.meta_key, self.source_config, self.target_config)
            df_return = xetra_etl.extract()
        # Test after method executions
        self.assertTrue(df_return.empty)

    def test_extract_files(self):
        """
        Tests the extract method when
        there are files to be extracted
        """
        # Expected results
        df_exp = self.data_frame_src.loc[2:5].reset_index(drop=True)
        # Test init
        extract_date = '2022-09-03'
        extract_date_list = ['2022-09-02', '2022-09-03', '2022-09-04']
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.src_bucket_conn, self.trg_bucket_conn,
                         self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.extract()
        # Test after method execution
        print(df_result)
        self.assertTrue(df_exp.equals(df_result))

    def test_transform_report1_emptydf(self):
        """
        Tests the transform_report1 method with
        an empty DataFrame as input argument
        """
        # Expected results
        log_exp = 'No transformation is required as dataframe is empty'
        # Test init
        extract_date = '2022-09-03'
        extract_date_list = ['2022-09-02', '2022-09-03', '2022-09-04']
        df_input = pd.DataFrame()
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.src_bucket_conn, self.trg_bucket_conn,
                         self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertTrue(df_result.empty)
 
    def test_transform_report1_ok(self):
        """
        Tests the transform_report1 method with
        an DataFrame as input argument
        """
        # Expected results
        log1_exp = 'Applying transformations to rwa data started...'
        log2_exp = 'Transformation completed'
        df_exp = self.df_report
        # Test init
        extract_date = '2022-09-03'
        extract_date_list = ['2022-09-02', '2022-09-03', '2022-09-04','2022-09-05']
        df_input = self.data_frame_src.loc[0:8].reset_index(drop=True)
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.src_bucket_conn, self.trg_bucket_conn,
                         self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, logm.output[0])
                self.assertIn(log2_exp, logm.output[1])
        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

    def test_load(self):
        """
        Tests the load method
        """
        # Expected results
        log1_exp = 'Loading of the data is completed'
        log2_exp = 'Meta file updated with latest dates'
        df_exp = self.df_report
        meta_exp = ['2022-09-02']
        # Test init
        extract_date = '2022-09-03'
        extract_date_list = ['2022-09-02','2022-09-03', '2022-09-04', '2022-09-05']
        df_input = self.df_report
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.src_bucket_conn, self.trg_bucket_conn,
                         self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                xetra_etl.load(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, logm.output[1])
                self.assertIn(log2_exp, logm.output[4])
        # Test after method execution
        trg_file = self.trg_bucket_conn.list_files_in_prefix(self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.trg_bucket_conn.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.trg_bucket_conn.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )

if __name__=="__main__":
    unittest.main()
