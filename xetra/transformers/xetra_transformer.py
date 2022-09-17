"""Xetra ETL COmponent"""
import logging
from typing import NamedTuple
from datetime import datetime, timedelta
import pandas as pd
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_end_price:str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_col_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    """
    Reads the Xetra data, transforms and writes to the target location
    """

    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector, meta_key: str, src_args: XetraSourceConfig, trg_args:XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self._logger=logging.getLogger(__name__)
        self.s3_bucket_src=s3_bucket_src
        self.s3_bucket_trg=s3_bucket_trg
        self.meta_key=meta_key
        self.src_arg=src_args
        self.trg_args=trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(\
            self.s3_bucket_trg, self.meta_key, self.src_arg.src_first_extract_date)
        self.meta_update_list=[]

    def extract(self):
        """
        Extraction of files based on list of prefixes. 
        Conversion into Pandas DataFrame
        """
        self._logger.info("Estracting Xetra files started...")
        files = [key for date in self.extract_date_list \
                for key in self.s3_bucket_src.list_file_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file) \
                for file in files], ignore_index=True)
        self._logger.info("Extracting is completed")
        return data_frame

    def transform_report1(self, data_frame:pd.DataFrame):
        """
        Transforming raw data into final report
        """
        #part of code to select only required columns
        if data_frame.empty:
            self._logger.info("No transformation is required as dataframe is empty")
            return data_frame
        self._logger.info("Applying transformations to rwa data started...")
        #filtering columns
        data_frame=data_frame.loc[:,self.src_arg.src_columns]
        #removing empty rows
        data_frame.dropna(inplace=True)
        #calculating opening price
        data_frame[self.trg_args.trg_col_op_price] = data_frame.sort_values(\
            by=[self.src_arg.src_col_time]).\
                groupby([self.src_arg.trg_col_isin,self.src_arg.src_col_date])\
                    [self.src_arg.src_col_start_price].transform('first')
        #calculating closing price
        data_frame[self.trg_args.trg_col_clos_price]=data_frame.sort_values(\
            by=[self.src_arg.src_col_time]).groupby(\
                [self.src_arg.trg_col_isin,
                self.src_arg.src_col_date])\
                    [self.src_arg.src_col_end_price].transform('last')
        #renaming columns
        data_frame.rename(columns={
            self.src_arg.src_col_max_price:self.trg_args.trg_col_max_price,
            self.src_arg.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_arg.src_col_traded_vol: self.src_arg.src_col_traded_vol
        })
        data_frame=data_frame.groupby([
            self.src_arg.trg_col_isin,
            self.src_arg.src_col_date], as_index=False)\
                .agg({
                    self.trg_args.trg_col_op_price: 'min',
                    self.trg_args.trg_col_clos_price: 'max',
                    self.trg_args.trg_col_min_price: 'min',
                    self.trg_args.trg_col_max_price: 'max',
                    self.trg_args.trg_col_dail_trad_vol: 'sum'})
        #percent change prev closing
        data_frame[self.trg_args.trg_col_prev_clos]=\
            data_frame.sort_values(by=[self.src_arg.src_col_date])\
                .groupby([self.src_arg.trg_col_isin])[self.trg_args.trg_col_clos_price].shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos]=(data_frame[self.trg_args.trg_col_clos_price]\
            -data_frame[self.trg_args.trg_col_prev_clos])/data_frame[self.trg_args.trg_col_prev_clos]*100
        data_frame.drop(columns=[self.trg_args.trg_col_prev_clos],inplace=True)
        data_frame=data_frame.round(decimals=2)
        #filtering
        data_frame=data_frame[data_frame.Date>=self.extract_date].reset_index(drop=True)
        self._logger.info("Trransformation completed")
        return data_frame

    def load(self, data_frame:pd.DataFrame):
        """
        Loading dataframe to S3 target bucket
        """
        if data_frame.empty:
            self._logger.info("No data to be saved...")
            return True
        self._logger.info("Loading data to S3 started..")
        key = self.trg_args.trg_key + datetime.today().strftime(
            MetaProcessFormat.META_PROCESS_DATE_FORMAT) + self.trg_args.trg_format
        self.s3_bucket_trg.write_df_to_s3(data_frame, key)
        self._logger.info("Loading of the data is completed")
        MetaProcess.update_meta_file(self.meta_update_list, self.meta_key, self.s3_bucket_trg)
        self._logger.info("Meta file updated with latest dates")
        return True

    def etl_report1(self):
        """
        ETL process 
        """
        data_frame=self.extract()
        data_frame=self.transform_report1(data_frame)
        self.load(data_frame)
        return True