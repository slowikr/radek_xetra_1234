"""
Methods for processing the meta file
"""
import collections
from datetime import datetime, timedelta
import pandas as pd
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exception import WrongMetaFileException

class MetaProcess():
    """
    class for working with the meta file
    """
    @staticmethod
    def update_meta_file(s3_bucket_connector: S3BucketConnector, meta_key:str, extract_date_list:list):
        """
        Updates meta file with new dates for which the data where sourced from S3 source bucket
        """
        df_new=pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL,
                                    MetaProcessFormat.META_PROCESS_COL])
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL]=extract_date_list
        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL]= \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT)
        #error handling in case no prior metafile with dates
        try:
            df_meta=s3_bucket_connector.read_csv_to_df(meta_key)
            #check if both have the same number of columns. Leveraging Counter from collections
            if collections.Counter(df_meta.columns)!=collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all=pd.concat([df_new,df_meta], ignore_index=True)
        except s3_bucket_connector.session.client('s3').exceptions.NoSuchKey:
            df_all=df_new
        #Writing to S3
        s3_bucket_connector.write_df_to_s3(df_all, meta_key,MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list(s3_bucket_connector: S3BucketConnector, meta_key:str, start_date:str):
        """
        Return list of prefixes from S3 target bucket for which the data were already sourced 
        """
        min_date=datetime.strptime(start_date, 
                                   MetaProcessFormat.META_DATE_FORMAT)\
                                   .date()-timedelta(days=1)
        today=datetime.today().date()
        try:
            #Check if meta file exists
            df_meta=s3_bucket_connector.read_csv_to_df(meta_key)
            dates=[(min_date + timedelta(days=x)) for x in range(0,(today-min_date).days+1)]
            src_date=set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL]).dt.date)
            #Creating set of all dates in meta file
            date_list=set(dates[1:])-src_date
            if date_list:
                # Determining the earliest date that should be extracted
                min_date=min(set(dates[1:])-src_date)- timedelta(days=1)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT) for date in dates if date>=min_date]
                return_min_date=(min_date+timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT)
            else:
                return_dates=[]
                return_min_date=datetime(2200,1,1).date()
        except s3_bucket_connector.session.client('s3').exceptions.NoSuchKey
            return_dates=[(min_date+timedelta(days=x))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT) \
                    for x in range(0,(today-min_date).days+1)]
            return_min_date=start_date
        return return_min_date, return_dates