from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import io
import re
import csv


class s3ToGlue(BaseOperator):
    """PostgresToSparkTransfer: custom operator created to move a s3 to glue and back to s3
       Author: Oscar Garcia.      
       Creation Date: 01/02/2022.                   

    Attributes:
    """

    template_fields = ()

    template_ext = ()

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_key,
            wildcard_match=False,
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',          
            *args, **kwargs):
        super(s3ToGlue, self).__init__(*args, **kwargs)
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.wildcard_match = wildcard_match
  
    def execute(self, context):
        
        
        self.log.info(self.aws_conn_postgres_id)        
        self.s3 = S3Hook(aws_conn_id = self.aws_conn_id, verify = self.verify)

        self.log.info("Downloading S3 file")
        self.log.info(self.s3_key + ', ' + self.s3_bucket)

        # Validate if the file source exist or not in the bucket.
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key))
                  
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)
        list_srt_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
   
                                                 
