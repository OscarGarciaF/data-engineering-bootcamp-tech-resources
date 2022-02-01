from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import io
import re
import csv


class PostgresToSparkTransfer(BaseOperator):
    """PostgresToSparkTransfer: custom operator created to move a postgres table to spark back to postgres
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
            schema,
            table,
            s3_bucket,
            s3_key,
            table_types,
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',
            verify=None,
            wildcard_match=False,
            copy_options=tuple(),
            autocommit=False,
            parameters=None,            
            *args, **kwargs):
        super(PostgresToSparkTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.table_types = table_types
  
    def execute(self, context):
        
        # Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)   
        
        self.pg_hook = PostgresHook(postgre_conn_id = self.aws_conn_postgres_id)
   

        # Query and print the values of the table products in the console.
        self.request = 'SELECT * FROM ' + self.current_table + " LIMIT(5);"
        self.log.info(self.request) 
        self.connection = self.pg_hook.get_conn()
        self.cursor = self.connection.cursor()
        self.cursor.execute(self.request)
        self.sources = self.cursor.fetchall()
        self.log.info(self.sources)

        for source in self.sources:           
            self.log.info("invoice_number: {0} - stock_code: {1} - detail: {2} - quantity: {3} - invoice_date: {4} - unit_price: {5} - customer_id: {6} - country: {7} ".format(source[0],source[1],source[2],source[3],source[4],source[5], source[6], source[7]))                                                  
