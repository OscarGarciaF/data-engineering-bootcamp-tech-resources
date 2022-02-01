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
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',          
            *args, **kwargs):
        super(PostgresToSparkTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
  
    def execute(self, context):
        
        # Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)        
        self.pg_hook = PostgresHook(postgre_conn_id = self.aws_conn_postgres_id)
        self.current_table = self.schema + "." + self.table
   

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
