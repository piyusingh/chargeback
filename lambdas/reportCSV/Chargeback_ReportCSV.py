import boto3
import datetime
import pandas as pd
import os
from aws_lambda_powertools import Logger
from http import HTTPStatus
import time
import calendar


PARQUET_TEMP_FILENAME = '/tmp/parquet.csv'
OUTPUT_BUCKET = os.environ['S3_BUCKET']

s3_resource = boto3.resource('s3')
athena_client = boto3.client('athena')

lambda_name = os.environ['SERVICE']
logger = Logger(service=lambda_name)

def lambda_handler(event, context):
    response = parquet_to_csv(event)
    logger.info(f'{lambda_name}:Successful Event')
    return response
    
def parquet_to_csv(event):
    response = ''
    try:
        cycle_close_year = event['cycle_close_year']
        cycle_close_month = event['cycle_close_month']
        record_year = event['record_year']
        record_month = event['record_month']
        start_date = event['start_date']
        end_date = event['end_date']
        report_type = event['report_type']
        logger.append_keys(orderYearMonth = f'{record_year}-{record_month:02d}', 
                        cycleCloseYearMonth = f'{cycle_close_year}-{cycle_close_month:02d}',
                        startDate = start_date, endDate = end_date)
        full_df = query_athena(report_type, record_year, record_month, start_date, end_date, cycle_close_year, cycle_close_month)
        
        if full_df.empty == False:
            logger.info(f'{lambda_name} : parquet_to_csv, report csv file size : {full_df.shape}')
            full_df.to_csv(PARQUET_TEMP_FILENAME, header=True, index=False)
            s3_resource.Bucket(OUTPUT_BUCKET).upload_file(PARQUET_TEMP_FILENAME, f'chargeback_reports/intermediate_files/{report_type.lower()}/final_csv_file/ingestion_yyyymm={cycle_close_year}{cycle_close_month:02d}/chargeback_data.csv')
            response = {
                'statusCode': HTTPStatus.OK.value,
                'response': 'Uploaded chargeback_monthly_report_csv successfully to S3'
            }
        else:
            response = {
                'statusCode': HTTPStatus.NOT_FOUND.value,
                'response': 'Upload failed, no data found.'
            }
        logger.info("Successful Event")
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : parquet_to_csv, Exception : {ex}')
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
    return response

def query_athena(report_type, record_year, record_month, start_date, end_date, cycle_close_year, cycle_close_month):
    data_df = pd.DataFrame()
    try:
        last_day = calendar.monthrange(record_year, record_month)[1]
        last_day_of_month = f'{record_month:02d}/{last_day}/{record_year}'
        start_day_of_month = f'{record_month:02d}/01/{record_year}'
        
        db_name = os.environ['ATHENA_DB_NAME']
        athena_client = boto3.client('athena')

        s3_output_path = f's3://{OUTPUT_BUCKET}/Unsaved/ingestion_yyyymmdd={cycle_close_year}{cycle_close_month:02d}/'

        athena_query=f"SELECT * FROM {db_name}.{report_type.lower()} where ingestion_yyyymmdd between {start_date} and {end_date} and firstorderdate between cast(date_parse('{start_day_of_month}', '%m/%d/%Y') as DATE ) and cast(date_parse('{last_day_of_month}', '%m/%d/%Y') as DATE )"

        query_id=''
        query_response = athena_client.start_query_execution(
            QueryString=athena_query,
            ResultConfiguration={
                'OutputLocation': s3_output_path
            }
        )
        query_id = query_response["QueryExecutionId"]
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = response["QueryExecution"]["Status"]["State"]
        
        while state == "QUEUED" or state == "RUNNING":
            response = athena_client.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                output_location=response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
                data_df = pd.read_csv(output_location, dtype = {'pk':'str','producercode':'str','quoteid':'str','policynumber':'str'})
                date_cols = ['quotedate','policyissuedate','orderdate','currenttimestamp','closedate','firstorderdate']
                date_cols.append('startdate')
                data_df[date_cols] = data_df[date_cols].apply(pd.to_datetime)
                logger.info(f'{lambda_name} : query_athena, athena query executed successfully, QueryExecutionId : {query_id}, data_df size: {data_df.shape}')
            elif state == "FAILED":
                logger.info(f'{lambda_name} : query_athena, athena query execution failed, QueryExecutionId : {query_id}')
            time.sleep(2)
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : query_athena', extra={'error' : ex})

    return data_df