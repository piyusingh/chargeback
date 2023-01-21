
import pandas as pd 
import boto3
import datetime
import os
from aws_lambda_powertools import Logger
from http import HTTPStatus
from datetime import date, timedelta
import calendar
import io

FINAL_MERGED_PARQUET = '/tmp/final_parquet.parquet'
s3_resource = boto3.resource('s3')

lambda_name = os.environ['SERVICE']
logger = Logger(service=lambda_name)


def lambda_handler(event, context):
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
    output_key = f'{report_type}/transactions_report/ingestion_yyyymm={cycle_close_year}{cycle_close_month:02d}/transactions_report.parquet'
    
    response = transform_merged_parquet(report_type, record_year,record_month,cycle_close_month, cycle_close_year,output_key)
    logger.info(f'{lambda_name}:Successful Event')
    return response
        
def transform_merged_parquet(report_type, record_year,record_month,cycle_close_month, cycle_close_year,output_key):
    response = ''
    bucket_name = os.environ['S3_BUCKET']
    pb = s3_resource.Bucket(bucket_name)
    temp_df = pd.DataFrame()
    temp_df2 = pd.DataFrame()
    try:
        folder_key = f'{report_type}/final_csv_file/ingestion_yyyymm={cycle_close_year}{cycle_close_month:02d}/csv_file.csv'
        s3=boto3.client("s3")
        s3_object = s3.get_object(
            Bucket= bucket_name, 
            Key= folder_key
        )
        merged_csv_df = pd.read_csv(io.BytesIO(s3_object['Body'].read()), dtype ='str')
        available_fields = []
        unavailable_fields = []
        transaction_level_fields = ['baseState','secondaryProducerCode',
                            'quoteID', 'costPerMVR','quoteDate','policyNumber','issuanceDate', 'waived']
        driver_cols = ['totalOrderedDrivers', 'driversOrderedOn']
        
        transaction_level_fields = [field.lower() for field in transaction_level_fields]
        driver_cols = [col.lower() for col in driver_cols]
        
        if merged_csv_df.empty == False:   
            merged_csv_df[driver_cols] = merged_csv_df[driver_cols].apply(pd.to_numeric, errors='coerce')
            grouped_data = merged_csv_df.groupby('quoteid')
            temp_df = pd.DataFrame(merged_csv_df.groupby(['quoteid'])[f'{driver_cols[0]}'].max().reset_index())
            temp_df.rename(columns = {f'{driver_cols[0]}':f'{driver_cols[1]}'}, inplace = True)
            
                    
            logger.info(f'{lambda_name} : transform_merged_parquet, transaction_csv_file {record_year}{record_month:02d} unique quoteId count : {len(grouped_data.groups)}')
            merged_csv_df = merged_csv_df.sort_values(by='currenttimestamp').drop_duplicates(subset=['quoteid'], keep="last")
            logger.info(f'{lambda_name} : transform_merged_parquet, transaction_csv_file {record_year}{record_month:02d} size : {merged_csv_df.shape}')
            
            for item in transaction_level_fields:
                if item in merged_csv_df.columns:
                    available_fields.append(item)
                else:
                    unavailable_fields.append(item)
            
            
            final_df = merged_csv_df.loc[:,available_fields]
            
            final_df = final_df.reindex(columns = final_df.columns.tolist() + unavailable_fields)
     
            final_df.loc[final_df['policynumber'].notnull(), 'waived'] = 'Y'

            final_df['waived'].fillna('N', inplace=True)
            
            final_df = pd.merge(final_df, temp_df, how = 'left', on = ['quoteid'])

            final_df.rename(columns = {'basestate':'baseState', 
                                   'costpermvr':'costPerMVR', 'quotedate':'quoteDate',
                                   'policynumber':'policyNumber','issuancedate':'issuanceDate',
                                   'secondaryproducercode':'producerCode','driversorderedon':'driversOrderedOn',
                                   'quoteid':'quoteId','waived':'waived'}, inplace = True)            
            
                                       
            final_df.fillna('', inplace=True)
            final_df=final_df.astype(str)
            final_df.loc[final_df['costPerMVR'] == '', 'costPerMVR'] = '0'
            final_df.loc[final_df['driversOrderedOn'] == '', 'driversOrderedOn'] = '0'
            cols = ['driversOrderedOn', 'costPerMVR']
            final_df[cols] = final_df[cols].apply(pd.to_numeric, errors='coerce')

            logger.info(f'{lambda_name} : transform_merged_parquet, transaction level file {cycle_close_year}{cycle_close_month:02d} size : {final_df.shape}')

            final_df.to_parquet(FINAL_MERGED_PARQUET, index = False)
    
            s3_resource.Bucket(bucket_name).upload_file(FINAL_MERGED_PARQUET, output_key)
            response = {
                'statusCode': HTTPStatus.OK.value,
                'response': 'Uploaded chargeback_transaction_level_report successfully to S3'
            }
        else:
            logger.info(f'{lambda_name} : transform_merged_parquet, Parquet/ORC merged csv file is missing')
            response = {
                'statusCode': HTTPStatus.NOT_FOUND.value,
                'response': 'Upload failed, no data found.'
            }
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : transform_merged_parquet, Exception : {ex}')
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
    return response
