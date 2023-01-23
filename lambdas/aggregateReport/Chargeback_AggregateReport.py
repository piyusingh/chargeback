import boto3
import datetime
import pandas as pd
import numpy as np
import fastparquet as fp
import s3fs
import os
from aws_lambda_powertools import Logger
from http import HTTPStatus

AGGREGATED_TEMP_FILENAME = '/tmp/aggregated.parquet'
s3_resource = boto3.resource('s3')
bucket_name = os.environ['S3_BUCKET']

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
    
    response = concatenate(report_type,start_date,end_date,record_year,record_month, cycle_close_year, cycle_close_month)
    logger.info(f'{lambda_name}:Successful Event')
    return response
    
def concatenate(report_type,start_date,end_date,record_year,record_month,cycle_close_year, cycle_close_month):
    response = ''
    try:
        transaction_df = convert_to_df(f'{bucket_name}/{report_type}/transactions_report/ingestion_yyyymm={cycle_close_year}{cycle_close_month:02d}')
        transaction_level_fields = ['baseState','producerCode','orderedDriversForCurrReq','QuoteId','costPerReport','quoteDate','policyNumber','policyIssueDate','waived']

        if transaction_df.empty == False:
              
            final_aggregate_df = calculate_total_amount(transaction_df,record_year,record_month)

            if final_aggregate_df.empty == False:

                final_aggregate_df.to_parquet(AGGREGATED_TEMP_FILENAME, index=False)
                
                logger.info(f'{lambda_name} : concatenate, aggregated report {record_year}{record_month:02d} size : {final_aggregate_df.shape}')
                
                s3_resource.Bucket(bucket_name).upload_file(AGGREGATED_TEMP_FILENAME, f'{report_type}/aggregate_report/ingestion_yyyymm={cycle_close_year}{cycle_close_month:02d}/aggregate_report.parquet')
                response = {
                        'statusCode': HTTPStatus.OK.value,
                        'response': 'Uploaded chargeback_aggregate_report successfully to S3'
                }
            else:
                logger.info(f'{lambda_name} : aggregate parquet file is missing.')
                response = {
                "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
                "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
                }
        else:
            logger.info(f'{lambda_name} : aggregate parquet file is missing')
            response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : concatenate', extra={'error' : ex})
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
    return response

def convert_to_df(root_dir_path):
    try:
        df=pd.DataFrame()
        
        s3 = s3fs.S3FileSystem()
        fs = s3fs.core.S3FileSystem()
        fs.clear_instance_cache()
        s3.clear_instance_cache()

        s3_path = f'{root_dir_path}/*.parquet'
        all_paths_from_s3 = fs.glob(path=s3_path)
        myopen = s3.open
        if all_paths_from_s3:
            fp_obj = fp.ParquetFile(all_paths_from_s3,open_with=myopen, root=root_dir_path)
            df = fp_obj.to_pandas()
        return df
    except IndexError as ie:
        logger.info(f'{lambda_name} : convert_to_df, s3_path : {s3_path} does not exist')
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : convert_to_df', extra={'error' : ex})

        
def calculate_total_amount(transaction_df,record_year,record_month):
    final_aggregate_df=pd.DataFrame()
    aggregate_report_fields = ['producerCode','bindRatio','totalAmount']
    try:
        amount_criteria = int(os.environ['AMOUNT_CRITERIA'])
        producer_code_groups = transaction_df.groupby('producerCode')
        producer_code_count = len(producer_code_groups.groups)
        logger.info(f'{lambda_name} : calculate_total_amount, transaction level file {record_year}{record_month:02d} size : {transaction_df.shape}, producerCode count : {producer_code_count}')
        temp_df = pd.DataFrame()
        for name,group in producer_code_groups:
            producer_code = name
            order_counter = group.shape[0]
            bind_counter = (group['policyNumber'] != '').sum()
            bind_ratio = str(round((bind_counter / order_counter) * 100))+'%'
            new_row = {'producerCode':producer_code, 'bindCounter':bind_counter, 'orderCounter':order_counter, 'bindRatio':bind_ratio}
            temp_df = temp_df.append(new_row, ignore_index=True)
           
        bind_ratio_merge = pd.merge(transaction_df, temp_df[['producerCode','bindCounter','orderCounter','bindRatio']], how = 'left', on = ['producerCode'])
        bind_ratio_merge["bindRatio"] = pd.to_numeric(bind_ratio_merge["bindRatio"].replace(regex=['%'], value=''), errors='coerce')
        bind_ratio_merge[['orderedDriversForCurrReq','costPerReport']] = bind_ratio_merge[['orderedDriversForCurrReq','costPerReport']].apply(pd.to_numeric, errors='coerce')
        bind_ratio_merge.loc[(bind_ratio_merge['bindRatio'].notnull()) & (bind_ratio_merge['bindRatio'] > amount_criteria-1), 'totalAmount'] = 0
        bind_ratio_merge.loc[(bind_ratio_merge['bindRatio'].notnull()) & (bind_ratio_merge['bindRatio'] < amount_criteria) & (bind_ratio_merge['waived'] == 'N') , 'totalAmount'] = bind_ratio_merge['orderedDriversForCurrReq'] * bind_ratio_merge['costPerReport']
        bind_ratio_merge.loc[(bind_ratio_merge['bindRatio'].notnull()) & (bind_ratio_merge['bindRatio'] < amount_criteria) & (bind_ratio_merge['waived'] == 'Y'), 'totalAmount'] = 0
        
        total_amount_df = bind_ratio_merge.groupby(['producerCode'])['totalAmount'].sum().reset_index()

        final_aggregate_df = pd.merge(temp_df, total_amount_df[['producerCode','totalAmount']], how = 'left', on = ['producerCode'])
        final_aggregate_df = final_aggregate_df.loc[:,aggregate_report_fields]
        final_aggregate_df['totalAmount'] = final_aggregate_df['totalAmount'].round(decimals = 2)
        final_aggregate_df.fillna({'totalAmount':0}, inplace=True)
        return final_aggregate_df
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : calculate_total_amount', extra={'error' : ex})
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        return response