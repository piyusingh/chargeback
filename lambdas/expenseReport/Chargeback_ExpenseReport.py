import boto3
import pathlib
import logging
import io
import os
from datetime import datetime,date
from http import HTTPStatus
import pandas as pd
import xlsxwriter
import time
import numpy as np
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
todays_date = date.today()
current_time = datetime.now()
lambda_name = os.environ['SERVICE']
s3client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def lambda_handler(event, context):
    bucket_name = os.environ['S3_BUCKET']
    start_time = time.perf_counter()
    logger.info(f"lambda: {lambda_name}, lambda_handler() {event} ")
    s3_object_list = []
    process_list = []
    try: 
        year = event["cycle_close_year"] 
        month = event["cycle_close_month"]
        rec_year = event["record_year"]
        rec_month = event["record_month"]
        start_date = event["start_date"]
        end_date = event["end_date"]
        report_type = event['report_type']
        my_bucket = s3_resource.Bucket(bucket_name)
        file_ext = f"{event['record_year']}{event['record_month']:02d}_{event['start_date']}-{event['end_date']}"
        [s3_object_list.append(object_summary.key) for object_summary in my_bucket.objects.filter(Prefix=f"{report_type}/transactions_report/ingestion_yyyymm={year}{month:02d}")]
        [s3_object_list.append(object_summary.key) for object_summary in my_bucket.objects.filter(Prefix=f"{report_type}/aggregate_report/ingestion_yyyymm={year}{month:02d}")]
        logger.info(f"lambda: {lambda_name}, lambda_handler(), Files are: {s3_object_list} in BucketName: {bucket_name}")
        for keys in s3_object_list:
            process_list.append(keys)
        logger.info(f"lambda: {lambda_name}, lambda_handler(), Keys are: {process_list} in BucketName: {bucket_name}")
        if len(process_list) == 2:
                s3_key_1 = process_list[0]
                s3_key_2 = process_list[1]
                logger.info(f"lambda: {lambda_name}, lambda_handler(), BucketName: {bucket_name}, FileNames: {s3_key_1} and {s3_key_2}")
                if pathlib.Path(s3_key_1).suffix == '.parquet'and pathlib.Path(s3_key_2).suffix == '.parquet':
                    response = create_merged_file(report_type, s3client,s3_resource,bucket_name,s3_key_1,s3_key_2,year,month,rec_year,rec_month, start_date, end_date)
                    end_time = time.perf_counter()
                    logger.info(f"lambda: {lambda_name}, lambda_handler() finished  with Response {response} with time {end_time-start_time} ")
                    return {
                        "statusCode": 200,
                        "headers": {
                            "Content-Type": "application/json"
                        },
                        "body": "Success"
                    }
        else:
            response = {
                "statusCode": HTTPStatus.NOT_FOUND.value,
                "response": "The required files are not available in the folder"
                }
            logger.exception(f'{lambda_name} : {response} for keys {s3_object_list}')
            return response
    except Exception as ex:
        end_time = time.perf_counter()
        logger.exception(f"lambda: {lambda_name}, lambda_handler() exception {ex} with time {end_time-start_time} ")
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        return response
 
def create_merged_file(report_type,s3,s3_resource,bucket_name,keyname1,keyname2,year,month,rec_year,rec_month, start_date, end_date):
    response_list=[]
    response1 = s3.get_object(Bucket=bucket_name, Key=keyname1)
    response2 = s3.get_object(Bucket=bucket_name, Key=keyname2)
    
    '''read parquet file'''
    df_1 = pd.read_parquet(io.BytesIO(response1['Body'].read()))
    df_2 = pd.read_parquet(io.BytesIO(response2['Body'].read()))
    
    if df_1.empty == False and df_2.empty == False:
        df_merge = pd.merge(df_2, df_1,on='producerCode',how='outer')
        response = create_report(report_type,df_merge,year,month,rec_year,rec_month,bucket_name)
        return response
    else:
        response = "No data in files to process"
        return response

def create_report(report_type,df,year,month,rec_year,rec_month,bucket_name):
    response_list=[]
    try:
        group_by_sec_ag = df.groupby("producerCode")
        for sec_group,df_secondary in group_by_sec_ag:
            df_secondary.sort_values(by = ['quoteId'],inplace=True)
            df_secondary.reset_index(inplace = True, drop = True)
            response = dataframe_structure(report_type, s3_resource,df,df_secondary,sec_group,year,month,rec_year,rec_month, bucket_name)
            response_list.append(response)
        
        final_response = {  "Total number of files generated":len(response_list),
                            "BucketName": bucket_name,"CloseYear": year,"CloseMonth": month,"CallEDS":"N"}
        logger.info(f'lambda: {lambda_name} , lambda_handler() final_response: {final_response}')
        return  {
            "Total number of files generated":len(response_list),
            "CloseYear": year,
            "CloseMonth": month
        }

    except Exception as ex:
        logger.info(f"lambda: {lambda_name}, lambda_handler() exception {ex} ")
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        return response
def dataframe_structure(report_type, s3_resource,df_1,df_split,group,year,month,rec_year,rec_month, bucket_name):

    cost = []
    '''Calculate the costs for header details'''
    temp_group = df_split.groupby('producerCode')
    keys = temp_group.groups.keys()
    for df_head, df_data in df_1.iterrows():
        for key in keys:
            if str(df_data['producerCode']) == str(key):
                cost.append(float("{:.2f}".format(float(df_data['totalAmount']))))
    mvr_cost = float("{:.2f}".format(sum(cost)))
    df_split['producerCode']= df_split['producerCode'].apply(lambda x: '{0:0>7}'.format(x))
    df_split['quoteId']= df_split['quoteId'].apply(lambda x: '{0:0>10}'.format(x))
    df_split = df_split[["baseState", "producerCode","orderedDriversForCurrReq",
                    "quoteId", "reportType","costPerReport","quoteDate","policyNumber",
                    "policyIssueDate","waived",
                    "bindRatio","totalAmount"]]
    writer = pd.ExcelWriter("/tmp/Chargeback_ExpenseReport.xlsx", engine='xlsxwriter')
    df_split.to_excel(writer,index = False, sheet_name = 'Expense_Report',startrow=7,freeze_panes=(8,0))
    workbook = writer.book
    worksheet = writer.sheets['Expense_Report']
    format_excel_output(df_split,workbook,worksheet,mvr_cost)
    writer.save()
    response = upload_excel_to_s3(report_type, s3_resource,"/tmp/Chargeback_ExpenseReport.xlsx",bucket_name,group,year,month,rec_year,rec_month)
    retry_count = 3  
    while retry_count > 0 and response["statusCode"] != 200:
        time.sleep(0.25)
        response = upload_excel_to_s3(s3_resource,"/tmp/Chargeback_ExpenseReport.xlsx",bucket_name,group,year,month,rec_year,rec_month)
        retry_count -= 1
    return response

def format_excel_output(df_merge,workbook,worksheet,mvr_cost):
    
    """Change header names"""
    df_merge.rename(columns={"baseState": "BaseState", "producerCode": "ProducerCode","orderedDriversForCurrReq":"DriversOrderedOn",
                    "quoteId": "QuoteId", "reportType": "ReportType","costPerReport":"CostPerReport","quoteDate":"QuoteDate","policyNumber":"PolicyNumber",
                    "policyIssueDate":"PolicyIssueDate","waived":"ChargeWaived",
                    "bindRatio":"BindRatio","totalAmount":"TotalAmount"}, inplace=True)
                   
    '''cell format for colour in header'''
    cell_format = workbook.add_format({'bold': True, 'bg_color': '#b6d7a8','border': 1 })
    for col_num, value in enumerate(df_merge.columns.values):
        worksheet.write(7, col_num, value, cell_format)
        column_len = df_merge[value].astype(str).str.len().max()
        column_len = max(column_len,len(value)) + 3
        worksheet.set_column(col_num,col_num,column_len)
    ''' formats for merged cells  '''  
    merge_format = workbook.add_format({'align': 'left','valign':'top'})
    row_format = workbook.add_format({'align': 'right','valign':'top'})
    heading_format = workbook.add_format({'align': 'center','valign':'vcenter','bold': True,'border':1})
    other_format = workbook.add_format({'align': 'left','valign':'vcenter','bold': False})
    for agent_id in df_merge["ProducerCode"].unique():
        index = df_merge.loc[df_merge["ProducerCode"]==agent_id].index.values + 1
        df_merge['BindRatio'] = df_merge['BindRatio'].fillna('')
        df_merge['ChargeWaived'] = df_merge['ChargeWaived'].fillna('')
        if len(index) < 2:pass
        else:
            worksheet.merge_range(index[0]+7, 10, index[-1]+7, 10, df_merge.loc[index[0], 'BindRatio'], merge_format)
            worksheet.merge_range(index[0]+7, 11, index[-1]+7, 11,df_merge.loc[index[0], 'TotalAmount'], row_format)
            
    '''Heading to the Sheet and other required additional data'''  
    row_num = df_merge.index.values[-1] + 12
    sheet_name = f"{df_merge['ReportType'].iloc[0]} Expense Report"
    worksheet.merge_range('A2:G2', sheet_name, heading_format)
    worksheet.merge_range('A3:G3', todays_date.strftime("%B, %Y"), heading_format)
    worksheet.merge_range('A5:D5', 'Date Report Generated: '+ todays_date.strftime("%d-%b-%Y"), other_format)
    worksheet.merge_range('A6:D6', f'Total Cost:  ${mvr_cost}', other_format)
    
def upload_excel_to_s3(report_type, s3_resource, file_name, bucket, key, year, month,rec_year,rec_month):
    
    try:
        s3_resource.Bucket(bucket).upload_file(file_name,
                                               f'{report_type}/expense_report/ingestion_yyyymm={year}{month:02d}/expense_report.xlsx')
        return {'statusCode': HTTPStatus.OK.value,
                'Key': key,
                'Message': f'ExpenseReport-{key}-{rec_year}-{rec_month:02d}.xlsx has been successfully uploaded'}
    except Exception as ex:
        logger.exception(f"lambda {lambda_name}: Upload excel to S3: The upload failed with an exception {ex}")
        err_message = {'statusCode': HTTPStatus.NOT_FOUND.value,
                       'Key': key,
                       'message': f'ExpenseReport.xlsx  failed to upload for {key}-{rec_year}-{rec_month:02d}'}
        return err_message