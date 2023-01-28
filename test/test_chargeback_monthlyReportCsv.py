import json
import pytest
import boto3
import os
from mockito import when, ANY, unstub
from moto.s3 import mock_s3
from http import HTTPStatus
import random
import pandas as pd


os.environ['S3_BUCKET'] = 'test-bucket'
os.environ['SERVICE'] = 'Chargeback-ReportCSV'
os.environ['ATHENA_DB_NAME'] = 'test_athena_db'

lambda_handler_success_response = {
                'statusCode': HTTPStatus.OK.value,
                'response': 'Uploaded chargeback_monthly_report_csv successfully to S3'
            }

parquet_to_csv_failure_response = {
                'statusCode': HTTPStatus.NOT_FOUND.value,
                'response': 'Upload failed, no data found.'
            }

parquet_to_csv_exception_response={
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }

DEFAULT_REGION = 'us-east-1'

def test_lambda_handler():
    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV).parquet_to_csv(ANY).thenReturn(lambda_handler_success_response)
    assert lambda_handler_success_response == Chargeback_ReportCSV.lambda_handler({},None)

class AthenaQuery():
    empty = False
    def to_csv(file,header,index):
        return

class AthenaQueryFailure():
    empty = True
    def to_csv(file,header,index):
        return

class S3_reource():
    def Bucket(ANY):
        def upload_file(ANY,any):
            return
        return upload_file(None,None)

class Bucket():
    def upload_file(ANY,any):
        return
    

def test_success_parquet_to_csv():
    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV).query_athena(ANY,ANY,ANY,ANY,ANY,ANY,ANY).thenReturn(AthenaQuery())
    when(Chargeback_ReportCSV.s3_resource).Bucket(ANY).thenReturn(Bucket())
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
    response = Chargeback_ReportCSV.parquet_to_csv(event)
    assert response == lambda_handler_success_response
    unstub()

def test_failure_parquet_to_csv():
    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV).query_athena(ANY,ANY,ANY,ANY,ANY,ANY,ANY).thenReturn(AthenaQueryFailure())
    when(Chargeback_ReportCSV.s3_resource).Bucket(ANY).thenReturn(Bucket())
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
        response = Chargeback_ReportCSV.parquet_to_csv(event)
    print('printing response:',response)
    assert response == parquet_to_csv_failure_response
    unstub()

def test_exception_parquet_to_csv():
    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV).query_athena(ANY,ANY,ANY,ANY,ANY,ANY,ANY).thenReturn(AthenaQueryFailure())
    when(Chargeback_ReportCSV.s3_resource).Bucket(ANY).thenReturn(Bucket())
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["fail_event"]      #Reading successful event from json file
        response = Chargeback_ReportCSV.parquet_to_csv(event)
    print('printing response:',response)
    assert response == parquet_to_csv_exception_response
    unstub()

class Athena_Client():
    def start_query_execution(ANY,any):
        return {"QueryExecutionId" : "1234"}
    def get_query_execution(ANY):
        return { "QueryExecution" : { "Status" : {"State": random.choice(["RUNNING","SUCCEEDED"])},"ResultConfiguration" : {"OutputLocation" : "./csvFiles/chargeback_data.csv" }}}



def test_success_query_athena():
    #Creating expected response
    output_location="./csvfiles/chargeback_data.csv"
    data_df = pd.read_csv(output_location, dtype = {'pk':'str','producercode':'str','quoteid':'str','policynumber':'str'})
    date_cols = ['quotedate','policyissuedate','orderdate','currenttimestamp','closedate','firstorderdate']
    date_cols.append('startdate')
    data_df[date_cols] = data_df[date_cols].apply(pd.to_datetime)
    data_df=pd.DataFrame()

    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV.athena_client).start_query_execution(ANY,ANY).thenReturn(Athena_Client.start_query_execution(ANY,ANY))
    when(Chargeback_ReportCSV.athena_client).get_query_execution(ANY).thenReturn(Athena_Client.get_query_execution(ANY))
    #when(mvrExternalization_monthlyReportCsv.query_athena).query_response.then({"QueryExecutionId" : "1234"})

    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
        report_type = "MVR",
        record_year = 2023,
        record_month = 1,
        cycle_close_year = 2023,
        cycle_close_month = 3,
        start_date = 20230101,
        end_date = 20230131
        response = Chargeback_ReportCSV.query_athena(report_type, record_year, record_month, start_date, end_date, cycle_close_year, cycle_close_month)
    assert list(response.columns) == list(data_df.columns)
    unstub()


class Athena_Client_Exception():
    def start_query_execution(ANY,any):
        return {"QueryExecutionId" : "1234"}
    def get_query_execution(ANY):
        return { "QueryExecution" : { "Status" : {}}}

def test_exception_query_athena():
    #Creating expected response
    output_location="./csvfiles/chargeback_data.csv"
    data_df = pd.read_csv(output_location, dtype = {'pk':'str','producercode':'str','quoteid':'str','policynumber':'str'})
    date_cols = ['quotedate','policyissuedate','orderdate','currenttimestamp','closedate','firstorderdate']
    date_cols.append('startdate')
    #data_df[date_cols] = data_df[date_cols].apply(pd.to_datetime)
    data_df=pd.DataFrame()

    from lambdas.reportCSV import Chargeback_ReportCSV
    when(Chargeback_ReportCSV.athena_client).start_query_execution(ANY,ANY).thenReturn(Athena_Client_Exception.start_query_execution(ANY,ANY))
    when(Chargeback_ReportCSV.athena_client).get_query_execution(ANY).thenReturn(Athena_Client_Exception.get_query_execution(ANY))

    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
        report_type = "MVR"
        record_year = 2022
        record_month = 4
        start_date = 20220401
        end_date = 20220430
        cycle_close_year = 2022
        cycle_close_month = 6
        response = Chargeback_ReportCSV.query_athena(report_type, record_year, record_month, start_date, end_date, cycle_close_year, cycle_close_month)
    assert list(response.columns) == list(data_df.columns)
    unstub()

