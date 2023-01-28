import os, json
import sys
import pytest
import boto3
import datetime
from mock import patch
from moto import mock_stepfunctions
from mockito import when, unstub, ANY
from moto.s3 import mock_s3
from unittest.mock import MagicMock
import pandas as pd
#from pandas.util.testing import assert_frame_equal
from pandas.testing import assert_frame_equal

lambda_handler_success_response = response = {
    'statusCode': 200,
    'response': 'Uploaded chargeback_aggregate_report successfully to S3'
}

lambda_handler_fail_response = response = {
    "statusCode": 500,
    "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
}

transaction_level = {
  "baseState": "OH",
  "producerCode": "87654321",
  "reportType": "MVR",
  "quoteId": "0987654123",
  "costPerReport": 5,
  "quoteDate": "2023-01-22 05:49:50",
  "policyNumber": "",
  "policyIssueDate": "",
  "waived": "N",
  "orderedDriversForCurrReq": 2
}

d1 = {
  "baseState": "OH",
  "producerCode": "87654321",
  "reportType": "MVR",
  "quoteId": "0987654123",
  "quoteDate": "2023-01-22 05:49:50",
  "policyNumber": "",
  "policyIssueDate": "",
  "waived": "N",
  "orderedDriversForCurrReq": 2
}

aggregate_level = {
  "producerCode": "87654321",
  "bindRatio": "0%",
  "totalAmount": 10
}

final_aggregate = {
    "producerCode": "87654321",
    "bindRatio": "0%",
    "totalAmount": 10
    }

DEFAULT_REGION = 'us-east-1'
keyToTransactionsLevelFile = "chargeback_reports/intermediate_files/mvr/transactions_report/ingestion_yyyymm=202206/transactions_report.parquet"

@pytest.fixture(scope='module')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture()
def s3_resource(aws_credentials):
    with mock_s3():
        resource = boto3.resource("s3", region_name="us-east-1")
        yield resource

@pytest.fixture()
def s3_client(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn

@mock_s3
@patch('s3fs.core.S3FileSystem')
@patch('s3fs.S3FileSystem.open', side_effect=open)
@patch('s3fs.S3FileSystem.ls', side_effect=os.listdir)
def test_success_lambda_handler(mock_s3fs, s3fs_open, s3fs_ls, monkeypatch, s3_resource, s3_client):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    from lambdas.aggregateReport.Chargeback_AggregateReport import lambda_handler
    from lambdas.aggregateReport import Chargeback_AggregateReport
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
    when(Chargeback_AggregateReport).concatenate(ANY,ANY,ANY,ANY,ANY,ANY,ANY).thenReturn(lambda_handler_success_response)
    response = lambda_handler(event,None)
    assert response == lambda_handler_success_response
    unstub()

@mock_s3
@patch('s3fs.core.S3FileSystem')
@patch('s3fs.S3FileSystem.open', side_effect=open)
@patch('s3fs.S3FileSystem.ls', side_effect=os.listdir)
def test_concatenate(mock_s3fs, s3fs_open, s3fs_ls, monkeypatch, s3_resource, s3_client):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('parquetFiles/transactions_report.parquet','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",keyToTransactionsLevelFile)
    from lambdas.aggregateReport import Chargeback_AggregateReport
    transaction_df = pd.DataFrame(transaction_level, index=[0])
    when(Chargeback_AggregateReport).convert_to_df(ANY).thenReturn(transaction_df)
    final_aggregate_df = pd.DataFrame(final_aggregate,index=[0])
    when(Chargeback_AggregateReport).calculate_total_amount(ANY,ANY,ANY).thenReturn(final_aggregate_df)
    response = Chargeback_AggregateReport.concatenate("mvr", "20220401", "20220430", 2022, 4, 2022, 6)
    print('printing response:',response)
    assert response == lambda_handler_success_response
    unstub()

@mock_s3
@patch('s3fs.core.S3FileSystem')
@patch('s3fs.S3FileSystem.open', side_effect=open)
@patch('s3fs.S3FileSystem.ls', side_effect=os.listdir)
def test_concatenate_empty_df(mock_s3fs, s3fs_open, s3fs_ls, monkeypatch, s3_resource, s3_client):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('parquetFiles/transactions_report.parquet','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",keyToTransactionsLevelFile)
    from lambdas.aggregateReport import Chargeback_AggregateReport
    transaction_df = pd.DataFrame()
    when(Chargeback_AggregateReport).convert_to_df(ANY).thenReturn(transaction_df)
    final_aggregate_df = pd.DataFrame(final_aggregate,index=[0])
    when(Chargeback_AggregateReport).calculate_total_amount(ANY,ANY,ANY).thenReturn(final_aggregate_df)
    response = Chargeback_AggregateReport.concatenate("mvr", "20220401", "20220430", 2022, 4, 2022, 6)
    assert response == lambda_handler_fail_response
    unstub()


@mock_s3
@patch('s3fs.core.S3FileSystem')
@patch('s3fs.S3FileSystem.open', side_effect=open)
@patch('s3fs.S3FileSystem.ls', side_effect=os.listdir)
def test_concatenate_fail(mock_s3fs, s3fs_open, s3fs_ls, monkeypatch, s3_resource, s3_client):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    from lambdas.aggregateReport import Chargeback_AggregateReport
    response = Chargeback_AggregateReport.concatenate("mvr", "20220401", "20220430", 2022, 4, 2022, 6)
    assert response == lambda_handler_fail_response
    unstub()

@patch('s3fs.core.S3FileSystem')
@patch('s3fs.S3FileSystem.open', side_effect=open)
@patch('s3fs.S3FileSystem.ls', side_effect=os.listdir)
def test_convert_to_df(mock_s3fs, s3fs_open, s3fs_ls, monkeypatch,s3_client):
    expected_output=None
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('parquetFiles/transactions_report.parquet','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",keyToTransactionsLevelFile)
    from lambdas.aggregateReport import Chargeback_AggregateReport
    root_dir_path = "chargebackinsurance/chargeback_reports/intermediate_files/mvr/transactions_report/ingestion_yyyymm=202206/transactions_report.parquet"
    response = Chargeback_AggregateReport.convert_to_df(root_dir_path)
    assert response == None
    unstub()

@mock_s3
def test_calculate_total_amount(monkeypatch):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    final_aggregate_df = pd.DataFrame(final_aggregate, index=[0])
    from lambdas.aggregateReport import Chargeback_AggregateReport
    transaction_df = pd.DataFrame(transaction_level,index=[0])
    response = Chargeback_AggregateReport.calculate_total_amount(transaction_df, 2022, 4)
    print('printing response:',response)
    assert_frame_equal(response, final_aggregate_df, check_dtype=False)
    unstub()

def test_fail_calculate_total_amount(monkeypatch):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    monkeypatch.setenv('AMOUNT_CRITERIA', "50")
    from lambdas.aggregateReport import Chargeback_AggregateReport
    transaction_df = pd.DataFrame(d1,index=[0])
    response = Chargeback_AggregateReport.calculate_total_amount(transaction_df, 2022, 4)
    assert response == lambda_handler_fail_response
    unstub()
