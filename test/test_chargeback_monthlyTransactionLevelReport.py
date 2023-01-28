import json
import pytest
import boto3
import os
from mockito import when, ANY, unstub
from moto.s3 import mock_s3
from http import HTTPStatus
import mock

event = {
"report_type": "MVR",
"record_year": 2023,
"record_month": 1,
"cycle_close_year": 2023,
"cycle_close_month": 3,
"start_date": "20230101",
"end_date": "20230131"
}

from lambdas.transactionReport.Chargeback_TransactionReport import lambda_handler
report_type = event["report_type"]
year = event["cycle_close_year"]
month = event["cycle_close_month"]
rec_month = event["record_month"]
rec_year = event["record_year"]
start_date= event["start_date"]
end_date = event["end_date"]

key = f'chargeback_reports/intermediate_files/mvr/final_csv_file/ingestion_yyyymm={year}{month:02d}/chargeback_data.csv'

os.environ["SERVICE"]="Chargeback-TransactionReport"
os.environ['S3_BUCKET']="chargebackinsurance"

bucket_name = "chargebackinsurance"

lambda_handler_success_response =  {
                'statusCode': HTTPStatus.OK.value,
                'response': 'Uploaded chargeback_transaction_level_report successfully to S3'
            }

lambda_handler_empty_file_response={'statusCode': 404, 'response': 'Upload failed, no data found.'}

lambda_handler_fail_response={
            "statusCode": 500,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }

DEFAULT_REGION = 'us-east-1'

@pytest.fixture(scope='module')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture()
def s3con():
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture()
def s3_client(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn

@pytest.fixture()
def s3_resource(aws_credentials):
    with mock_s3():
        resource = boto3.resource("s3", region_name="us-east-1")
        yield resource

@pytest.fixture()
def s3_bucket(s3_client, s3_resource):
    s3_bucket=s3_client.create_bucket(Bucket=bucket_name)
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           'csvfiles/chargeback_data.csv'), 'rb') as data:
        s3_client.upload_fileobj(data, bucket_name, key)
    yield 

def test_bucket(s3_client, s3_bucket):
    response = s3_client.list_buckets()
    buckets = [bucket["Name"] for bucket in response["Buckets"]][0]
    assert buckets == bucket_name


def test_file(s3_client, s3_bucket):
    data = s3_client.list_objects(Bucket=bucket_name, Prefix=f"chargeback_reports/intermediate_files/mvr/final_csv_file/ingestion_yyyymm={year}{month:02d}")
    file_name = data['Contents'][0]['Key']
    assert file_name == f"chargeback_reports/intermediate_files/mvr/final_csv_file/ingestion_yyyymm={year}{month:02d}/chargeback_data.csv"


class Bucket():
    def upload_file(ANy,ANY):
        return
@mock.patch("lambdas.transactionReport.Chargeback_TransactionReport.s3_resource.Bucket", return_value=Bucket)
def test_success_lambda_handler(mock_bucket,s3_client, s3_bucket, s3_resource,monkeypatch):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('csvfiles/chargeback_data.csv','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",key)
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
        response = lambda_handler(event,None)
        assert response == lambda_handler_success_response

def test_success_lambda_handler_with_empty_csv(s3_client, s3_bucket, s3_resource,monkeypatch):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('csvfiles/chargeback_data_empty.csv','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",key)
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["success_event"]      #Reading successful event from json file
        response = lambda_handler(event,None)
        assert response == lambda_handler_empty_file_response

def test_fail_lambda_handler(s3_client, s3_bucket, s3_resource,monkeypatch):
    monkeypatch.setenv('S3_BUCKET', "chargebackinsurance")
    s3_client.create_bucket(Bucket="chargebackinsurance")
    with open('csvfiles/chargeback_data.csv','rb') as data:
        s3_client.upload_fileobj(data,"chargebackinsurance",key)
    with open('jsonfiles/monthlyReportCsv.json') as jsonfile:
        event=json.load(jsonfile)["fail_event_1"]      #Reading successful event from json file
        response = lambda_handler(event,None)
        assert response == lambda_handler_fail_response

    