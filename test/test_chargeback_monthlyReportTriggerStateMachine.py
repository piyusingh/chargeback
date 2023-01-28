import os, json
import sys
import pytest
import boto3
from moto import mock_stepfunctions
from mockito import when, unstub, ANY
import datetime
from dateutil.tz import tzutc
from http import HTTPStatus
from mock import patch
from lambdas.reportTriggerStateMachine import Chargeback_ReportTriggerStateMachine


#Setting environment variables
os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:us-east-1:187879053795:stateMachine:ChargebackReportStateMachine"
os.environ["SERVICE"] = "Chargeback-ReportTriggerStateMachine"

DEFAULT_REGION = 'us-east-1'

class Context:
    def __init__(self):
        self.function_name = "ChargebackReportStateMachine"
        self.aws_request_id = "6c890102-9503-42a9-89a1-6e3f87966115"

lambda_handler_success_response={'statusCode': 200, 'body': '{"message": "chargeback monthly report state machine triggered"}'}
lambda_handler_fail_response= {'statusCode': 500, 'body': '{"message": "Oops, something went wrong ! Please reach out to the technical team for assistance."}'}
invoke_state_machine_response={'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test_step_function:8e8d2514-4487-45ee-a704-181e6556c8cd', 'startDate': datetime.datetime(2021, 7, 7, 20, 47, 24, 702000, tzinfo=tzutc()), 'ResponseMetadata': {'RequestId': 'Q39T87M2FA74O67MQLOT4EYC7VCFS1BNPMFOY8OFF5XDIEET5MC9', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com', 'x-amzn-requestid': 'Q39T87M2FA74O67MQLOT4EYC7VCFS1BNPMFOY8OFF5XDIEET5MC9'}, 'RetryAttempts': 0}}

def test_failure_lambda_handler():
    with mock_stepfunctions():
        context=Context()
        event=None
        response=Chargeback_ReportTriggerStateMachine.lambda_handler(event, context)
        assert response == lambda_handler_fail_response

def test_success_lambda_handler():
    with mock_stepfunctions():
        when(Chargeback_ReportTriggerStateMachine).trigger_stepfunction(ANY, ANY).thenReturn(invoke_state_machine_response)
        context=Context()
        event={
            "queryStringParameters": 
            {"report_type": "mvr",
            "end_date": "20230331",
            "start_date": "20230101"}}
        response=Chargeback_ReportTriggerStateMachine.lambda_handler(event, context)
        assert response == lambda_handler_success_response