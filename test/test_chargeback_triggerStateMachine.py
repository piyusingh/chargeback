import os, json
import sys
import pytest
import boto3
from moto import mock_stepfunctions,mock_dynamodb
from mockito import when, unstub, ANY
import datetime
from dateutil.tz import tzutc
from http import HTTPStatus

from mock import patch
from lambdas.triggerStateMachine import Chargeback_TriggerStateMachine


#Setting environment variables
os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:us-east-1:187879053795:stateMachine:ChargebackReportStateMachine"
os.environ["SERVICE"] = "Chargeback-TriggerStateMachine"
os.environ["AUDIT_LOGGER_TABLE"]="Chargeback-Audit"
os.environ['LOG_LEVEL']="INFO"

DEFAULT_REGION = 'us-east-1'

class Context:
    def __init__(self):
        self.function_name = "Chargeback-TriggerStateMachine"
        self.aws_request_id = "6c890102-9503-42a9-89a1-6e3f87966115"

lambda_handler_success_response={'statusCode': 200, 'body': '{"message": "chargeback monthly report state machine triggered"}'}
lambda_handler_fail_response= {'statusCode': 500, 'body': '{"message": "Oops, something went wrong ! Please reach out to the technical team for assistance."}'}
invoke_state_machine_response={'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test_step_function:8e8d2514-4487-45ee-a704-181e6556c8cd', 'startDate': datetime.datetime(2021, 7, 7, 20, 47, 24, 702000, tzinfo=tzutc()), 'ResponseMetadata': {'RequestId': 'Q39T87M2FA74O67MQLOT4EYC7VCFS1BNPMFOY8OFF5XDIEET5MC9', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com', 'x-amzn-requestid': 'Q39T87M2FA74O67MQLOT4EYC7VCFS1BNPMFOY8OFF5XDIEET5MC9'}, 'RetryAttempts': 0}}
describeStateMachineExecutin_response={
	"executionArn": "arn:aws:states:us-east-1:123456789012:execution:test_step_function:4373179a-1b78-4cab-8e6f-57be7d6bf82f",
	"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:test_step_function",
	"name": "4373179a-1b78-4cab-8e6f-57be7d6bf82f",
	"status": "SUCCEEDED",
	"input": "{\"resource\": \"/getChargeback\", \"body\": \"{\\r\\n\\\"quoteID\\\": \\\"0000059925\\\"\\r\\n}\"}",
	"output": "{\"statusCode\": \"200\", \"body\": \"{\\r\\n\\\"quoteID\\\": \\\"0000059925\\\"\\r\\n}\"}",
	"ResponseMetadata": {
		"RequestId": "GSN19FHZ51EXK13AD8MMASKUKLIYQU0FN3ZWNC2F4DQ8Z8QJFBPN",
		"HTTPStatusCode": 200,
		"HTTPHeaders": {
			"server": "amazon.com",
			"x-amzn-requestid": "GSN19FHZ51EXK13AD8MMASKUKLIYQU0FN3ZWNC2F4DQ8Z8QJFBPN"
		},
		"RetryAttempts": 0
	}
}
lambda_handler_getChargeback_success_response={'statusCode': '200', 'body': '"{\\r\\n\\"quoteID\\": \\"0000059925\\"\\r\\n}"'}
# def test_failure_lambda_handler():
#     with mock_stepfunctions():
#         context=Context()
#         event=None
#         response=Chargeback_ReportTriggerStateMachine.lambda_handler(event, context)
#         assert response == lambda_handler_fail_response

def test_success_lambda_handler():
    with mock_stepfunctions():
        table_name = os.getenv('AUDIT_LOGGER_TABLE','Chargeback-Audit')
        with mock_dynamodb():
            dynamodb = boto3.resource("dynamodb")
            policy_table = dynamodb.create_table(
                TableName='Chargeback-Audit',
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                            {"AttributeName": "SK", "KeyType": "RANGE"}
                ],AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"}
                ],
                ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1
            }
        )
            table = dynamodb.Table(table_name)
            when(Chargeback_TriggerStateMachine).invoke_state_machine(ANY, ANY).thenReturn(invoke_state_machine_response)
            when(Chargeback_TriggerStateMachine).describe_state_machine_execution(ANY).thenReturn(describeStateMachineExecutin_response)
            context=Context()
            with open('jsonFiles/triggerStateMachine.json') as jsonfile:
                event=json.load(jsonfile)["getChargeBackEvent"]
            response=Chargeback_TriggerStateMachine.lambda_handler(event, context)
            assert response == lambda_handler_getChargeback_success_response