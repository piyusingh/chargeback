import os, json
import sys
import pytest
import boto3
from lambdas.prepareDataRetrieveFromDynamoDB.Chargeback_PrepareDataRetrieveFromDynamoDB import lambda_handler

os.environ["SERVICE"]="Chargeback-PrepareDataSaveToDynamoDB"
os.environ["LOG_LEVEL"]="INFO"

#Expected Successful Response for lambda_handler
success_response ={
  "quoteID": "9830661492",
  "reportType": "MVR",
  "resource": "/getChargeback",
  "source": "APIGateway",
  "correlation_id": "9e0a9c19-4b09-4d99-afed-28c2383b9a11"
}

#Expected Fail Response for lambda_handler
fail_response = {
            "statusCode":500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Exception occured while executing lambda",
                "integrationName":"chargeback"
            })
        }

def test_success():
    with open('jsonfiles/prepareDataRetrieveFromDynamoDB.json') as jsonfile:
            event=json.load(jsonfile)["success_event_preparedataretrievefromdynamodb_chargeback"]       #Reading successful event from json file
    response = lambda_handler(event,None)
    print('printing response:',response)
    assert response == success_response

def test_fail():
    with open('jsonfiles/prepareDataRetrieveFromDynamoDB.json') as jsonfile:
            event=json.load(jsonfile)["failure_event_preparedataretrievefromdynamodb_chargeback"]         #Reading failing event from json file
    with pytest.raises(Exception) as excinfo:
       lambda_handler(event,None)
    assert str(excinfo.value) == str(fail_response)