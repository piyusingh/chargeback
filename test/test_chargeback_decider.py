import sys
import pytest
import boto3
import os, json
from lambdas.decider.Chargeback_Decider import *

os.environ["SERVICE"]="Chargeback-Decider"
os.environ["LOG_LEVEL"]="INFO"
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


#Expected Success Response for lambda_handler
success_response ={
  "quoteID": "9830661492",
  "reportType": "MVR",
  "resource": "/getChargeback",
  "source": "APIGateway",
  "correlation_id": "9e0a9c19-4b09-4d99-afed-28c2383b9a11"
}


#Expected Fail Response for lambda_handler

fail_response={
            "status": 500,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        
#Expected Return Response for getChargeback function
inputToStepFunctionResponseGetChargeback = {
  "quoteID": "9830661492",
  "reportType": "MVR",
  "resource": "/getChargeback",
  "source": "APIGateway",
  "correlation_id": "9e0a9c19-4b09-4d99-afed-28c2383b9a11"
}

#Expected Return Response for saveChargeback function
inputToStepFunctionResponseSaveChargeback={"body": {"chargeback": {"startDate": "01/22/2023","producerCode": "123456","policyNumber": "","baseState": "OH","lob": "PersonalAuto","quoteTime": "05:49:50","policyIssueDate": "","policyIssueTime": "","shallOrderFlag": True,"tracker": 1,"totalDrivers": 1,"totalOrderedDrivers": 1,"orderedDriversForCurrReq": 1,"orderDate": "01/22/2023","orderTime": "05:50:21","firstOrderDate": "01/22/2023","quoteID": "15151511515","policyIssued": False,"quoteDate": "01/22/2023","reportType": "MVR","driver": [{"firstName": "CATHERINE","DOB": "04/22/1979","lastName": "ANDERSON","licenseNumber": "OH242424","licenseType": "ActiveUS","violation": [{"occurrenceDate": "12/01/2021","convictionDate": "","incidentCategory": "MinorViolation"},{"occurrenceDate": "01/27/2021","convictionDate": "02/08/2021","incidentCategory": "MinorViolation"}]}]}},"resource": "/saveChargeback","source": "APIGateway","correlation_id": "0dd6ac23-50a4-4c25-80ee-5bd1cd897636"}

def test_success_lambda_handler_getChargeback():
    with open('jsonfiles/decider.json') as jsonfile:
            event=json.load(jsonfile)["success_event_get_chargeback"] #Reading successful event from json file
    response = lambda_handler(event,None)
    assert response == success_response

def test_success_lambda_handler_saveChargeback():
    with open('jsonfiles/decider.json') as jsonfile:
            event=json.load(jsonfile)["success_event_save_chargeback"] #Reading successful event from json file
    response = lambda_handler(event,None)
    assert response == inputToStepFunctionResponseSaveChargeback

def test_fail_lambda_handler():
  with open('jsonfiles/decider.json') as jsonfile:
    event=json.load(jsonfile)["failure_event_save_chargeback"]#Reading failing event from json file
  with pytest.raises(Exception) as excinfo:   
      lambda_handler(event,None) 
  assert str(excinfo.value) == str(fail_response)

def test_success_getChargeback():
  with open('jsonfiles/decider.json') as jsonfile:
    event=json.load(jsonfile)["success_event_get_chargeback"]#Reading successful event for getViolation function testing
  resource = event['resource']
  source = event['source']
  payload = json.loads(event['body'])
  correlation_id=event['correlation_id']
  getChargebackRequest = {
            'path' : resource,
            'requestbody' : payload,
            'source' : source,
            'correlation_id': correlation_id
        }
  
  response = get_chargeback(getChargebackRequest)
  assert response == inputToStepFunctionResponseGetChargeback

def test_success_saveChargeback():
  with open('jsonfiles/decider.json') as jsonfile:
    event=json.load(jsonfile)["success_event_save_chargeback"] #Reading successful event for saveViolation function testing
  resource = event['resource']
  payload = json.loads(event['body'])
  source = event['source']
  correlation_id=event['correlation_id']
  saveChargebackRequest = {
            'path' : resource,
            'requestbody' : payload,
            'source' : source,
            'correlation_id' : correlation_id
        }
  response = save_chargeback(saveChargebackRequest)
  assert response == inputToStepFunctionResponseSaveChargeback

    
    
    