import sys
import pytest
import boto3
import os,json
from lambdas.prepareDataSaveToDynamoDB.Chargeback_PrepareDataSaveToDynamoDB import lambda_handler

os.environ["SERVICE"]="mvrExternalization_saveViolationDetails"
os.environ["LOG_LEVEL"]="INFO"

#Expected successful response for lambda_handler
success_response ={
  "quoteID": "9830661494",
  "producerCode": "12345678",
  "lob": "PersonalAuto",
  "baseState": "OH",
  "startDate": "01/22/2023",
  "reportType": "CLUE",
  "orderDate": "01/22/2023",
  "orderTime": "05:50:21",
  "quoteDate": "01/22/2023",
  "quoteTime": "05:49:50",
  "policyIssued": "False",
  "policyIssueDate": "",
  "policyIssueTime": "",
  "policyNumber": "",
  "payload": {
    "chargeback": {
      "producerCode": "12345678",
      "lob": "PersonalAuto",
      "baseState": "OH",
      "quoteID": "9830661494",
      "totalDrivers": 5,
      "totalOrderedDrivers": 3,
      "orderedDriversForCurrReq": 3,
      "firstOrderDate": "01/22/2023",
      "startDate": "01/22/2023",
      "reportType": "CLUE",
      "orderDate": "01/22/2023",
      "orderTime": "05:50:21",
      "quoteDate": "01/22/2023",
      "quoteTime": "05:49:50",
      "policyIssued": "False",
      "policyIssueDate": "",
      "policyIssueTime": "",
      "policyNumber": "",
      "shallOrderFlag": "True",
      "driver": [
        {
          "firstName": "CATHERINE",
          "DOB": "04/22/1979",
          "lastName": "ANDERSON",
          "licenseNumber": "OH242424",
          "licenseType": "ActiveUS",
          "violation": [
            {
              "occurrenceDate": "12/01/2021",
              "convictionDate": "",
              "incidentCategory": "MinorViolation"
            },
            {
              "occurrenceDate": "01/27/2021",
              "convictionDate": "02/08/2021",
              "incidentCategory": "MinorViolation"
            }
          ]
        }
      ]
    }
  },
  "resource": "/saveChargeback",
  "correlation_id": "f1945ec5-ac18-4890-9130-ac5f10016436"
}
#Expected fail response for lambda_handler
fail_reponse={'status': 500, 'response': 'Oops, something went wrong ! Please reach out to the technical team for assistance.'}

def test_success():
  with open('jsonfiles/prepareDataSaveToDynamoDB.json') as jsonfile:
          event=json.load(jsonfile)["success_event_preparedatasavetodynamodb_chargeback"]             #Reading successful event from json file
  response = lambda_handler(event,None)
  print('printing response:',response)
  assert response == success_response

def test_fail():
  with open('jsonfiles/prepareDataSaveToDynamoDB.json') as jsonfile:
    event=json.load(jsonfile)["failure_event_preparedatasavetodynamodb_chargeback"]                      #Reading failing event from json file
  with pytest.raises(Exception) as excinfo:
    lambda_handler(event,None)
  assert str(excinfo.value) == str(fail_reponse)
