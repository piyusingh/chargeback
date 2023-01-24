import sys
import pytest
import boto3
from moto import mock_dynamodb, mock_s3
from mockito import when, unstub, ANY
import datetime
import os,json
from lambdas.saveAndRetrieveFromDynamoDB import Chargeback_SaveAndRetrieveFromDynamoDB

#Setting environment variable
os.environ["CHARGEBACK_TABLE"]="Chargeback"
os.environ["CHARGEBACK_COST_TABLE"]="Chargeback-Cost"
os.environ["SERVICE"]="Chargeback-SaveAndRetrieveFromDynamoDB"
os.environ["LOG_LEVEL"]="INFO"
os.environ['AUDIT_LOGGER_TABLE']="Chargeback-Audit"

#Creating context class for lambda_handler
class Context:
    def __init__(self):
        self.function_name = "Chargeback-SaveAndRetrieveFromDynamoDB"

#Expected getViolation success response for lambda_handler
success_reponse_getChargeback ={
  "statusCode": 200,
  "body": {
    "message": "Data not available for the requested PK:NOTAID"
  }
}

#Expected getViolation fail response for lambda_handler
fail_reponse_getChargeback = {
            "statusCode": 500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Error occurred while reading event details",
                "integrationName":"chargeback"
            })
           
        }

fail_reponse_saveViolation = {
            "statusCode": 500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Error occurred while creating payload for Set or Get Violation",
                "integrationName":"chargeback"
            })
           
        }

currentTime= str(datetime.datetime.now()); ## Sort key which is considered as current timestamp

#Expected success response for request_payload function
request_payload_response={
  'CorrelationID': 'UNIQUE_ID_PER_API_CALL',
	"SK": "2021-08-09",
	"PK": "0000563743",
	"QuoteID": "0000563743",
	"CustomerID": "6826072e-0ac0-44ad-ad12-32b2c78acf82",
	"PrimaryID": "",
	"SecondaryAgencyID": "",
	"ProducerCode": "0232425",
	"LOB": "PersonalAuto",
	"BaseState": "WV",
	"RiskState": "WV",
	"Source": "l1-5-lite",
	"StartedIN": "Portal",
	"InitiatedBy": "Agent",
	"StartDate": "",
	"ReportType": "MVR",
	"QuoteDate": "22/07/2021",
	"QuoteTime": "11:00:10",
	"OrderDate": "30/07/2021",
	"OrderTime": "00:01:00",
	"PolicyIssued": False,
	"PolicyIssueDate": "",
	"PolicyIssueTime": "",
	"ShallOrderMVR": False,
	"MvrTracker": 0,
	"TotalDriver": 1,
	"payload": {
		"mvr": {
			"lob": "PersonalAuto",
			"startDate": "",
			"producerCode": "0232425",
			"baseState": "WV",
			"primaryID": "",
			"secondaryAgencyID": "",
			"startedIN": "Portal",
			"initiatedBy": "Agent",
			"orderDate": "30/07/2021",
			"orderTime": "00:01:00",
			"quoteTime": "11:00:10",
			"policyIssueDate": "",
			"policyIssueTime": "",
			"shallOrderMVR": False,
			"mvrTracker": 0,
			"totalDriver": 1,
			"reportType": "MVR",
			"quoteDate": "22/07/2021",
			"policyIssued": False,
			"customerID": "6826072e-0ac0-44ad-ad12-32b2c78acf82",
			"quoteID": "0000563743",
			"driver": [{
				"policyDriverFName": "gxh",
				"policyDriverLName": "gjf",
				"policyDriverDOB": "1990-01-01",
				"driverPublicId": "35327",
				"violation": [{
					"occurrenceDate": "2018-06-30",
					"attributeDerivedFlag_Ext": "Agent_Ext",
					"isNAFForgiven": False,
					"exclusionIndicator": "No",
					"derivedAttributeDesc": "DEFECTIVE EQUIPMENT",
					"incidentCategory": "Info",
					"isRated": False,
					"forgivenessCode": "N",
					"derivedAttributeCode": "DEQ"
				}],
				"isMVRProcessed_Ext": False
			}],
			"source": "l1-5-lite"
		}
	}
}


request_payload_fail_response={
            "statusCode": 500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Error while checking existing of fields",
                "integrationName":"MVRExternalization"
            })
           
        }
##Expected success response for lambda_handler save violation 

success_reponse_saveViolation = {
        'statusCode':200,
        'body': {
            "message":"Dynamo DB insertion is successful for QuoteID:" + "0000563743"
        }
    }
#Expected success response for insert_data_dynamo function
insert_data_dynamodb_response = {
        'statusCode':200,
        'body': {
            "message":"Dynamo DB insertion is successful for QuoteID:" + "0000563743"
        }
    }

#Expected success response for retrieve_from_dynamodb function
retrieve_from_dynamodb_response = {
            "statusCode":200,
            "body":{
                 "message":"Data not available for the requested PK:" + "NOTAID"
            }
        }
retrieve_from_dynamodb_response_data = {
  "statusCode": 200,
  "body": {
    "mvr": {
      "mvrTracker": 0,
      "totalDriver": 1,
      "producerCode": "0232425",
      "source": "l1-5-lite",
      "primaryID": "",
      "policyIssued": False,
      "quoteID": "0000563743",
      "reportType": "MVR",
      "baseState": "WV",
      "quoteTime": "11:00:10",
      "shallOrderMVR": False,
      "orderTime": "00:01:00",
      "driver": [
        {
          "policyDriverLName": "gjf",
          "policyDriverDOB": "1990-01-01",
          "isMVRProcessed_Ext": False,
          "policyDriverFName": "gxh",
          "driverPublicId": "35327",
          "violation": [
            {
              "occurrenceDate": "2018-06-30",
              "exclusionIndicator": "No",
              "isRated": False,
              "forgivenessCode": "N",
              "derivedAttributeCode": "DEQ",
              "derivedAttributeDesc": "DEFECTIVE EQUIPMENT",
              "incidentCategory": "Info",
              "isNAFForgiven": False,
              "attributeDerivedFlag_Ext": "Agent_Ext"
            }
          ]
        }
      ],
      "policyIssueDate": "",
      "customerID": "6826072e-0ac0-44ad-ad12-32b2c78acf82",
      "startedIN": "Portal",
      "orderDate": "30/07/2021",
      "policyIssueTime": "",
      "lob": "PersonalAuto",
      "startDate": "",
      "secondaryAgencyID": "",
      "initiatedBy": "Agent",
      "quoteDate": "22/07/2021"
    }
  }
}
retrieve_from_dynamodb_response_fail = {
            "body":{
                "error":{
                    "message":"Data not available for the requested PK:" + "9830661492",
                    "type": "Data Not Found"
                }
            }
            }

def test_success_lambda_handler_getChargeback():
  #When statements : returning specifc values for function calls in lambda_handler
  when(Chargeback_SaveAndRetrieveFromDynamoDB).request_payload(ANY).thenReturn({})
  when(Chargeback_SaveAndRetrieveFromDynamoDB).check_property_exist(ANY, ANY).thenReturn({})
  when(Chargeback_SaveAndRetrieveFromDynamoDB).insert_data_dynamo(ANY, ANY, ANY, ANY, ANY, ANY, ANY).thenReturn({})
  when(Chargeback_SaveAndRetrieveFromDynamoDB).retrieve_from_dynamo_db(ANY,ANY,ANY).thenReturn(success_reponse_getChargeback)
  with open('jsonfiles/saveAndRetrieveFromDynamoDB.json') as jsonfile:
            event=json.load(jsonfile)["success_event_saveandretrievefromdynamodb_chargeback"]     #Reading successful event for getViolation request
  context=Context()
  result = Chargeback_SaveAndRetrieveFromDynamoDB.lambda_handler(event, context)
  assert success_reponse_getChargeback == result
  unstub()

# def test_success_lambda_handler_saveViolation():
#   #When statements : returning specifc values for function calls in lambda_handler
#   with mock_dynamodb2():
#     when(Chargeback_SaveAndRetrieveFromDynamoDB).request_payload(ANY).thenReturn({})
#     when(Chargeback_SaveAndRetrieveFromDynamoDB).check_property_exist(ANY, ANY).thenReturn({})
#     when(Chargeback_SaveAndRetrieveFromDynamoDB).insert_data_dynamo(ANY, ANY, ANY).thenReturn(insert_data_dynamodb_response)
#     when(Chargeback_SaveAndRetrieveFromDynamoDB).retrieve_from_dynamo_db(ANY,ANY,ANY).thenReturn({})
#     with open('jsonfiles/saveAndgetViolation.json') as jsonfile:
#               event=json.load(jsonfile)["saveViolationSuccess"]     #Reading successful event for getViolation request
#     chargeback_table = os.environ["CHARGEBACK_TABLE"]
#     dynamoDB_client = boto3.resource("dynamodb")
#     _ = dynamoDB_client.create_table(
#               TableName = chargeback_table,
#               KeySchema=[
#                   {"AttributeName": "PK", "KeyType": "HASH"},
#                   {"AttributeName": "SK", "KeyType": "RANGE"},
#                   ],
#               AttributeDefinitions=[
#                   {"AttributeName": "PK", "AttributeType": "S"},
#                   {"AttributeName": "SK", "AttributeType": "S"},
#                   ]
#                 )
#     table = dynamoDB_client.Table(chargeback_table)
#     context=Context()
#     result = Chargeback_SaveAndRetrieveFromDynamoDB.lambda_handler(event, context)
#     assert success_reponse_saveViolation == result
#     unstub()

# def test_fail_lambda_handler():
#   #When statements : returning specifc values for function calls in lambda_handler
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).request_payload(ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).check_property_exist(ANY, ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).insert_data_dynamo(ANY, ANY, ANY,ANY,ANY,ANY,ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).retrieve_from_dynamo_db(ANY,ANY,ANY).thenReturn(success_reponse_getViolation)
#   with open('jsonfiles/saveAndRetrieveFromDynamoDB.json') as jsonfile:
#             event=json.load(jsonfile)["failure_event_saveandretrievefromdynamodb_chargeback"]        #Reading failing event for getViolation request
#   #with pytest.raises(Exception) as excinfo:
#   context=Context()
#   result = Chargeback_SaveAndRetrieveFromDynamoDB.lambda_handler(event, context)
#   unstub()
#   assert str(result) == str(fail_reponse_getChargeback)
  
# def test_Fail_lambda_handler_saveviolation():
#   #When statements : returning specifc values for function calls in lambda_handler
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).request_payload(ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).check_property_exist(ANY, ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).insert_data_dynamo(ANY, ANY, ANY).thenReturn({})
#   when(Chargeback_SaveAndRetrieveFromDynamoDB).retrieve_from_dynamo_db(ANY,ANY,ANY).thenReturn(success_reponse_getViolation)
#   with open('jsonfiles/saveAndgetViolation.json') as jsonfile:
#             event=json.load(jsonfile)["saveViolationFail"]        #Reading failing event for getViolation request
#   with pytest.raises(Exception) as excinfo:
#     context=Context()
#     Chargeback_SaveAndRetrieveFromDynamoDB.lambda_handler(event, context)
#   unstub()
#   assert str(excinfo.value) == str(fail_reponse_saveViolation)

# def test_success_request_payload():
#   with open('jsonfiles/saveAndgetViolation.json') as jsonfile:
#     event=json.load(jsonfile)["saveViolationRequestPayload"]                     #Reading successful event for saveViolation request
#   result = Chargeback_SaveAndRetrieveFromDynamoDB.request_payload(event)
#   result["SK"]="2021-08-09"              #Only saving date values in SK to compare            
#   assert request_payload_response == result


# def test_check_property_exist():
#   with open('jsonfiles/saveAndgetViolation.json') as jsonfile:
#     event=json.load(jsonfile)["saveViolationRequestPayload"]                    #Reading successful event for saveViolation request
#   MVR = event['payload']['mvr']
#   result=Chargeback_SaveAndRetrieveFromDynamoDB.check_property_exist(MVR, 'quoteID')
#   assert MVR['quoteID'] == result 

# def test_insert_data_dynamo():
#   #mocking dynaodb
#   with mock_dynamodb2():
#     chargeback_table = os.environ["CHARGEBACK_TABLE"]
#     payloadforDynamoDB = request_payload_response
#     with open('jsonfiles/saveAndgetViolation.json') as jsonfile: 
#       event = json.load(jsonfile)["saveViolationRequestPayload"]              #Reading successful event for saveViolation request
#     QuoteID = event['payload']['mvr']['quoteID']
#     dynamodb = boto3.resource("dynamodb")
#     _ = dynamodb.create_table(
#             TableName = chargeback_table,
#             KeySchema=[
#                 {"AttributeName": "PK", "KeyType": "HASH"},
# 				        {"AttributeName": "SK", "KeyType": "RANGE"},
#                 ],
#             AttributeDefinitions=[
#                 {"AttributeName": "PK", "AttributeType": "S"},
#                 {"AttributeName": "SK", "AttributeType": "S"},
#                 ]
#               )
#     table = dynamodb.Table(chargeback_table)
#     response = Chargeback_SaveAndRetrieveFromDynamoDB.insert_data_dynamo(payloadforDynamoDB,mvr_violations_table,QuoteID)
#     assert insert_data_dynamodb_response == response

# def test_retrieve_from_dynamo_db_data():
#   #mocking dynamodb
#   with mock_dynamodb2():
#     mvr_violations_table = os.environ["CHARGEBACK_TABLE"]
#     with open('jsonfiles/saveAndgetViolation.json') as jsonfile:
#       event = json.load(jsonfile)["getViolationSuccessData"]             #Reading successful event for getViolation request
#     QuoteID = event['quoteID']
#     source = event['source']
#     dynamodb = boto3.resource("dynamodb")
#     _ = dynamodb.create_table(
#             TableName=mvr_violations_table,
#             KeySchema=[
#                 {"AttributeName": "PK", "KeyType": "HASH"},
# 				        {"AttributeName": "SK", "KeyType": "RANGE"},
#                 ],AttributeDefinitions=[
#                 {"AttributeName": "PK", "AttributeType": "S"},
#                 {"AttributeName": "SK", "AttributeType": "S"},
#                 ]
#               )
#     table = dynamodb.Table(mvr_violations_table)
#     table.put_item(Item=request_payload_response)
#     response_payload=Chargeback_SaveAndRetrieveFromDynamoDB.retrieve_from_dynamo_db(QuoteID,source,mvr_violations_table)
#     assert retrieve_from_dynamodb_response_data == response_payload

# def test_retrieve_from_dynamo_db_fail():
#   #mocking dynamodb
#   with mock_dynamodb():
#     chargeback_violations_table = os.environ["CHARGEBACK_TABLE"]
#     with open('jsonfiles/saveAndRetrieveFromDynamoDB.json') as jsonfile:
#       event = json.load(jsonfile)["failure_event_saveandretrievefromdynamodb_getchargeback"]#Reading successful event for getViolation request
#     QuoteID = event['quoteID']
#     reportType = "INVALIDSOURCE"
#     dynamodb = boto3.resource("dynamodb")
#     _ = dynamodb.create_table(
#             TableName=chargeback_violations_table,
#             KeySchema=[
#                 {"AttributeName": "PK", "KeyType": "HASH"},
# 				        {"AttributeName": "SK", "KeyType": "RANGE"},
#                 ],AttributeDefinitions=[
#                 {"AttributeName": "PK", "AttributeType": "S"},
#                 {"AttributeName": "SK", "AttributeType": "S"},
#                 ],
#             ProvisionedThroughput={
#             'ReadCapacityUnits': 1,
#             'WriteCapacityUnits': 1
#         }
#       )
            
#     table = dynamodb.Table(chargeback_violations_table)
#     response_payload=Chargeback_SaveAndRetrieveFromDynamoDB.retrieve_from_dynamo_db(QuoteID,reportType,chargeback_violations_table)
#     assert retrieve_from_dynamodb_response_fail == response_payload