import json
from unittest.mock import patch
import boto3
import os
import time
import traceback
import datetime
from pytz import timezone
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger
from botocore.config import Config
from decimal import Decimal
from dateutil.relativedelta import relativedelta

## Global Variable
config = Config(
    retries={
        'max_attempts': 2,
        'mode': 'standard'
    }
)
dynamodb_client = boto3.resource('dynamodb', config=config)
lambda_client = boto3.client('lambda')
lambda_name = os.environ['SERVICE']
log_level = os.environ['LOG_LEVEL']
logger = Logger(service=lambda_name)
logger.setLevel(log_level)
lambda_function_name = ''
function_name = 'lambda_handler'
start_time = time.perf_counter()
eastern = timezone('US/Eastern')

class chargebackException(Exception):
    pass

def lambda_handler(event, context):
    response = ''
    try:
        chargeback_table = os.environ['CHARGEBACK_TABLE']
        audit_logger_table = os.environ['AUDIT_LOGGER_TABLE']
        chargeback_cost_table = os.environ['CHARGEBACK_COST_TABLE']
        log_event_details(event, context)
    
        ## Flow for Retrieve Chargeback
        if event['resource'] == "/saveChargeback":
            return save_chargeback(event, chargeback_table, audit_logger_table)

        ## Flow for Get Chargeback
        else:
            return get_chargeback(event, chargeback_table)
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : {function_name}, Exception : {ex}')

def get_chargeback(event, chargeback_table):
    response_from_dynamo_db = ''
    function_name = 'get_chargeback'
    try:
        logger.info(f'GetChargeback-saveAndGetChargebackDetails- Start : {lambda_name}')
        if "quoteID" in event:
            logger.info(f'{lambda_name}: {function_name} : Creating payload for Get Chargeback operation')
            quote_id = check_property_exist(event, 'quoteID')
            report_type = check_property_exist(event, 'reportType')
            logger.info(f'{lambda_name}: {function_name} : Successfully created payload for retrieval from DynamoDB')
            response_from_dynamo_db = retrieve_from_dynamo_db(quote_id, report_type, chargeback_table)
            logger.debug(f'{lambda_name}: {function_name} : response_from_dynamo_db : {response_from_dynamo_db}')
            end_time = time.perf_counter()
            logger.info(f'{lambda_name}: {function_name} : Duration : {end_time - start_time} seconds')
            logger.info(f'GetChargeback-saveAndGetChargebackDetails- End : {lambda_name}')
            return response_from_dynamo_db
    except:
        logger.error(f'{lambda_name}: {function_name} : Error while checking existing of fields')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_name}: {function_name}: Duration : {end_time - start_time} seconds')
        response = {
                    "statusCode": 500,
                    "body": json.dumps({
                        "status": "ERROR",
                        "code": 500,
                        "message": "Internal Server Error",
                        "error": "Error occurred while creating payload for Get Chargeback",
                        "integrationName": "chargeback"
                    })
                }
        raise chargebackException(response)

def save_chargeback(event, chargeback_table, audit_logger_table):
    response = ''
    function_name = 'save_chargeback'
    eastern = timezone('US/Eastern')  ## US/Eastern
    try:
        logger.info(f'SaveChargeback-saveAndGetChargebackDetails- Start : {lambda_name}')
        logger.info(f'{lambda_name}: {function_name}: Creating payload for Save Chargeback operation')
        loc_dt = datetime.datetime.now(eastern)
        current_time = str(loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
        dynamodb_insert = request_payload(event)
        payload = event['payload']
        producer_code = payload['chargeback']['producerCode']
        report_type = payload['chargeback']['reportType']
        if payload['chargeback']['policyIssued']:
            waived = 'Y'
        else:
            waived = 'N'
        partition_key = payload['chargeback']['quoteID']
        dynamodb_insert['PK'] = partition_key + '#' + report_type
        dynamodb_insert['SK'] = current_time + '#' + 'v0' + '#' + 'ProducerCode' + '#' + producer_code + '#' + report_type
        dynamodb_insert['payload'] = payload
        dynamodb_insert['Waived'] = waived
        if payload['chargeback']['shallOrderFlag']== True:
            dynamodb_insert['chargeUpdatedFlag'] = False
        if payload['chargeback']['shallOrderFlag']== False:
            dynamodb_insert['chargeUpdatedFlag'] = True
        quote_id = event['payload']['chargeback']['quoteID']
        report_type = event['payload']['chargeback']['reportType']
        logger.info(f'{lambda_name}: {function_name}: Successfully created payload for insertion to Dynamo DB')
        
        response = insert_data_dynamo(dynamodb_insert, chargeback_table, quote_id, report_type, current_time, event,
                                            audit_logger_table)
        end_time = time.perf_counter()
        logger.info(f'{lambda_name}: {function_name}: Duration : {end_time - start_time} seconds')
        logger.info(f'SaveChargeback-saveAndGetChargebackDetails- End : {lambda_name}')
        return response
    except:
        logger.error(f'{lambda_name}: {function_name}: Error while saving chargeback')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_name}: {function_name}: Duration : {end_time - start_time} seconds')
        response = {
                    "statusCode": 500,
                    "body": json.dumps({
                        "status": "ERROR",
                        "code": 500,
                        "message": "Internal Server Error",
                        "error": "Error occurred while creating payload for saving chargeback",
                        "integrationName": "chargeback"
                    })
                }
        raise chargebackException(response)

def log_event_details(event, context):
    function_name = 'log_event_details'
    lambda_function_name = context.function_name
    try:
        quote_id = event['quoteID']

        if event['resource'] == "/saveChargeback":
            payload = event['payload']['chargeback']
            if 'producerCode' in payload:
                producer_code = payload['producerCode']
            if 'totalDrivers' in payload:
                total_drivers = payload['totalDrivers']
            else:
                total_drivers = 0
            if 'totalOrderedDrivers' in payload:
                total_ordered_drivers = payload['totalOrderedDrivers']
            else:
                total_ordered_drivers = 0
            if 'orderedDriversForCurrReq' in payload:
                current_ordered_drivers = payload['orderedDriversForCurrReq']
            else:
                current_ordered_drivers = 0
            if 'baseState' in payload:
                base_state = payload['baseState']
            if 'policyIssued' in payload:
                policy_issued = payload['policyIssued']

            eastern = timezone('US/Eastern')  ## US/Eastern
            loc_dt = datetime.datetime.now(eastern)

            logger.append_keys(x_correlation_id=event["correlation_id"], quoteID=quote_id, producerCode=producer_code,
                               totalDrivers=total_drivers, totalOrderedDrivers=total_ordered_drivers,
                               currentOrderedDrivers=current_ordered_drivers, state=base_state,
                               policyIssued=policy_issued, currentTimeStamp=loc_dt)
        
        logger.debug(f'{lambda_name} : {function_name} : Event : {event}')
    except:
        logger.error(f'{lambda_name}: {function_name}: Error reading event details : Event : {event}')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_name}: {function_name}: Duration : {end_time - start_time} seconds')
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "ERROR",
                "code": 500,
                "message": "Internal Server Error",
                "error": "Error occurred while reading event details",
                "integrationName": "chargeback"
            })
        }
        raise chargebackException(response)

def request_payload(event):
    function_name = 'request_payload'
    try:
        dynamodb_insert = {}
        chargeback = event['payload']['chargeback']
        if "correlation_id" in event:
            dynamodb_insert['CorrelationID'] = event['correlation_id']
        if "quoteID" in chargeback:
            dynamodb_insert['QuoteID'] = check_property_exist(chargeback, 'quoteID')
        if "producerCode" in chargeback:
            dynamodb_insert['ProducerCode'] = check_property_exist(chargeback, 'producerCode')
        if "lob" in chargeback:
            dynamodb_insert['LOB'] = check_property_exist(chargeback, 'lob')
        if "baseState" in chargeback:
            dynamodb_insert['BaseState'] = check_property_exist(chargeback, 'baseState')
        if "startDate" in chargeback:
            dynamodb_insert['StartDate'] = check_property_exist(chargeback, 'startDate')
        if "reportType" in chargeback:
            dynamodb_insert['ReportType'] = check_property_exist(chargeback, 'reportType')
        if "quoteDate" in chargeback:
            dynamodb_insert['QuoteDate'] = check_property_exist(chargeback, 'quoteDate')
        if "quoteTime" in chargeback:
            dynamodb_insert['QuoteTime'] = check_property_exist(chargeback, 'quoteTime')
        if "firstOrderDate" in chargeback:
            dynamodb_insert['FirstOrderDate'] = check_property_exist(chargeback, 'firstOrderDate')
        if "orderDate" in chargeback:
            dynamodb_insert['OrderDate'] = check_property_exist(chargeback, 'orderDate')
        if "orderTime" in chargeback:
            dynamodb_insert['OrderTime'] = check_property_exist(chargeback, 'orderTime')
        if "policyIssued" in chargeback:
            dynamodb_insert['PolicyIssued'] = check_property_exist(chargeback, 'policyIssued')
        if "policyIssueDate" in chargeback:
            dynamodb_insert['PolicyIssueDate'] = check_property_exist(chargeback, 'policyIssueDate')
        if "policyIssueTime" in chargeback:
            dynamodb_insert['PolicyIssueTime'] = check_property_exist(chargeback, 'policyIssueTime')
        if "policyNumber" in chargeback:
            dynamodb_insert['PolicyNumber'] = check_property_exist(chargeback, 'policyNumber')
        if "shallOrderFlag" in chargeback:
            dynamodb_insert['shallOrderFlag'] = check_property_exist(chargeback, 'shallOrderFlag')
        if "totalDrivers" in chargeback:
            dynamodb_insert['TotalDrivers'] = check_property_exist(chargeback, 'totalDrivers')
        if "orderedDriversForCurrReq" in chargeback:
            dynamodb_insert['DriversOrderedOn'] = check_property_exist(chargeback, 'orderedDriversForCurrReq')
        if "totalOrderedDrivers" in chargeback:
            dynamodb_insert['TotalOrderedDrivers'] = check_property_exist(chargeback, 'totalOrderedDrivers')
        if "payload" in event:
            dynamodb_insert['payload'] = event['payload']

    except:
        logger.error(f'{lambda_function_name} : {function_name} : Error while checking existing of fields')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_function_name} : {function_name} : Duration : {end_time - start_time} seconds')
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "ERROR",
                "code": 500,
                "message": "Internal Server Error",
                "error": "Error while checking existing of fields",
                "integrationName": "chargeback"
            })

        }
        raise chargebackException(response)
    return dynamodb_insert


## Check for NULL for property
def check_property_exist(event, field):
    if event.get(field) is None:
        return
    else:
        return event[field]


## Function for inserting data into DynamoDB
def insert_data_dynamo(payload_for_dynamodb, chargeback_table, quote_id, report_type, current_time, event, audit_logger_table):
    function_name = 'insert_data_dynamo'
    try:
        mvr_date = ''
        payload = payload_for_dynamodb['payload']
        monthly_report_payload = {}
        table = dynamodb_client.Table(chargeback_table)
        existing_charge = 0
        order_date = ''
        closing_month = ''
        closing_year = ''
        getquery = quote_id + '#' + report_type
        transaction_response = table.query(
            KeyConditionExpression=Key('PK').eq(getquery),
            ScanIndexForward=False,
            Limit=10,
            ConsistentRead=True
        )
        
        if transaction_response['Count'] > 0 and transaction_response['Items'][0]['PolicyIssued'] == True:
            logger.info('Ignore transaction after policy issued')
            res_payload = {
                'statusCode': 200,
                'body': ({
                    "message": "Dynamo DB insertion is restricted after issuance for QuoteID:" + quote_id
                    })
                }
            return res_payload
    
        if transaction_response['Count'] > 0 and transaction_response['Items'][0]['shallOrderFlag'] == False and payload['chargeback']['shallOrderFlag'] == True and payload['chargeback']['policyIssued'] == False:
            payload_for_dynamodb['orderCounterFlag'] = True
        elif transaction_response['Count'] > 0 and transaction_response['Items'][0]['shallOrderFlag'] == True and payload['chargeback']['policyIssued'] == False:
            orderCounterUpdated=0
            for item in transaction_response['Items']:
                if item['orderCounterFlag']== True:
                    orderCounterUpdated= orderCounterUpdated+1
            if(orderCounterUpdated==0):
                payload_for_dynamodb['orderCounterFlag'] = True
            else:
                payload_for_dynamodb['orderCounterFlag'] = False
        elif transaction_response['Count'] == 0 and payload['chargeback']['shallOrderFlag'] == True and payload['chargeback']['policyIssued'] == False:
            payload_for_dynamodb['orderCounterFlag'] = True
        elif transaction_response['Count'] > 0 and 'orderCounterFlag' not in transaction_response['Items'][0] and payload['chargeback']['shallOrderFlag'] == True and payload['chargeback']['policyIssued'] == False:
            payload_for_dynamodb['orderCounterFlag'] = True
        else:
            payload_for_dynamodb['orderCounterFlag'] = False

        if 'firstOrderDate' in payload['chargeback']:
            first_order_date = payload['chargeback']['firstOrderDate']
            if first_order_date != '':
                order_date = datetime.datetime.strptime(first_order_date, '%m/%d/%Y')
                close_date = datetime.datetime(order_date.year, order_date.month, 1) + relativedelta(months=3, days=-1)
                closing_month = str(close_date.strftime("%B"))
                closing_year = str(close_date.strftime("%Y"))

            logger.info(
                f'FirstOrderDate: {first_order_date}, OrderDate: {order_date}, ClosingMonth : {closing_month}, ClosingYear: {closing_year}')

            if "orderedDriversForCurrReq" in payload['chargeback']:
                status_code,total_charge, new_charge, per_report_charge = get_charge(event, payload, transaction_response,audit_logger_table)
                if(status_code!=200 ):
                    return {
                    'statusCode': 500,
                    'body': ({
                        "message": "DynamoDB insertion failed for QuoteID:" + quote_id + " due to decision model call failure"
                    })
                }
                payload_for_dynamodb['totalCharge'] = 0
                payload_for_dynamodb['costPerReport'] = Decimal(per_report_charge)
                payload_for_dynamodb['currentRequestCharge'] = Decimal(total_charge)
        
        else:
            payload_for_dynamodb['totalCharge'] = 0
            payload_for_dynamodb['costPerReport'] = 0
            payload_for_dynamodb['currentRequestCharge'] = 0

        
        if payload['chargeback']['policyIssued'] == True:
            time.sleep(1)

        insert_response = table.put_item(Item=payload_for_dynamodb)

        response_payload = {
            'statusCode': 200,
            'body': ({
                "message": "Dynamo DB insertion is successful for QuoteID:" + quote_id
            })
        }
        shall_order_mvr = str(payload_for_dynamodb['shallOrderFlag'])
        logger.info(f'Dynamo DB insertion is successful for QuoteID : {quote_id} : CurrentTime : { str(datetime.datetime.now(eastern)) } : ShallOrderMVR :{shall_order_mvr}')
        
        if insert_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            payload_for_audit_table = {}
            payload_for_audit_table['PK'] = payload_for_dynamodb['PK']
            payload_for_audit_table['SK'] = payload_for_dynamodb['SK']
            payload_for_audit_table['Event'] = "SaveChargeback/DynamoDB"
            payload_for_audit_table['Status'] = "Success"
            payload_for_audit_table['CorrelationID']= event['correlation_id']
            insert_record_in_audit_table(audit_logger_table, payload_for_audit_table)

    except Exception as ex:
        logger.error(f'{lambda_function_name} : {function_name} : Error while inserting data to dynamo db : Exception : {ex}')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_function_name} : {function_name} : Duration : {end_time - start_time} seconds')
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "ERROR",
                "code": 500,
                "message": "Internal Server Error",
                "error": "Error while inserting data to dynamo db",
                "integrationName": "chargeback"
            })

        }
        raise chargebackException(response)
    
    return response_payload

def insert_record_in_audit_table(audit_logger_table, payload_for_audit_table):
    function_name = 'insert_in_audit_table'
    try:
        audit_table = dynamodb_client.Table(audit_logger_table)
        audit_table.put_item(Item=payload_for_audit_table)
    except Exception as ex:
        logger.exception(f'Exception occurred while inserting in audit table : {lambda_name} : {function_name} : Exception : {ex}')


def get_charge(event, payload, transaction_response,audit_logger_table):
    total_charge = 0
    new_charge = 0
    per_report_charge = 0
    existing_charge = 0
    status_code= 200
    function_name = 'get_charge'
    eastern = timezone('US/Eastern')  ## US/Eastern
    loc_dt = datetime.datetime.now(eastern)
    current_time=str(loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    try:
        if(payload['chargeback']['orderedDriversForCurrReq'] > 0 and payload['chargeback']['firstOrderDate']!= ''):
            chargeback_cost = retrieve_cost(payload['chargeback']['baseState'], payload['chargeback']['reportType'])
            total_charge = payload['chargeback']['orderedDriversForCurrReq'] * chargeback_cost
            per_report_charge = chargeback_cost
            logger.info(f'Total Charge : {total_charge}')
            if transaction_response['Count'] > 0:
                if 'currentRequestCharge' in transaction_response['Items'][0]:
                    existing_charge = transaction_response['Items'][0]['totalCharge']
                new_charge = existing_charge + Decimal(total_charge)
            else:
                new_charge = Decimal(total_charge)
        else:
            if transaction_response['Count'] > 0:
                if 'totalCharge' in transaction_response['Items'][0]:
                    new_charge = transaction_response['Items'][0]['totalCharge']
                if 'costPerReport' in transaction_response['Items'][0]:
                    per_report_charge = transaction_response['Items'][0]['costPerReport']
    except Exception as ex:
        logger.exception(f'Exception occurred while getting charge : {lambda_name} : {function_name} : Exception : {ex}')
    return status_code,total_charge,new_charge,per_report_charge

## Method to retrieve the latest record from dynamoDB for getChargeback
def retrieve_from_dynamo_db(quote_id, reportType, chargeback_table):
    table = dynamodb_client.Table(chargeback_table)
    getquery = quote_id + '#' + reportType
    response = table.query(
        KeyConditionExpression=Key('PK').eq(getquery),
        ScanIndexForward=False,
        Limit=1,
        ConsistentRead=True
    )

    if response['Count'] > 0:
        payload = response['Items'][0]['payload']
        producer_code = payload['chargeback']['producerCode']

        response_payload = {
            "statusCode": 200,
            "body": payload
        }
        return response_payload
    else:
        response_payload = {
            "body": {
                "error": {
                    "message": "Data not available for the requested PK:" + quote_id,
                    "type": "Data Not Found"
                }
            }
        }

        return response_payload

## Method for retrieving the cost from Dynamo DB based on State and reportType
def retrieve_cost(baseState, reportType):
    chargeback_cost_table = os.environ['CHARGEBACK_COST_TABLE']
    table = dynamodb_client.Table(chargeback_cost_table)
    transaction_response = table.query(
            KeyConditionExpression=Key('BaseState').eq(baseState) & Key('ReportType').eq(reportType),
            ScanIndexForward=False,
            Limit=10,
            ConsistentRead=True
        )
    if transaction_response['Count'] > 0:
        cost_per_driver = transaction_response['Items'][0]['Cost']
    return cost_per_driver
    
    

def handle_decimal_type(event_payload):
    if isinstance(event_payload, Decimal):
        if float(event_payload).is_integer():
            return int(event_payload)
        else:
            return float(event_payload)
    raise TypeError
    