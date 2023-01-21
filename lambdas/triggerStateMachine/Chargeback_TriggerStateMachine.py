import boto3
import json
import os
import time
import traceback
from datetime import datetime
from datetime import date
from aws_lambda_powertools import Logger
from botocore.config import Config
from pytz import timezone

config = Config(
    retries = dict(
        max_attempts = 2
    )
)

sf_client = boto3.client('stepfunctions',config=config)
dynamoDB_client = boto3.resource('dynamodb')

# Lambda Handler method beginning point of Lambda execution
def lambda_handler(event, context):
    lambda_name = os.environ['SERVICE']
    log_level = os.environ['LOG_LEVEL']
    logger = Logger(service=lambda_name)
    logger.setLevel(log_level)
    lambda_function_name = ''
    function_name =  'lambda_handler'
    de_response = ''
    my_state_machine_arn = ''
    start_time = ''
    end_time = ''
    response = ''
    resource=''
    request_payload=''
    audit_logger_table=os.environ['AUDIT_LOGGER_TABLE']
    eastern = timezone('US/Eastern')## US/Eastern
    loc_dt = datetime.now(eastern)
    timestamp = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    try:
        start_time = time.perf_counter()
        if('x-correlation-id' in event['headers']):
            correlation_id = event['headers']['x-correlation-id']
        else:
            correlation_id = context.aws_request_id
        if(event['resource']=="/getChargeback"):
            request_payload= json.loads(event['body'])
            quote_id= request_payload['quoteID']
            report_type = request_payload['reportType']
            resource = event['resource']
            body = event['body']
            source = "APIGateway"
           
        elif(event['resource']=="/saveChargeback"):
            request_payload= json.loads(event['body'])
            quote_id=request_payload['chargeback']['quoteID']
            report_type = request_payload['chargeback']['reportType']
            resource = event['resource']
            body = event['body']
            source = "APIGateway"
           
        payload = {
            'resource' : resource,
            'source' : source,
            'body' : body,
            'correlation_id' :correlation_id
        }
       
        logger.append_keys(x_correlation_id=correlation_id, quoteID=quote_id)
        lambda_function_name = context.function_name
        logger.debug(': %s : %s : Event : %s', lambda_function_name ,function_name, event)
        my_state_machine_arn = os.environ['STATE_MACHINE_ARN']
        logger.info(': %s : %s : my_state_machine_arn : %s ' , lambda_function_name, function_name, my_state_machine_arn)
    except Exception as e:
        logger.error(': %s : %s :  Error occurred while creating chargeback step function payload', lambda_function_name, function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        response = {
            "statusCode":500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Error occurred while creating chargeback step function payload",
                "integrationName":"chargeback"
            })
        }
        try:
            if(resource=='/saveChargeback'):
                audit_table= dynamoDB_client.Table(audit_logger_table)
                payload_for_audit_table={}
                payload_for_audit_table['PK']=f'{report_type}#Failure'
                payload_for_audit_table['SK']= str(timestamp)
                payload_for_audit_table['Status']='Failure'
                payload_for_audit_table['Event']= "SaveChargeback/DynamoDB"
                payload_for_audit_table['Payload']=request_payload
                payload_for_audit_table['CorrelationID']=correlation_id
                payload_for_audit_table['Input']= event
                payload_for_audit_table['QuoteId']=request_payload['chargeback']['quoteID']
                audit_table.put_item(Item=payload_for_audit_table)
        except Exception as ex:
            logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')
        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        return response
       
    try:
        response = invoke_state_machine(my_state_machine_arn, payload)
        logger.info(': %s : %s : StartExecution API Response : %s', lambda_function_name, function_name, response)

        de_response = describe_state_machine_execution(response['executionArn'])

        while de_response["status"] == "RUNNING":
            time.sleep(2)
            de_response = describe_state_machine_execution(response['executionArn'])

        logger.info(': %s : %s : DescribeExecution API Response : %s', lambda_function_name, function_name, de_response)
    except Exception as e:
        logger.error(': %s : %s : Exception occured while invoking Step function', lambda_function_name, function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        response = {
            "statusCode": 500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Exception occured while invoking step function",
                "integrationName":"chargeback"
            })
        }
        try:
            if(resource=='/saveChargeback'):
                audit_table= dynamoDB_client.Table(audit_logger_table)
                payload_for_audit_table={}
                payload_for_audit_table['PK']=f'{report_type}#Failure'
                payload_for_audit_table['SK']= str(timestamp)
                payload_for_audit_table['Status']='Failure'
                payload_for_audit_table['Event']= "SaveChargeback/DynamoDB"
                payload_for_audit_table['Payload']=request_payload
                payload_for_audit_table['CorrelationID']=correlation_id
                payload_for_audit_table['Input']= event
                payload_for_audit_table['QuoteId']=request_payload['chargeback']['quoteID']
                audit_table.put_item(Item=payload_for_audit_table)
        except Exception as ex:
            logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')

        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        return response

    try:
        response = json.loads(de_response['output'])
        api_response = {
            "statusCode" : response['statusCode'],
            "body":json.dumps(response['body'])
            }
        end_time = time.perf_counter()
        logger.info('Successful Event : %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        return api_response
    except:
        logger.error('Failed Event : %s : %s : Internal error might have happened in State Machine', lambda_function_name, function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        response = {
            "statusCode":500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Internal error might have happened in State Machine",
                "integrationName":"chargeback"
            })
        }
        try:
            if(resource=='/saveChargeback'):
                audit_table= dynamoDB_client.Table(audit_logger_table)
                payload_for_audit_table={}
                payload_for_audit_table['PK']=f'{report_type}#Failure'
                payload_for_audit_table['SK']= str(timestamp)
                payload_for_audit_table['Status']='Failure'
                payload_for_audit_table['Event']= "SaveChargeback/DynamoDB"
                payload_for_audit_table['Payload']=request_payload
                payload_for_audit_table['CorrelationID']=correlation_id
                payload_for_audit_table['Input']= event
                payload_for_audit_table['QuoteId']=request_payload['chargeback']['quoteID']
                audit_table.put_item(Item=payload_for_audit_table)
        except Exception as ex:
            logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')

        logger.info('%s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        return response
       


## Trigger state machine and getexecution ARN from Step function
def invoke_state_machine(state_machine_arn, event):
    response = sf_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(event)
    )
    return response

 ## Fetching each Step response of state machine
def describe_state_machine_execution(arn):
    response = sf_client.describe_execution(
        executionArn=arn
    )
    return response