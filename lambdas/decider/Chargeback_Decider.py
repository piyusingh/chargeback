import json
import boto3
import os
import time
import traceback
from aws_lambda_powertools import Logger


# Global Variables
sf_client = boto3.client('stepfunctions')

class chargebackException(Exception):
    pass

## Beginning of lambda execution

def lambda_handler(event, context):
    lambda_function_name = ''
    function_name =  'lambda_handler'
    start_time = ''
    response = ''
    lambda_name = os.environ['SERVICE']
    log_level = os.environ['LOG_LEVEL']
    logger = Logger(service=lambda_name)
    logger.setLevel(log_level)
   
       
    try:
        start_time = time.perf_counter()
        resource = event['resource']
        payload = json.loads(event['body'])
        source = event['source']
        correlation_id=event['correlation_id']
        if(resource=="/getchargeback"):
            quote_id=payload['quoteID']
            logger.append_keys(x_correlation_id=correlation_id, quoteID=quote_id)
            logger.info('getchargeback :%s',lambda_name)
            get_chargeback_request ={
            'path': resource,
            'requestbody': payload,
            'source' : source,
            'correlation_id' : correlation_id
            }
            request_payload = get_chargeback(get_chargeback_request)
            logger.debug(': %s : %s : Payload : %s', lambda_function_name, function_name, request_payload)
            end_time = time.perf_counter()       
            logger.info('Successful Event : %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)  
            return request_payload
        else:
            quote_id=payload['chargeback']['quoteID']
            logger.append_keys(x_correlation_id=correlation_id, quoteID=quote_id)
            logger.info('SaveChargeback :%s',lambda_name)
            save_chargeback_request = {
            'path' : resource,
            'requestbody' : payload,
            'source' : source,
            'correlation_id' : correlation_id
            }
            request_payload = save_chargeback(save_chargeback_request)
            logger.debug(': %s : %s : Payload : %s', lambda_function_name, function_name, request_payload)
            end_time = time.perf_counter()       
            logger.info('Successful Event: %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)  
            return request_payload
    except:
        logger.error(': %s : %s : Exception occured since ["Body"] is not available in payload data', lambda_function_name, function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info('Failed : %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        response = {
            "status": 500,
            "response": "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        raise chargebackException(response)
   
   
##   Request for Step Function {'PK' : 251288, 'detail-type' : '/getchargeback'}
def get_chargeback(get_chargeback_request):
    quote_id = get_chargeback_request['requestbody']['quoteID']  ## The partition key for retrieving the details of violation from dynamo DB
    resource = get_chargeback_request['path']  ## This is to identify which route in Step Function should be invoked
    source = get_chargeback_request['source']
    correlation_id=get_chargeback_request['correlation_id']
    input_to_step_function = {
        'quoteID': quote_id,
        'resource': resource,
        'source' : source,
        'correlation_id' : correlation_id
    }
    return input_to_step_function
   
def save_chargeback(save_chargeback_request):
    request = save_chargeback_request['requestbody']
    resource = save_chargeback_request['path']## This is to identify which route in Step Function should be invoked
    source = save_chargeback_request['source']
    correlation_id=save_chargeback_request['correlation_id']
    input_to_step_function = {
        'body': request,
        'resource': resource,
        'source' : source,
        'correlation_id' : correlation_id
        }
    return input_to_step_function
