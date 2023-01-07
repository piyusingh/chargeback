import json
import boto3
import time
import os
import traceback
from aws_lambda_powertools import Logger


class chargebackException(Exception):
    pass

def lambda_handler(event, context):
    lambda_function_name = ''
    function_name = 'lambda_handler'
    start_time = ''
    response = ''
    lambda_name = os.environ['SERVICE']
    log_level = os.environ['LOG_LEVEL']
    logger = Logger(service=lambda_name)
    logger.setLevel(log_level)
   
    try:
        start_time = time.perf_counter()
        quote_id = event['quoteID']
        correlation_id =event['correlation_id']
        logger.append_keys(x_correlation_id=correlation_id, quoteID=quote_id)
        logger.info('GetChargebackDetails Start :%s',lambda_name)
        end_time = time.perf_counter()
        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        logger.info('GetChargebackDetails End :%s',lambda_name)
        return event
       
    except Exception as e:
        logger.error(': %s : %s : Exception occured while executing lambda', lambda_function_name,function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        response = {
            "statusCode":500,
            "body":json.dumps({
                "status":"ERROR",
                "code":500,
                "message":"Internal Server Error",
                "error":"Exception occured while executing lambda",
                "integrationName":"chargeback"
            })
        }
        raise chargebackException(response)