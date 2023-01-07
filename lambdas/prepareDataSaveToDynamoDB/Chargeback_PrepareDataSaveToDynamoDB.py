import json
import boto3
import time
import traceback
import os
from aws_lambda_powertools import Logger


class chargebackException(Exception):
    pass

def lambda_handler(event, context):
    lambda_function_name = ''
    function_name =  'lambda_handler'
    start_time = ''
    lambda_name = os.environ['SERVICE']
    log_level = os.environ['LOG_LEVEL']
    logger = Logger(service=lambda_name)
    logger.setLevel(log_level)
   
    # Reading the attributes from payload
    try:
        start_time = time.perf_counter()
        input_to_dynamodb = {}
        input_event=event['body']['chargeback']
        quote_id = input_event['quoteID']
        logger.append_keys(x_correlation_id=event["correlation_id"],quoteID=quote_id)
        logger.info('SaveData-saveChargebackDetails- Start :%s',lambda_name)
        if "quoteID" in input_event:
            quote_id = event['body']['chargeback']['quoteID']
            input_to_dynamodb['quoteID'] = quote_id
        if "producerCode" in input_event:
            producer_code = event['body']['chargeback']['producerCode']
            input_to_dynamodb['producerCode'] = producer_code
        if "lob" in input_event:
            lob = event['body']['chargeback']['lob']
            input_to_dynamodb['lob'] = lob
        if "baseState" in input_event:
            base_state = event['body']['chargeback']['baseState']
            input_to_dynamodb['baseState'] = base_state
        if "startDate" in input_event:
            start_date = event['body']['chargeback']['startDate']
            input_to_dynamodb['startDate'] = start_date
        if "reportType" in input_event:
            report_type = event['body']['chargeback']['reportType']
            input_to_dynamodb['reportType'] = report_type
        if "orderDate" in input_event:
            order_date = event['body']['chargeback']['orderDate']
            input_to_dynamodb['orderDate'] = order_date
        if "orderTime" in input_event:
            order_time = event['body']['chargeback']['orderTime']
            input_to_dynamodb['orderTime'] = order_time
        if "quoteDate" in input_event:
            quote_date = event['body']['chargeback']['quoteDate']
            input_to_dynamodb['quoteDate'] = quote_date
        if "quoteTime" in input_event:
            quote_time = event['body']['chargeback']['quoteTime']
            input_to_dynamodb['quoteTime'] = quote_time
        if "policyIssued" in input_event:
            policy_issued = event['body']['chargeback']['policyIssued']
            input_to_dynamodb['policyIssued'] = policy_issued
        if "policyIssueDate" in input_event:
            policy_issue_date = event['body']['chargeback']['policyIssueDate']
            input_to_dynamodb['policyIssueDate'] = policy_issue_date
        if "policyIssueTime" in input_event:
            policy_issue_time = event['body']['chargeback']['policyIssueTime']
            input_to_dynamodb['policyIssueTime'] = policy_issue_time
        if "policyNumber" in input_event:
            policy_number = event['body']['chargeback']['policyNumber']
            input_to_dynamodb['policyNumber'] = policy_number
        if "shallOrderflag" in input_event:
            shall_order_flag = event['body']['chargeback']['shallOrderflag']
            input_to_dynamodb['shallOrderflag'] = shall_order_flag
        if "tracker" in input_event:
            tracker = event['body']['chargeback']['tracker']
            input_to_dynamodb['tracker'] = tracker
        if "totalDriver" in input_event:
            total_driver = event['body']['chargeback']['totalDriver']
            input_to_dynamodb['totalDriver'] = total_driver
        input_to_dynamodb['payload'] = event['body']
        input_to_dynamodb['resource'] = event['resource']
        input_to_dynamodb['correlation_id']=event['correlation_id']
        end_time = time.perf_counter()
        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        logger.info('SaveData-saveChargebackDetails- End :%s',lambda_name)
        return input_to_dynamodb
       
    except:
        logger.error(': %s : %s : Internal error might have happened in State Machine', lambda_function_name, function_name)
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(': %s : %s : Duration : %s seconds', lambda_function_name, function_name, end_time - start_time)
        msg = {
            'status': 500,
            'response': "Oops, something went wrong ! Please reach out to the technical team for assistance."
        }
        raise chargebackException(msg)