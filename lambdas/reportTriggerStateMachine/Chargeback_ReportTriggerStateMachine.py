import json
import boto3
from aws_lambda_powertools import Logger
import os
from http import HTTPStatus
import datetime
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta, datetime
client = boto3.client('stepfunctions')
def lambda_handler(event, context):
    lambda_name = os.environ['SERVICE']
    logger = Logger(service=lambda_name)
    response = ''
    query_parameters = ['start_date','end_date','report_type']
    try:
        event_data = {'start_date':'','end_date':'','report_type':''}

        if event and 'queryStringParameters' in event.keys():
            queryStringParameters = event['queryStringParameters']
            for param in query_parameters:
                if param in queryStringParameters.keys():
                    event_data[param] = queryStringParameters[param]
        
        logger.info(f'{lambda_name}, event_data : {event_data}')
        
        start = datetime.strptime(event_data['start_date'], '%Y%m%d')
        end = datetime.strptime(event_data['end_date'], '%Y%m%d')
        
        order_month = int(start.strftime('%m'))
        order_year = int(start.strftime('%Y'))
        
        cycle_close_date = datetime(order_year,order_month , 1) + relativedelta(months=3, days=-1)
        cycle_close_month = int(cycle_close_date.strftime('%m'))
        cycle_close_year = int(cycle_close_date.strftime('%Y'))
        
        logger.append_keys(orderYearMonth = f'{order_year}-{order_month:02d}', 
                        cycleCloseYearMonth = f'{cycle_close_year}-{cycle_close_month:02d}',
                        startDate = event_data['start_date'], endDate = event_data['end_date'])
        
        input = { "report_type": event_data['report_type'], "record_year":order_year, "record_month":order_month, "cycle_close_year":cycle_close_year, "cycle_close_month":cycle_close_month, "start_date":event_data['start_date'], "end_date":event_data['end_date']}

        payload = {
            "input" : {
                "year_month_detail": input
            }
        }
        logger.info(f'{lambda_name} input payload to state machine : {payload} ')
        step_function_response=trigger_stepfunction(os.environ['STATE_MACHINE_ARN'],payload)
        
        response = {
            "statusCode":step_function_response['ResponseMetadata']['HTTPStatusCode'],
            "body": json.dumps({
            "message": "chargeback monthly report state machine triggered",
            })
        }
        
        logger.info(f'{lambda_name} executed for OrderDate : {order_year}_{order_month} and CycleCloseDate : {cycle_close_year}_{cycle_close_month}')
        logger.info(f'chargeback monthly report state machine triggered: {step_function_response}')
        logger.info(f'{lambda_name}:Successful Event')
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : lambda_handler, Exception : {ex}')
        response = {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "body": json.dumps({
            "message": "Oops, something went wrong ! Please reach out to the technical team for assistance.",
            })
        }
    return response

def trigger_stepfunction(arn,payload):
    step_function_response = client.start_execution(
            stateMachineArn = arn,
            input = json.dumps(payload)
        )
    return step_function_response