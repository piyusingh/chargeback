import json
import boto3
import os
from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key
from decimal import Decimal

lambda_name = os.environ['SERVICE']
log_level = os.environ['LOG_LEVEL']
logger = Logger(service=lambda_name)
logger.setLevel(log_level)
dynamodb_client = boto3.resource('dynamodb')
chargeback_table = os.getenv('CHARGEBACK_TABLE')
table = dynamodb_client.Table(chargeback_table)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def lambda_handler(event, context):
    # TODO implement
    transaction_response = ''
    bind_counter = ''
    order_counter = ''
    bind_ratio = ''
    query_parameters = ['ordermonth','orderyear','producercode', 'reporttype']
    try:
        final_response = {}
        event_data = {'ordermonth':'','orderyear':'','producercode':'','reporttype':''}
        if event and 'queryStringParameters' in event.keys():
            queryStringParameters = event['queryStringParameters']
            print(queryStringParameters)
            for param in query_parameters:
                if param in queryStringParameters.keys():
                    event_data[param] = queryStringParameters[param]
        logger.info(f'{lambda_name}, event_data : {event_data}')
        report_type = event_data['reporttype']
        producer_code_key=f'v0ProducerCode#{report_type}'
        order_month = event_data['ordermonth']
        order_year = event_data['orderyear']
        agency_code = event_data['producercode']
        sort_key = 'OrderMonth' + '#'+ order_month+'#'+'OrderYear'+'#'+ order_year+'#'+ 'ProducerCode' + '#' + agency_code
        transaction_response = table.query(
            KeyConditionExpression=Key('PK').eq(producer_code_key) & Key('SK').eq(sort_key),
            ScanIndexForward=False,
            Limit=10,
            ConsistentRead=True
        )
        if transaction_response['Count'] > 0:
            bind_counter= Decimal(transaction_response['Items'][0]['BindCounter'])
            order_counter= Decimal(transaction_response['Items'][0]['OrderCounter'])
            bind_ratio= transaction_response['Items'][0]['BindRatio']
            payload = transaction_response['Items'][0]['payload']
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "orderCounter": int(order_counter),
                "bindCounter" : int(bind_counter),
                "bindRatio": bind_ratio
            }, cls = DecimalEncoder)
        }
        
        return response
    except Exception as ex:
        logger.exception(f'Exception occurred in {lambda_name} : lambda_handler, Exception : {ex}')
        final_response = {
            "body": json.dumps({
            "message": "Oops, something went wrong ! Please reach out to the technical team for assistance.",
            })
        }