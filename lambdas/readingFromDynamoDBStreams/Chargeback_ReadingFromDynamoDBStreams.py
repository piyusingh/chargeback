import json
import boto3
from aws_lambda_powertools import Logger
import os
import traceback
import time
from datetime import datetime
from datetime import date
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from boto3.dynamodb.types import TypeDeserializer,TypeSerializer
from boto3.dynamodb.conditions import Key
from pytz import timezone

kinesis_client = boto3.client("kinesis")
payload_for_audit_table={}
error_payload_audit_table={}
dynamoDB_client = boto3.resource('dynamodb')
lambda_name = os.environ['SERVICE']
log_level = os.environ['LOG_LEVEL']
audit_logger_table=os.environ['AUDIT_LOGGER_TABLE']
logger = Logger(service=lambda_name)
logger.setLevel(log_level)

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}
    
def handle_decimal_type(event_payload):
    if isinstance(event_payload, Decimal):
        if float(event_payload).is_integer():
            return int(event_payload)
        else:
            return float(event_payload)
    raise TypeError

def lambda_handler(event, context):
    function_name =  'lambda_handler'
    lambda_function_name = context.function_name
    new_image = {}
    try:
        start_time = time.perf_counter()
        for record in event['Records']:
            if(record['eventName']== 'REMOVE'):
                logger.info('Ignore Delete event')
            elif('payload' not in event['Records'][0]['dynamodb']['NewImage']):
                logger.info('Ignore event')
            else:
                new_image= event['Records'][0]['dynamodb']['NewImage']
                if 'M' in new_image['payload'].keys():
                    mapdata = new_image['payload']['M']
                    final_mapdata = from_dynamodb_to_json(mapdata)
                    event_payload=final_mapdata
                    report_type = event_payload['chargeback']['reportType']
                    new_image_pk = f'v0ProducerCode#{report_type}'
                    if(new_image['PK']['S']==new_image_pk):
                        logger.info('Ignore producer code event')
                        return
                logger.info(f'{lambda_name} : {function_name} : Transaction new_image event : {new_image}')
                return put_record_to_kinesis(new_image, record, start_time,event)
    except:
        logger.error(f'{lambda_name} : {function_name} : Exception occured while passing record to Kinesis')
        traceback.print_exc()
        end_time = time.perf_counter()
        logger.info(f'{lambda_name} : {function_name} : Duration : {end_time - start_time} seconds')
        insert_failed_record_in_audit_table(event,new_image)
        return{
            'statusCode': 500,
            'body': json.dumps('Oops something went wrong!')
            }

def put_record_to_kinesis(new_image, record, start_time,event):
    function_name =  'put_record_to_kinesis'
    try:
        producer_total_charge=0
        producer_code_charge=0
        current_request_charge=0
        cost_per_report=0
        transaction_total_charge=0
        if 'M' in new_image['payload'].keys():
            mapdata = new_image['payload']['M']
            final_mapdata = from_dynamodb_to_json(mapdata)
            event_payload=final_mapdata
        if 'Charge' in new_image:
            producer_total_charge=new_image['Charge']['N']
        if 'currentRequestCharge' in new_image:
            current_request_charge=new_image['currentRequestCharge']['N']
        if 'totalCharge' in new_image:
            transaction_total_charge= new_image['totalCharge']['N']
        if 'costPerReport' in new_image:
            cost_per_report=new_image['costPerReport']['N']
        if 'ReportType' in new_image:
            report_type=new_image['ReportType']['S']

        event_payload['PK']= new_image['PK']['S']
        event_payload['SK']= new_image['SK']['S']
        event_payload['chargeUpdatedFlag']= new_image['chargeUpdatedFlag']['BOOL']
        event_payload['chargeback']['currentRequestCharge']=current_request_charge
        event_payload['chargeback']['charge']= producer_total_charge
        event_payload['chargeback']['totalCharge']= transaction_total_charge
        event_payload['chargeback']['costPerReport']= cost_per_report
        event_payload['chargeback']['reportType'] = report_type
        
        correlation_id = new_image['CorrelationID']['S']
        quote_id= event_payload['chargeback']['quoteID']
        
        logger.append_keys(quoteID=quote_id,  correlation_id= correlation_id)   

        event_payload['chargeback']['correlationID']=correlation_id
        producer_code= str(event_payload['chargeback']['producerCode'])
        if('firstOrderDate' in event_payload['chargeback'] and event_payload['chargeback']['firstOrderDate']!=''):
            first_order_date= datetime.strptime(event_payload['chargeback']['firstOrderDate'], '%m/%d/%Y')
        else:
            first_order_date=''
        
        body = json.dumps(event_payload, default=handle_decimal_type)
        ##Passing msg to kinesis - define the stream name and partition key
        logger.info(event_payload)
        logger.info(f'{lambda_name} : {function_name} :producer code : {producer_code}')
        
        kinesis_response = kinesis_client.put_record(
        StreamName= os.environ['STREAM_NAME'],
        Data = body,
        PartitionKey= producer_code
        )
        logger.info(f'{lambda_name} : {function_name} : Kinesis response : {kinesis_response}')
        
        return insert_record_in_audit_table(quote_id, event_payload, kinesis_response, new_image, correlation_id, record,event)
    except Exception as ex:
        logger.error(f'{lambda_name} : {function_name} : Exception occured while putting record in Kinesis')
        traceback.print_exc()
        end_time = time.perf_counter()
        insert_failed_record_in_audit_table(event,new_image)
        
        logger.info(f'{lambda_name} : {function_name} : Duration : {end_time - start_time} seconds')
        return{
            'statusCode': 500,
            'body': json.dumps('Oops something went wrong!')
            }

def insert_record_in_audit_table(quote_id, event_payload, kinesis_response, new_image, correlation_id, record,event):
    function_name = 'insert_record_in_audit_table'
    try:
        eastern = timezone('US/Eastern')## US/Eastern
        loc_dt = datetime.now(eastern)
        timestamp = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    
        payload_for_audit_table['PK']=quote_id
        payload_for_audit_table['SK']= str(timestamp)
        payload_for_audit_table['Status']='Success'
        payload_for_audit_table['Event']= "SaveChargeback/Kinesis"
        payload_for_audit_table['CorrelationID']= correlation_id
        
        audit_table= dynamoDB_client.Table(audit_logger_table)
        logger.info(kinesis_response['ResponseMetadata']['HTTPStatusCode'])
        if(new_image['PK']['S']==quote_id and record['eventName']== 'INSERT'):
            if(kinesis_response['ResponseMetadata']['HTTPStatusCode']== 200):
                logger.info(f'{lambda_name} : {function_name} :correlation_id : {correlation_id}')
                audit_table.put_item(Item=payload_for_audit_table)
                logger.info(f'{lambda_name} : {function_name} : Sucessfully uploaded record to Kinesis')
                return{
                'statusCode': 200,
                'body': json.dumps('Successful insertion into kinesis')
                }
            else:
                insert_failed_record_in_audit_table(event, new_image)
                return{
                'statusCode': 500,
                'body': json.dumps('Error during insertion into kinesis')
                }
    except:
        logger.error(f'{lambda_name} : {function_name} : Exception occured while putting record to Audit Table')

def insert_failed_record_in_audit_table(event, new_image):
    try:
        eastern = timezone('US/Eastern')## US/Eastern
        loc_dt = datetime.now(eastern)
        timestamp = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        quote_id=''
        event_payload = ''
        correlation_id = ''
        if 'payload' in new_image.keys() and 'M' in new_image['payload'].keys():
            mapdata = new_image['payload']['M']
            final_mapdata = from_dynamodb_to_json(mapdata)
            event_payload=final_mapdata
            quote_id= event_payload['chargeback']['quoteID']
            correlation_id = new_image['CorrelationID']['S']
        error_payload_audit_table={}
        error_payload_audit_table['PK']=f"{event_payload['chargeback']['reportType']}#Failure"
        error_payload_audit_table['SK']= str(timestamp)
        error_payload_audit_table['Status']= 'Failure'
        error_payload_audit_table['Event']= 'SaveChargeback/Kinesis'
        error_payload_audit_table['Payload']=event_payload
        error_payload_audit_table['Input']= json.dumps(event, default=handle_decimal_type)
        error_payload_audit_table['QuoteId']=quote_id   
        error_payload_audit_table['CorrelationID']= correlation_id
        audit_table= dynamoDB_client.Table(audit_logger_table)
        audit_table.put_item(Item=error_payload_audit_table)
        
    except Exception as ex:
        logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')