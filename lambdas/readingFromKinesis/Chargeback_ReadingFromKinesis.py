import json
import boto3
import traceback
from pytz import timezone
from datetime import datetime,date
import base64
import os
import time
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from aws_lambda_powertools import Logger
from botocore.config import Config

lambda_function_name = ''
config = Config(
       retries = {
          'max_attempts': 2,
          'mode': 'standard'
       }
    )
lambda_name = os.environ['SERVICE']
log_level = os.environ['LOG_LEVEL']
logger = Logger(service=lambda_name)
logger.setLevel(log_level)
audit_logger_table=os.getenv("AUDIT_LOGGER_TABLE")
dynamodb_client = boto3.resource('dynamodb', config=config)
chargeback_table = os.getenv('CHARGEBACK_TABLE')
table = dynamodb_client.Table(chargeback_table)

def handle_decimal_type(event_payload):
    if isinstance(event_payload, Decimal):
        if float(event_payload).is_integer():
            return int(event_payload)
        else:
            return float(event_payload)
    raise TypeError

def lambda_handler(event, context):
    eastern = timezone('US/Eastern')## US/Eastern
    loc_dt = datetime.now(eastern)
    date_time = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        start_time = time.perf_counter()
        function_name =  'lambda_handler'
        lambda_function_name = context.function_name
        order_month=''
        order_year=''
        close_date=''
        existing_charge=0
        new_charge=0
        charge_amount=0
        closing_month= ''
        closing_year= ''
        delta_days=0
        gspk_order_dt=''
        previous_bind_ratio=0
        
        event_json = base64.b64decode(event["Records"][0]["kinesis"]["data"])
        event_payload=json.loads(event_json)
        logger.info(f'{lambda_name} : {function_name}',extra ={ 'Event' : event_payload})
        
        quote_id= event_payload['chargeback']['quoteID']
        correlation_id=event_payload['chargeback']['correlationID']
        payload=event_payload['chargeback']        
        
        if('producerCode' in payload): 
            producer_code = payload['producerCode']
        eastern = timezone('US/Eastern')## US/Eastern
        loc_dt = datetime.now(eastern)
        logger.info(f'{lambda_name} : {function_name} : Quote details',
                    extra={'quoteID':quote_id, 'x_correlation_id':correlation_id,'producerCode':producer_code})

        producer_code=  event_payload['chargeback']['producerCode']
        
        
        
        if('firstOrderDate' in event_payload['chargeback'] and event_payload['chargeback']['firstOrderDate']!='' ):
            report_type=event_payload['chargeback']['reportType']
            first_order_date=event_payload['chargeback']['firstOrderDate']
            order_date=datetime.strptime(first_order_date, '%m/%d/%Y')
            date_= datetime(order_date.year, order_date.month, 1) + relativedelta(months=3, days=-1)
            order_month= str(order_date.strftime("%B"))
            order_year= str(order_date.strftime("%Y"))
            close_date= str(date_.strftime("%m/%d/%Y"))
            closing_month=str(date_.strftime("%B"))
            closing_year=str(date_.strftime("%Y"))
            delta_days= (datetime.now() - date_).days
            current_cycle= datetime.now() - relativedelta(months=2)
            current_cycle_month = str(current_cycle.strftime("%B"))
            current_cycle_year = str(current_cycle.strftime("%Y"))
        else:
            return {
                'statusCode': 200,
                'body': json.dumps('Lambda Invocation Successful')
                }
            
            
        producer_code_key=f'v0ProducerCode#{report_type}'
        
        if(delta_days > 0):
            sort_key= 'OrderMonth' + '#'+ current_cycle_month+'#'+'OrderYear'+'#'+ current_cycle_year+'#'+ 'ProducerCode' + '#' + producer_code
            order_date_key= 'OrderMonth' + '#'+ order_month+'#'+'OrderYear'+'#'+ order_year+'#'+ 'ProducerCode' + '#' + producer_code
            producer_code_previous_superset = table.query(
            KeyConditionExpression=Key('PK').eq(producer_code_key) & Key('SK').eq(order_date_key),
            ScanIndexForward=False,
            Limit=1
            )
            
            if producer_code_previous_superset['Count'] > 0:
                previous_bind_counter= producer_code_previous_superset['Items'][0]['BindCounter']
                previous_order_counter= producer_code_previous_superset['Items'][0]['OrderCounter']
                previous_bind_ratio= round((previous_bind_counter / previous_order_counter)*100)
            
        else:
            sort_key= 'OrderMonth' + '#'+ order_month+'#'+'OrderYear'+'#'+ order_year+'#'+ 'ProducerCode' + '#' + producer_code
        
        logger.info(f'{lambda_name} : {function_name} : producer_code_key: {producer_code_key} : sort_key : {sort_key}',
                    extra = {'producerCode':producer_code, 'quoteID':quote_id, 'x_correlation_id':correlation_id})
                    
        transaction_number_key= 'JobNumber' + '#'+ event_payload['chargeback']['quoteID']
        logger.info(f'{lambda_name} : {function_name} : transaction_number_key : {transaction_number_key}',
                    extra = {'producerCode':producer_code, 'quoteID':quote_id, 'x_correlation_id':correlation_id})
        
        
        policyissued= event_payload['chargeback']['policyIssued']
        report_flag= event_payload['chargeback']['shallOrderFlag']

        #Query to check if there any entry with the producer code exists in DB ##PRODUCERCODE-MONTHLY-AGGREGATE
        producer_code_super_set = table.query(
            KeyConditionExpression=Key('PK').eq(producer_code_key) & Key('SK').eq(sort_key),
            ScanIndexForward=False,
            Limit=1
            )
        
        transaction_pk = f'{quote_id}#{report_type}'
        transaction_response = table.query(
                            KeyConditionExpression=Key('PK').eq(transaction_pk),
                            ScanIndexForward=False,
                            Limit=10,
                            ConsistentRead= True
                            )
        print('transactionResponse',transaction_response)
        
        if transaction_response['Count'] > 0 and  'SK' in event_payload:
            existing_charge=0
            transaction_sort_key= event_payload['SK']
           
            for item in transaction_response['Items']:
                if('chargeUpdatedFlag' in item and item['chargeUpdatedFlag']== True and 'totalCharge' in item):
                    existing_charge = item['totalCharge']
                    break
                    
            if 'currentRequestCharge' in event_payload['chargeback']:
                new_charge = existing_charge + Decimal(event_payload['chargeback']['currentRequestCharge'])
            else:
                new_charge = existing_charge
                
            if(event_payload['chargeUpdatedFlag']== False):
                table.update_item(
                    Key={
                        'PK': transaction_pk,
                        'SK': transaction_sort_key
                        },
                    UpdateExpression= "SET totalCharge= :var1,chargeUpdatedFlag= :var2",
                    ExpressionAttributeValues= {
                        ':var1': new_charge,
                        ':var2': True
                    }
                    )
                
                return {
            'statusCode': 200,
            'body': json.dumps('Lambda Invocation Successful')
            }
                
                
        logger.info(f'{lambda_name} : {function_name} : Producer code super set details',
                        extra={'quoteID':quote_id, 'x_correlation_id':correlation_id, 
                        'producerCode':producer_code,'producerCodeSuperSet':producer_code_super_set})   
        
        logger.info(f'{lambda_name} : {function_name} : Transaction response details',
                        extra={'quoteID':quote_id, 'x_correlation_id':correlation_id, 
                        'producerCode':producer_code,'transactionResponse':transaction_response})

        if(producer_code_super_set['Count'] == 0): ## Create  Producer Code Aggregate entry
            return insert_producer_code_row(report_type,event, quote_id, correlation_id, policyissued, report_flag, transaction_response, event_payload, producer_code_key, sort_key, producer_code, table,previous_bind_ratio,delta_days)
        else:
            return update_producer_code_row(report_type,event, quote_id, correlation_id, policyissued, transaction_response, event_payload, producer_code_key, sort_key, producer_code, table, producer_code_super_set,previous_bind_ratio,delta_days)
                
    except Exception as ex:
        logger.exception(f'{lambda_name} : lambda_handler : Exception occurred : {ex}')
        insert_failed_record_in_audit_table(event,report_type)
        return {
            'statusCode': 500,
            'body': json.dumps('Oops something went wrong!')
            }

def insert_producer_code_row(report_type,event, quote_id, correlation_id, policyissued, report_flag, transaction_response, event_payload, producer_code_key, sort_key, producer_code, table,previous_bind_ratio,delta_days):
    function_name = 'insert_producer_code_row'
    logger.info('Insert')
    try:
        logger.info(f'{lambda_name} : {function_name} : first entry of producer code : {producer_code}',
                    extra = {'quoteID':quote_id, 'x_correlation_id':correlation_id, 'producerCode':producer_code})
        if(report_flag==True):
            dynamodb_insert={}
            if(transaction_response['Items'][0]['orderCounterFlag']==True):
                order_counter=1
            else:
                order_counter=0
            bound_charge=0
            if(event_payload['chargeback']['policyIssued']):
                bind_counter=1
                if(previous_bind_ratio<50):
                    bound_charge= transaction_response['Items'][0]['totalCharge']
            else:
                bind_counter=0
            dynamodb_insert['PK']= producer_code_key
            dynamodb_insert['SK']= sort_key
            dynamodb_insert['ProducerCode']= event_payload['chargeback']['producerCode']
            dynamodb_insert['BoundCharges'] = bound_charge
            dynamodb_insert['Charge']= Decimal(event_payload['chargeback']['totalCharge'])
            if(event_payload['chargeback']['policyIssued'] and bound_charge==0):
                dynamodb_insert['TotalAmount']= 0
            elif(event_payload['chargeback']['policyIssued'] and bound_charge>0):
                dynamodb_insert['TotalAmount']= Decimal(event_payload['chargeback']['totalCharge'])- Decimal(bound_charge)
            else:
                dynamodb_insert['TotalAmount']= Decimal(event_payload['chargeback']['totalCharge'])
            dynamodb_insert['OrderCounter']=order_counter
            dynamodb_insert['BindCounter']=bind_counter
            bind_ratio=0
            if(order_counter>0):
                bind_ratio =  (bind_counter / order_counter)*100
            if(bind_ratio>100):
                bind_ratio=100
            dynamodb_insert['BindRatio'] = str(round(bind_ratio))+ '%'
            dynamodb_insert['payload']=event_payload
           
            totalDriverss=''
            totalOrderedDrivers=''
            orderedDriversForCurrReq=''
            baseState=''

            if 'chargeback' in event_payload:
                if 'totalDrivers' in event_payload['chargeback']:
                    totalDrivers = event_payload['chargeback']['totalDrivers']
                if 'totalOrderedDrivers' in event_payload['chargeback']:
                    totalOrderedDrivers = event_payload['chargeback']['totalOrderedDrivers']
                if 'orderedDriversForCurrReq' in event_payload['chargeback']:
                    orderedDriversForCurrReq = event_payload['chargeback']['orderedDriversForCurrReq']
                if 'baseState' in event_payload['chargeback']:
                    baseState = event_payload['chargeback']['baseState']
            
            logger.info(f'{lambda_name} : {function_name} : First producer code row details',
                    extra ={'PK': producer_code_key, 'SK': sort_key, 'bindCounter':bind_counter,
                    'orderCounter':order_counter,'bindRatio':'0%',
                    'charge':Decimal(event_payload['chargeback']['totalCharge']),
                    'producerCode':producer_code, 'quoteID':quote_id, 
                    'x_correlation_id':correlation_id, 'policyIssued' : policyissued,
                    'totalDrivers' : totalDrivers ,'totalOrderedDrivers' : totalOrderedDrivers , 
                    'orderedDriversForCurrReq' : orderedDriversForCurrReq , 'baseState' : baseState })
                    
            table.put_item(Item=dynamodb_insert)
            return {
            'statusCode': 200,
            'body': json.dumps('Lambda Invocation Successful')
            }
    except Exception as ex:
        logger.exception(f'{lambda_name} : {function_name} : Error in inserting producer code row, Exception : {ex}')
        insert_failed_record_in_audit_table(event,report_type)
        return {
            'statusCode': 500,
            'body': json.dumps('Producer code row insert failed')
            }

def update_producer_code_row(report_type,event, quote_id, correlation_id, policyissued, transaction_response, event_payload, producer_code_key, sort_key, producer_code, table, producer_code_super_set,previous_bind_ratio,delta_days):
    function_name = 'update_producer_code_row'
    logger.info('Update')
    try:
        logger.info(f'{lambda_name} : {function_name} : Quote details',
        extra= {'PK': producer_code_key, 'SK': sort_key,'quoteID':quote_id, 
        'x_correlation_id':correlation_id, 'producerCode' : producer_code})

        logger.debug(f'{lambda_name} : {function_name}: event payload : {event_payload}')
        if('BindCounter' in  producer_code_super_set['Items'][0]):
            bind_counter=producer_code_super_set['Items'][0]['BindCounter']
        if('OrderCounter' in  producer_code_super_set['Items'][0]):
            order_counter= producer_code_super_set['Items'][0]['OrderCounter']
        if('BindRatio' in  producer_code_super_set['Items'][0]):
            bind_ratio= producer_code_super_set['Items'][0]['BindRatio']
        if('Charge' in producer_code_super_set['Items'][0]):
            existing_charge =  Decimal(producer_code_super_set['Items'][0]['Charge'])
        if('TotalAmount' in producer_code_super_set['Items'][0]):
            total_producer_charge =  Decimal(producer_code_super_set['Items'][0]['TotalAmount'])
            print('total_producer_charge', total_producer_charge)
        if('BoundCharges' in producer_code_super_set['Items'][0]):
            bound_amount =  Decimal(producer_code_super_set['Items'][0]['BoundCharges'])
        else:
            bound_amount=0
            
        
        if('currentRequestCharge' in event_payload['chargeback']):
            new_charge= Decimal(event_payload['chargeback']['currentRequestCharge'])
            #total_amount=Decimal(producer_code_super_set['Items'][0]['TotalAmount'])
        if('TotalAmount' in producer_code_super_set['Items'][0]):
            producer_amount =  Decimal(producer_code_super_set['Items'][0]['TotalAmount'])
            
        charge_amount= existing_charge+new_charge
        total_amount= producer_amount+new_charge
        new_bind_ratio=0
        if(transaction_response['Count']>0 and 'orderCounterFlag' in transaction_response['Items'][0]):
            if(transaction_response['Items'][0]['orderCounterFlag']==True):
                order_counter=order_counter+1
                new_bind_ratio =  (bind_counter / order_counter)*100
                if(new_bind_ratio>100):
                    new_bind_ratio=100
                bind_ratio = str(round(new_bind_ratio))+ '%'
                logger.info(f'{lambda_name} : {function_name} : Order counter update',
                        extra= {'PK': producer_code_key, 'SK': sort_key,
                        'quoteID':quote_id, 'x_correlation_id':correlation_id, 
                        'producerCode' : producer_code, 'orderCounter' : order_counter,
                        'policyIssued' : policyissued})
                total_charge=0    
            if(event_payload['chargeback']['policyIssued']):
                bind_counter=bind_counter+1
                if(order_counter>0):
                    new_bind_ratio =  (bind_counter / order_counter)*100
                elif(order_counter==0):
                    new_bind_ratio = 100
                if(new_bind_ratio>100):
                    new_bind_ratio=100
                bind_ratio = str(round(new_bind_ratio))+ '%'
                logger.info(f'{lambda_name} : {function_name}: Policy issued details',
                        extra ={'PK': producer_code_key, 'SK': sort_key,
                         'quoteId' : quote_id, 'producerCode':producer_code,
                        'x_correlation_id':correlation_id, 'bindCounter' : bind_counter,
                        'bindRatio' :  bind_ratio, 'policyIssued' : policyissued})
                if('totalCharge' in transaction_response['Items'][0]):
                    total_charge = transaction_response['Items'][0]['totalCharge']
                        
                    if(previous_bind_ratio <50):
                        total_amount = total_producer_charge-total_charge
                        bound_amount = bound_amount+total_charge
                    elif(delta_days<=0):
                        total_amount = total_producer_charge-total_charge
                        bound_amount = bound_amount+total_charge
                    else:
                        total_amount = total_producer_charge
                
        logger.info(f'{lambda_name} : {function_name}, Existing charge amount, {existing_charge}',
                    extra = {'PK': producer_code_key, 'SK': sort_key, 
                    'bindCounter':bind_counter,'orderCounter':order_counter,
                    'bindRatio':bind_ratio,'charge':charge_amount, 'totalAmount' : total_amount, 
                    'quoteId' : quote_id, 'producerCode':producer_code,
                    'x_correlation_id':correlation_id, 'policyIssued' : policyissued})

        totalDrivers=''
        totalOrderedDrivers=''
        orderedDriversForCurrReq=''
        baseState=''

        if 'chargeback' in event_payload:
            if 'totalDrivers' in event_payload['chargeback']:
                totalDrivers = event_payload['chargeback']['totalDrivers']
            if 'totalOrderedDrivers' in event_payload['chargeback']:
                totalOrderedDrivers = event_payload['chargeback']['totalOrderedDrivers']
            if 'orderedDriversForCurrReq' in event_payload['chargeback']:
                orderedDriversForCurrReq = event_payload['chargeback']['orderedDriversForCurrReq']
            if 'baseState' in event_payload['chargeback']:
                baseState = event_payload['chargeback']['baseState']
                    
        logger.info(f'{lambda_name} : {function_name}, New charge amount, {new_charge}',
                    extra = {'PK': producer_code_key, 'SK': sort_key,
                    'bindCounter':bind_counter,'orderCounter':order_counter,
                    'bindRatio':bind_ratio,'charge':charge_amount, 'totalAmount' : total_amount, 
                    'quoteId' : quote_id, 'producerCode':producer_code,
                    'x_correlation_id':correlation_id, 'policyIssued' : policyissued,
                    'totalDrivers' : totalDrivers ,'totalOrderedDrivers' : totalOrderedDrivers , 
                    'orderedDriversForCurrReq' : orderedDriversForCurrReq , 'baseState' : baseState})
        
        
        table.update_item(
                    Key={
                        'PK': producer_code_key,
                        'SK': sort_key
                        },
                    UpdateExpression= "SET BindRatio= :var1, Charge= :var2, TotalAmount= :var3, OrderCounter=:var4,BindCounter=:var5, payload=:var6, BoundCharges=:var7",
                    ExpressionAttributeValues= {
                        ':var1': bind_ratio,
                        ':var2': charge_amount,
                        ':var3': total_amount,
                        ':var4': order_counter,
                        ':var5': bind_counter,
                        ':var6': event_payload,
                        ':var7': bound_amount
                    }
                    )
        return {
                'statusCode': 200,
                'body': json.dumps('Lambda Invocation Successful')
                }
    except Exception as ex:
        logger.exception(f'{lambda_name} : {function_name} : Error in updating producer code row, Exception : {ex}')
        insert_failed_record_in_audit_table(event,report_type)
        return {
            'statusCode': 500,
            'body': json.dumps('Producer code row update failed')
            }
            
def insert_failed_record_in_audit_table(event,report_type):
    try:
        audit_table= dynamodb_client.Table(audit_logger_table)
        event_json = base64.b64decode(event["Records"][0]["kinesis"]["data"])
        event_payload=json.loads(event_json)
        eastern = timezone('US/Eastern')## US/Eastern
        loc_dt = datetime.now(eastern)
        date_time = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        quote_id= event_payload['chargeback']['quoteID']
        correlation_id=event_payload['chargeback']['correlationID']
        error_payload_audit={}
        error_payload_audit['PK']=f'{report_type}#Failure'
        error_payload_audit['SK']=str(date_time)
        error_payload_audit['Status']= 'Failure'
        error_payload_audit['Event']= 'SaveViolation/ReadingFromKinesis'
        error_payload_audit['Payload']= event_payload
        error_payload_audit['Input']=  json.dumps(event)
        error_payload_audit['QuoteId']=quote_id
        error_payload_audit['CorrelationID']= correlation_id
        audit_table.put_item(Item=error_payload_audit)
    except Exception as ex:
        logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')