import json
import boto3
from aws_lambda_powertools import Logger
import os
import traceback
import time
from datetime import datetime,date
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from boto3.dynamodb.types import TypeDeserializer,TypeSerializer
from boto3.dynamodb.conditions import Key
import pandas as pd
import s3fs
import traceback
from pytz import timezone
import base64
import pyarrow as pa
import pyarrow.parquet as pq

payload_for_audit_table={}
error_payload_audit_table={}
dynamoDB_client = boto3.resource('dynamodb')
lambda_name = os.environ['SERVICE']
log_level = os.environ['LOG_LEVEL']
audit_logger_table=os.environ['AUDIT_LOGGER_TABLE']
logger = Logger(service=lambda_name)
logger.setLevel(log_level)
s3_bucket=os.environ['S3_BUCKET']


s3 = boto3.resource("s3")

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
		logger.info(event['Records'])
		start_time = time.perf_counter()
		for record in event['Records']:
			if(record['eventName']== 'REMOVE'):
				logger.info('Ignore Delete event')
			#elif('payload' not in record['dynamodb']['NewImage']):
			#    logger.info('Ignore event')
			else:
				new_image= record['dynamodb']['NewImage']
				primary_key=new_image['PK']['S']
				secondary_key=new_image['SK']['S']
				logger.append_keys(pk = primary_key, sk = secondary_key )
				#correlation_id= new_image['CorrelationID']['S']
				correlation_id=''
				logger.info(f'{lambda_name} : {function_name} : Transaction new_image event : {new_image}',extra={'x_correlation_id':correlation_id})
				put_record_to_S3(event,new_image)
				logger.info(f'{lambda_name} : {function_name} : Lambda Invocation Successful' )
		return{
			'statusCode': 200,
			'body': json.dumps('Data uploaded to s3!')
			}
	except Exception as ex:
		logger.error(f'{lambda_name} : {function_name} : Exception occured while passing record to S3, Exception : {ex}')
		traceback.print_exc()
		end_time = time.perf_counter()
		logger.info(f'{lambda_name} : {function_name} : Duration : {end_time - start_time} seconds')
		insert_failed_record_in_audit_table(event, new_image)
		return{
			'statusCode': 500,
			'body': json.dumps('Oops something went wrong!')
			}
			
def put_record_to_S3(event,new_image):
	function_name =  'put_record_to_S3'
	producer_total_charge=0
	producer_code_charge=0
	current_request_charge=0
	cost_per_report=0
	reportType = ''
	transaction_total_charge=0
	eastern = timezone('US/Eastern')## US/Eastern
	loc_dt = datetime.now(eastern)
	date_time = loc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
	create_dttime = date_time
	if 'M' in new_image['payload'].keys():
		mapdata = new_image['payload']['M']
		final_mapdata = from_dynamodb_to_json(mapdata)
		event_payload=final_mapdata
	if 'currentRequestCharge' in new_image:
		current_request_charge = new_image['currentRequestCharge']['N']
		event_payload['chargeback']['currentRequestCharge']= current_request_charge
	if 'totalCharge' in new_image:
		transaction_total_charge= new_image['totalCharge']['N']
		event_payload['chargeback']['totalCharge']= float(transaction_total_charge)
	if 'costPerReport' in new_image:
		cost_per_report=new_image['costPerReport']['N']
		event_payload['chargeback']['costPerReport']= cost_per_report
	if 'ReportType' in new_image:
		reportType=new_image['ReportType']['S']
		event_payload['chargeback']['reportType']= reportType
	if 'chargeUpdatedFlag' in new_image:
		event_payload['chargeback']['chargeUpdatedFlag']= new_image['chargeUpdatedFlag']['BOOL']
	if('firstOrderDate' in event_payload['chargeback'] and event_payload['chargeback']['firstOrderDate']!=''):
		first_order_date= datetime.strptime(event_payload['chargeback']['firstOrderDate'], '%m/%d/%Y')
	else:
		first_order_date=''
	
	event_payload['chargeback']['pk']=new_image['PK']['S']
	event_payload['chargeback']['sk']=new_image['SK']['S']
	if('CorrelationID' in new_image):
		event_payload['chargeback']['correlationID']= new_image['CorrelationID']['S']
	if('orderCounterFlag' in new_image):
		event_payload['chargeback']['orderCounterFlag']= new_image['orderCounterFlag']['BOOL']
	if('RiskState' in new_image):
		event_payload['chargeback']['riskState']= new_image['RiskState']['S']
	if('Waived' in new_image):
		event_payload['chargeback']['waived']= new_image['Waived']['S']
	
	
	payload = json.dumps(event_payload, default=handle_decimal_type)
	payload= json.loads(payload)
	quote_id= event_payload['chargeback']['quoteID']
	quote_id_report = event_payload['chargeback']['quoteID'] + '#' + reportType
	if(new_image['PK']['S']== quote_id_report and payload['chargeback']['shallOrderFlag']== True and 'firstOrderDate' in payload['chargeback'] and payload['chargeback']['firstOrderDate']!= '' and payload['chargeback']['chargeUpdatedFlag']== Tr):
		s3_parquet(event, new_image, payload)

	try:
		payload_for_audit_table['PK']= quote_id
		payload_for_audit_table['SK']=str(date_time)
		payload_for_audit_table['Event']= 'SaveChargeback/S3' 
		payload_for_audit_table['Status']= 'Success'
		if('CorrelationID' in new_image):
			payload_for_audit_table['CorrelationID']= new_image['CorrelationID']['S']
		else:
			payload_for_audit_table['CorrelationID']=''
		audit_table= dynamoDB_client.Table(audit_logger_table)
		audit_table.put_item(Item=payload_for_audit_table)
	except Exception as ex:
		logger.error(': %s : Error occurred while putting data in Audit Table for QuoteID:  %s , Exception : %s', function_name,quote_id,ex)

def s3_parquet(event, new_image, payload):
	report_parquet ={}
	eastern = timezone('US/Eastern')## US/Eastern
	loc_dt = datetime.now(eastern)
	date_time = loc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
	insert_year = loc_dt.strftime('%Y') ## 2021
	insert_month = loc_dt.strftime('%m') ## 06
	insert_date = loc_dt.strftime('%d') ## 22
	#timestamp = loc_dt.strftime("%m/%d/%Y %H:%M:%S.%f")
	timestamp = loc_dt
	payload_for_audit_table={}
	error_payload_audit={}
	function_name='s3_parquet'
	try:
		quote_id=payload['chargeback']['quoteID']
		report_type=payload['chargeback']['reportType']
		if('firstOrderDate' in payload['chargeback'] and payload['chargeback']['firstOrderDate']!=''):
			first_order_date = payload['chargeback']['firstOrderDate']
			order_date=datetime.strptime(first_order_date, '%m/%d/%Y')
			date_= datetime(order_date.year, order_date.month, 1) + relativedelta(months=3, days=-1) 
			reporting_month= order_date.strftime("%B")
			close_date=date_
		input_data=payload['chargeback']
		if("policyIssueDate" in input_data):
			policy_issue_date_time = input_data['policyIssueDate'] +  input_data['policyIssueTime']
		if("quoteDate" in input_data):
			quote_date_time = input_data['quoteDate'] +  input_data['quoteTime']
		if("orderDate" in input_data):
			order_date_time =  input_data['orderDate'] +  input_data['orderTime']
			
		payload_fields = ['pk','sk', 'quoteID', 'totalDriver','totalOrderedDrivers','policyIssued','policyNumber','correlationID','reportType','orderCounterFlag','waived','totalCharge','baseState','shallOrderFlag','lob']
		for item in payload_fields:
			if item in input_data:
				report_parquet[item.lower()] = input_data[item]
			else:
				report_parquet[item.lower()]= None
		if("policyIssueDate" in input_data):
			policy_issue_date_time = input_data['policyIssueDate'] +  input_data['policyIssueTime']
		if("quoteDate" in input_data):
			quote_date_time = input_data['quoteDate'] +  input_data['quoteTime']
		if("orderDate" in input_data):
			order_date_time =  input_data['orderDate'] +  input_data['orderTime']
		if(quote_date_time!='' ):
			report_parquet['quotedate']= datetime.strptime(quote_date_time, '%m/%d/%Y%H:%M:%S')
		if(order_date_time!='' ):
			report_parquet['orderdate']=datetime.strptime(order_date_time, '%m/%d/%Y%H:%M:%S')
		else:
			report_parquet['orderdate']= None
		if(policy_issue_date_time!='' ):
			report_parquet['issuancedate']= datetime.strptime(policy_issue_date_time, '%m/%d/%Y%H:%M:%S')
		else:
			report_parquet['issuancedate']= None
		if("producerCode" in input_data):
			report_parquet['secondaryproducercode']= input_data['producerCode']

		report_parquet['currenttimestamp'] = timestamp
		if("tracker" in input_data):
			report_parquet['clickcounter']= input_data['tracker']
		if("orderedDriverForCurrReq" in input_data):
			report_parquet['driversorderedon']= input_data['orderedDriverForCurrReq']
		if("firstOrderDate" in input_data):
			report_parquet['closedate']= close_date.date()
		if("firstOrderDate" in input_data):
			report_parquet['firstorderdate']= order_date.date()
		if("costPerMVR" in input_data):
			report_parquet['costpermvr']= float(input_data['costPerMVR'])
		if("currentRequestCharge" in input_data):
			report_parquet['chargebackamount']= float(input_data['currentRequestCharge'])
		if('startDate' in input_data and input_data['startDate']!='' ):
			report_parquet['startdate'] = datetime.strptime(input_data['startDate'], "%m/%d/%Y").strftime("%Y-%m-%d")
			report_parquet['startdate'] = datetime.strptime(report_parquet['startdate'], '%Y-%m-%d')
			report_parquet['startdate'] = report_parquet['startdate'].date()
		
		logger.info(': %s : Putting data in %s folder in s3 bucket for QuoteID:  %s ', function_name,report_type,quote_id)
		filename = date_time+'.parquet'
		tablename= payload['chargeback']['reportType']
		file = str(tablename) +'/'+'ingestion_yyyymmdd='+str(insert_year)+str(insert_month)+str(insert_date)+'/'+ filename
		response=write_data_to_s3parquet(report_parquet,file)
		logger.info(f'{lambda_name} : {function_name}, S3_parquet file  :{filename} uploaded successfully in {report_type} folder,WriteTime : {str(datetime.now())}')

	except Exception as ex:
		traceback.print_exc()
		logger.error(': %s : Error occurred while putting data in %s folder in for QuoteID:  %s , Exception : %s', function_name,report_type,quote_id, ex)
		insert_failed_record_in_audit_table(event, new_image)
	return {
					'statusCode': 200,
					'body': json.dumps(f'S3_function Invocation for {report_type} folder is Successful')
					}
		
def write_data_to_s3parquet(s3item_parquet,file_path):
	s3 = boto3.resource("s3")
	df=pd.DataFrame([s3item_parquet])
	schemadf = pa.Schema.from_pandas(df)
	table = pa.Table.from_pandas(df, schema=schemadf, preserve_index=False)
	pq.write_table(
		table,
		"/tmp/sample.parquet",
		coerce_timestamps="ms",
		use_deprecated_int96_timestamps=True,
	)
	res = s3.Bucket(s3_bucket).upload_file("/tmp/sample.parquet", file_path)
	return res
	
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
			if('CorrelationID' in new_image):
				correlation_id = new_image['CorrelationID']['S']
		if 'jobNumber' in new_image.keys():
			quote_id= new_image['jobNumber']
		error_payload_audit_table={}
		report_type = event_payload['chargeback']['reportType']
		error_payload_audit_table['PK']=f'{report_type}#Failure'
	
		error_payload_audit_table['SK']= str(timestamp)
		error_payload_audit_table['Status']= 'Failure'
		error_payload_audit_table['Event']= 'SaveChargeback/S3'
		error_payload_audit_table['Payload']=event_payload
		error_payload_audit_table['Input']= json.dumps(event, default=handle_decimal_type) 
		error_payload_audit_table['QuoteId']=quote_id
		error_payload_audit_table['CorrelationID']= correlation_id
		audit_table= dynamoDB_client.Table(audit_logger_table)
		audit_table.put_item(Item=error_payload_audit_table)
		
	except Exception as ex:
		logger.exception(f'{lambda_name} : insert_failure_record_in_audit_table : Error in inserting failed record, Exception : {ex}')