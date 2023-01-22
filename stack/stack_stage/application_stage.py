import os
import aws_cdk as cdk
from constructs import Construct
from stack.stack_stage.configuration import RawConfig, EnvSpecific
from config.AccountConfig import AwsConfig
from stack.common.chargeback_common_stack import ChargebackCommonStack
from stack.lambda_functions.decider_lambda_stack import DeciderLambdaStack
from stack.lambda_functions.triggerStateMachine_lambda_stack import TriggerStateMachineLambdaStack
from stack.lambda_functions.prepareDataSaveToDynamoDB_lambda_stack import PrepareDataSaveToDynamoDBLambdaStack
from stack.lambda_functions.prepareDataRetrieveFromDynamoDB_lambda_stack import PrepareDataRetrieveFromDynamoDBLambdaStack
from stack.lambda_functions.saveAndRetrieveFromDynamoDB_lambda_stack import SaveAndRetrieveFromDynamoDBLambdaStack
from stack.dynamodb.chargeback_audit_logger_dynamodb_stack import ChargebackAuditLoggerDynamodbStack
from stack.dynamodb.chargeback_cost_dynamodb_stack import ChargebackCostDynamodbStack
from stack.dynamodb.chargeback_dynamodb_stack import ChargebackDynamodbStack
from stack.kinesis.chargeback_kinesis_stack import ChargebackKinesisStack
from stack.lambda_functions.streamDataToS3_lambda_stack import StreamDataToS3LambdaStack
from stack.lambda_functions.readingFromDynamoDBStreams_lambda_stack import ReadingFromDynamoDBStreamsLambdaStack
from stack.lambda_functions.readingFromKinesis_lambda_stack import ReadingFromKinesisLambdaStack
from stack.lambda_functions.reportCSV_lambda_stack import ReportCSVLambdaStack
from stack.lambda_functions.aggregateReport_lambda_stack import AggregateReportLambdaStack
from stack.lambda_functions.transactionReport_lambda_stack import TransactionReportLambdaStack
from stack.lambda_functions.reportTriggerStateMachine_lambda_stack import ReportTriggerStateMachineLambdaStack
from stack.lambda_functions.expenseReport_lambda_stack import ExpenseReportLambdaStack
from stack.step_functions.chargeback_report_step_functions_stack import ChargebackReportStepFunctionsStack
from stack.s3.chargeback_s3_stack import ChargebackS3Stack
from stack.step_functions.chargeback_step_functions_stack import ChargebackStepFunctionsStack
from stack.athena.chargeback_athena_stack import ChargebackAthenaStack
from stack.apigateway.chargeback_apigateway_stack import ChargebackApigatewayStack


# Adding lambda
class ChargebackStacks(cdk.Stage):
  def __init__(self, scope: Construct,id: str, raw_config: EnvSpecific, **kwargs):
    super().__init__(scope,id, **kwargs)
 
    common_stack = ChargebackCommonStack( self,'ChargebackCommonStack',raw_config) 
    decider_lambda_stack  = DeciderLambdaStack( self,'DeciderLambdaStack',common_stack,raw_config=raw_config)
    triggerstatemachine_lambda_stack  = TriggerStateMachineLambdaStack( self,'TriggerStateMachineLambdaStack',common_stack,raw_config=raw_config)
    preparedatasavetodynamodb_lambda_stack = PrepareDataSaveToDynamoDBLambdaStack( self,'PrepareDataSaveToDynamoDBLambdaStack',common_stack,raw_config=raw_config)
    preparedataretrievefromdynamodb_lambda_stack = PrepareDataRetrieveFromDynamoDBLambdaStack( self,'PrepareDataRetrieveFromDynamoDBLambdaStack',common_stack,raw_config=raw_config)
    savesndretrievefromdynamodb_lambda_stack = SaveAndRetrieveFromDynamoDBLambdaStack( self,'SaveAndRetrieveFromDynamoDBLambdaStack',common_stack,raw_config=raw_config)
    streamdatatos3_lambda_stack = StreamDataToS3LambdaStack( self,'StreamDataToS3LambdaStack',common_stack,raw_config=raw_config)
    readingfromdynamodbstreams_lambda_stack = ReadingFromDynamoDBStreamsLambdaStack( self,'ReadingFromDynamoDBStreamsLambdaStack',common_stack,raw_config=raw_config)
    readingfromkinesis_lambda_stack = ReadingFromKinesisLambdaStack( self,'ReadingFromKinesisLambdaStack',common_stack,raw_config=raw_config)
    chargebackcostdynamodbstack = ChargebackCostDynamodbStack( self,'ChargebackCostDynamoDB')
    chargebackdynamodbstack = ChargebackDynamodbStack( self,'ChargebackDynamoDB')
    chargebackauditloggerdynamodbstack = ChargebackAuditLoggerDynamodbStack( self,'ChargebackAuditLoggerDynamoDB')
    chargebacks3stack = ChargebackS3Stack( self,'S3')
    chargebackkinesisstack = ChargebackKinesisStack( self,'Kinesis',raw_config=raw_config)
    chargeback_step_functions = ChargebackStepFunctionsStack( self,'ChargebackStepFunctionStack',raw_config=raw_config)
    reportcsv_lambda_stack = ReportCSVLambdaStack( self,'ReportCSVLambdaStack',common_stack,raw_config=raw_config)
    aggregatereport_lambda_stack = AggregateReportLambdaStack( self,'AggregateReportLambdaStack',common_stack,raw_config=raw_config)
    transactionreport_lambda_stack = TransactionReportLambdaStack( self,'TransactionReportLambdaStack',common_stack,raw_config=raw_config)
    reporttriggerstatemachine_lambda_stack = ReportTriggerStateMachineLambdaStack( self,'ReportTriggerStateMachineLambdaStack',common_stack,raw_config=raw_config)
    expensereport_lambda_stack = ExpenseReportLambdaStack( self,'ExpenseReportLambdaStack',common_stack,raw_config=raw_config)
    chargebackreport_step_functions = ChargebackReportStepFunctionsStack( self,'ChargebackReportStepFunctionStack', reportcsv_lambda_stack = reportcsv_lambda_stack.reportCSV,transactionreport_lambda_stack = transactionreport_lambda_stack.transactionReport,aggregatereport_lambda_stack = aggregatereport_lambda_stack.aggregateReport,expensereport_lambda_stack = expensereport_lambda_stack.expenseReport,raw_config=raw_config)
    chargebackapigatewaystack = ChargebackApigatewayStack( self,'ChargebackApigatewayStack',raw_config=raw_config)
    chargebackathenastack = ChargebackAthenaStack( self,'ChargebackAthenaStack',raw_config=raw_config)

   