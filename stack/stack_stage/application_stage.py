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
from stack.lambda_functions.streamDataToS3_lambda_stack import StreamDataToS3LambdaStack
from stack.s3.chargeback_s3_stack import ChargebackS3Stack
from stack.step_functions.chargeback_step_functions_stack import ChargebackStepFunctionsStack
from stack.apigateway.chargeback_apigateway_stack import ChargebackApigatewayStack


# Adding lambda
class ChargebackStacks(cdk.Stage):
  def __init__(self, scope: Construct,id: str, raw_config: EnvSpecific, **kwargs):
    super().__init__(scope,id, **kwargs)
 
    common_stack = ChargebackCommonStack( self,'ChargebackCommonStack',raw_config) 
    decider_lambda_stack  = DeciderLambdaStack( self,'DeciderLambdaStack',common_stack,raw_config=raw_config)
    triggerstatemachine_lambda_stack  = TriggerStateMachineLambdaStack( self,'TriggerStateMachineLambdaStack',common_stack,raw_config=raw_config)
    preparedatasavetodynamodb_lambda_stack = PrepareDataSaveToDynamoDBLambdaStack( self,'PrepareDataSaveToDynamoDB',common_stack,raw_config=raw_config)
    preparedataretrievefromdynamodb_lambda_stack = PrepareDataRetrieveFromDynamoDBLambdaStack( self,'PrepareDataRetrieveFromDynamoDB',common_stack,raw_config=raw_config)
    savesndretrievefromdynamodb_lambda_stack = SaveAndRetrieveFromDynamoDBLambdaStack( self,'SaveAndRetrieveFromDynamoDB',common_stack,raw_config=raw_config)
    streamdatatos3_lambda_stack = StreamDataToS3LambdaStack( self,'StreamDataToS3',common_stack,raw_config=raw_config)
    chargebackcostdynamodbstack = ChargebackCostDynamodbStack( self,'ChargebackCostDynamoDB')
    chargebackdynamodbstack = ChargebackDynamodbStack( self,'ChargebackDynamoDB')
    chargebackauditloggerdynamodbstack = ChargebackAuditLoggerDynamodbStack( self,'ChargebackAuditLoggerDynamoDB')
    chargebacks3stack = ChargebackS3Stack( self,'S3')
    chargeback_step_functions = ChargebackStepFunctionsStack( self,'ChargebackStepFunctionsStack',raw_config=raw_config)
    chargeback_step_functions = ChargebackApigatewayStack( self,'ChargebackApigatewayStack',raw_config=raw_config)


   