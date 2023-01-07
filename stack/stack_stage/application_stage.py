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

   