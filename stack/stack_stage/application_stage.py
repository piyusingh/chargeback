import os
import aws_cdk as cdk
from constructs import Construct
from stack.stack_stage.configuration import RawConfig, EnvSpecific
from config.AccountConfig import AwsConfig
from stack.common.chargeback_common_stack import ChargebackCommonStack
from stack.lambda_functions.decider_lambda_stack import DeciderLambdaStack



# Adding lambda
class ChargebackStacks(cdk.Stage):
  def __init__(self, scope: Construct,id: str, raw_config: EnvSpecific, **kwargs):
    super().__init__(scope,id, **kwargs)
 
    common_stack = ChargebackCommonStack( self,'ChargebackCommonStack',raw_config) 
    decider_lambda_stack  = DeciderLambdaStack( self,'DeciderLambdaStack',common_stack,raw_config=raw_config)

   