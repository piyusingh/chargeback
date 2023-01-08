from aws_cdk import (
                     aws_stepfunctions as sfn,
                     aws_stepfunctions_tasks as sfn_tasks,
                     aws_sqs as sqs,
                     aws_iam as iam_,
                     aws_logs as logs,
                     aws_lambda as lambda_)
import aws_cdk as cdk
from constructs import Construct

class ChargebackStepFunctionsStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,raw_config,**kwargs):
        super().__init__(scope,construct_id, **kwargs)
        
         ##Importing lambda functions
        decider_arn  = cdk.Fn.import_value(shared_value_to_import="chargeback-decider-export")
        decider = lambda_.Function.from_function_arn(self,"chargeback_decider_import",
                                                    function_arn = decider_arn )
        
        prepareDataSaveToDynamoDB_arn = cdk.Fn.import_value(shared_value_to_import="chargeback-prepareDataSaveToDynamoDB-export")
        prepareDataSaveToDynamoDB = lambda_.Function.from_function_arn(self,"chargeback_prepareDataSaveToDynamoDB_import",
                                                    function_arn = prepareDataSaveToDynamoDB_arn )
        
        prepareDataRetrieveFromDynamoDB_arn = cdk.Fn.import_value(shared_value_to_import="chargeback-prepareDataRetrieveFromDynamoDB-export")
        prepareDataRetrieveFromDynamoDB = lambda_.Function.from_function_arn(self,"chargeback_prepareDataRetrieveFromDynamoDB_import",
                                                    function_arn = prepareDataRetrieveFromDynamoDB_arn )
        
        saveAndRetrieveFromDynamoDB_arn = cdk.Fn.import_value(shared_value_to_import="chargeback-saveAndRetrieveFromDynamoDB-export")
        saveAndRetrieveFromDynamoDB = lambda_.Function.from_function_arn(self,"chargeback_saveAndRetrieveFromDynamoDB_import",
                                                    function_arn = saveAndRetrieveFromDynamoDB_arn )
        #Step Function
    
        Decider = sfn_tasks.LambdaInvoke(self, "Decider",
                               lambda_function=decider,
                               payload_response_only=True,
                               retry_on_service_exceptions=True,
                               )

        PrepareDataSaveToDynamoDB = sfn_tasks.LambdaInvoke(self, "prepareDataSaveToDynamoDB",
                               lambda_function=prepareDataSaveToDynamoDB,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               )  
        
        SaveAndRetrieveFromDynamoDB = sfn_tasks.LambdaInvoke(self, "saveAndRetrieveFromDynamoDB",
                               lambda_function=saveAndRetrieveFromDynamoDB,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               )  

        PrepareDataRetrieveFromDynamoDB = sfn_tasks.LambdaInvoke(self, "prepareDataRetrieveFromDynamoDB",
                               lambda_function=prepareDataRetrieveFromDynamoDB,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               )              

        choice = sfn.Choice(self, "Choice-SaveOrGet")
        choice.when(sfn.Condition.string_equals("$.resource", "/getChargeback"), PrepareDataRetrieveFromDynamoDB)
        choice.when(sfn.Condition.string_equals("$.resource", "/saveChargeback"), PrepareDataSaveToDynamoDB)
        
        # Definition
        definition_sf = Decider.next(choice.afterwards()).next(SaveAndRetrieveFromDynamoDB)

        #Log group creation
        log_group = logs.LogGroup(self, "ChargebackStateMachineLogGroup",
                                  log_group_name = "ChargebackStateMachineLogGroup",
                                  retention = logs.RetentionDays.INFINITE)

        sfn.StateMachine(
           self,"ChargebackStateMachine",
           state_machine_name="ChargebackStateMachine",
           definition=definition_sf,
           logs=sfn.LogOptions(destination=log_group,level=sfn.LogLevel.ALL),
           timeout=cdk.Duration.seconds(90),
           tracing_enabled=True
        )  


       
       