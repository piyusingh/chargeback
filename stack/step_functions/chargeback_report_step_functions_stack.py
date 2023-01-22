from aws_cdk import (
                     aws_stepfunctions as sfn,
                     aws_stepfunctions_tasks as sfn_tasks,
                     aws_sqs as sqs,
                     aws_iam as iam_,
                     aws_logs as logs,
                     aws_lambda as lambda_)
import aws_cdk as cdk
from constructs import Construct

class ChargebackReportStepFunctionsStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,reportcsv_lambda_stack,transactionreport_lambda_stack,aggregatereport_lambda_stack,expensereport_lambda_stack,raw_config,**kwargs):
        super().__init__(scope,construct_id, **kwargs)
        
        #Step Function
    
        ConcatParquetFiles = sfn_tasks.LambdaInvoke(self, "ConcatParquetFiles",
                               lambda_function=reportcsv_lambda_stack,
                               payload_response_only=True,
                               input_path= "$.input.year_month_detail",
                               result_path= sfn.JsonPath.DISCARD,
                               output_path= "$.input.year_month_detail",
                               retry_on_service_exceptions=True,
                               )

        TransactionReport = sfn_tasks.LambdaInvoke(self, "TransactionReport",
                               lambda_function=transactionreport_lambda_stack,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               result_path= sfn.JsonPath.DISCARD,
                               output_path="$"
                               )  
        
        AggregateReport = sfn_tasks.LambdaInvoke(self, "AggregateReport",
                               lambda_function=aggregatereport_lambda_stack,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               result_path= sfn.JsonPath.DISCARD,
                               output_path="$"
                               )  

        ExpenseReport = sfn_tasks.LambdaInvoke(self, "ExpenseReport",
                               lambda_function=expensereport_lambda_stack,
                               retry_on_service_exceptions=True,
                               payload_response_only=True,
                               )              
        
        # Definition
        definition_sf = ConcatParquetFiles.next(TransactionReport).next(AggregateReport).next(ExpenseReport)

        #Log group creation
        log_group = logs.LogGroup(self, "ChargebackReportStateMachineLogGroup",
                                  log_group_name = "ChargebackReportStateMachineLogGroup",
                                  retention = logs.RetentionDays.INFINITE)

        sfn.StateMachine(
           self,"ChargebackReportStateMachine",
           state_machine_name="ChargebackReportStateMachine",
           definition=definition_sf,
           logs=sfn.LogOptions(destination=log_group,level=sfn.LogLevel.ALL),
           timeout=cdk.Duration.seconds(90),
           tracing_enabled=True
        )  