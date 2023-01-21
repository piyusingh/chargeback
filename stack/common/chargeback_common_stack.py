from aws_cdk import (aws_ec2 as ec2_,aws_lambda as lambda_,aws_iam as iam_)
import aws_cdk as cdk
from constructs import Construct


class ChargebackCommonStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,raw_config, **kwargs):
        super().__init__(scope,construct_id, **kwargs)

        self.Vpc = ec2_.Vpc.from_vpc_attributes(
            self, 'Vpc',
            vpc_id = raw_config.common_stack_info["vpc_id"],
            availability_zones = raw_config.common_stack_info["availability_zones"],
            private_subnet_ids = raw_config.common_stack_info["private_subnet_ids"],
        )
        
        security_group_id = raw_config.common_stack_info["security_group_id"]
        
        self.Security_Group = ec2_.SecurityGroup.from_security_group_id(self, "Chargeback Default Security Group",
                                                                       security_group_id=security_group_id,
                                                                       mutable=False)
         #Create Policy
        self.policy_document = iam_.PolicyDocument(
            statements= [
                iam_.PolicyStatement(
                resources= ["*"],
                actions= ["DynamoDB:*"]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= ["s3:*"]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= ["athena:*"]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= ["glue:*"]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= [
                    "states:DescribeStateMachine",
                    "states:DescribeExecution",
                    "states:StartExecution"
                    ]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= [
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords"
                    ]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                    ]
                ),
                iam_.PolicyStatement(
                resources= ["*"],
                actions= [
                    "lambda:InvokeFunction",
                    "lambda:InvokeAsync"
                    ]
                )
            ]
        )
         #Create Role
        self.expense_report_iam_role = iam_.Role(
            self, "Chargeback_ExpenseReport_Role",
            role_name= "Chargeback_ExpenseReport_Role",
            assumed_by= iam_.ServicePrincipal('lambda.amazonaws.com'),
            description= 'Role to allow lambda to access resources',
            inline_policies= {'AccessResources': self.policy_document},
            managed_policies= [
            iam_.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
            iam_.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaVPCAccessExecutionRole')
            ]
        )
        common_python_runtime = lambda_.Runtime.PYTHON_3_7
        common_python_runtime_3_8 = lambda_.Runtime.PYTHON_3_8

        #Common layer
        self.aws_lambda_layer = lambda_.LayerVersion(
            self,
            "Chargeback_aws_lambda_layer",
            layer_version_name="Chargeback_AWS_Lambda_Layer",
            description="AWS powertools,Boto,Pandas,Pyarrow",
            code=lambda_.Code.from_asset("lambdas/layers/chargeback_layer"),
            compatible_runtimes=[common_python_runtime],
        )
        
        #FastParquet Common Layer
        self.aws_lambda_fastparquet_common_layer = lambda_.LayerVersion(
            self,
            "Chargeback_monthly_expense_fastparquet_layer",
            layer_version_name="Chargeback_fastparquet_expense_common_layer",
            description="The AWS Lambda Common Layer for monthly expense report lambda functions",
            code=lambda_.Code.from_asset("lambdas/layers/chargeback_fastparquet"),
            compatible_runtimes=[common_python_runtime],
            removal_policy=cdk.RemovalPolicy.RETAIN
        )
        #Xlsx Report Layer
        self.aws_expense_report_layer = lambda_.LayerVersion(
            self,
            "Chargeback_expense_report_lambda_layer",
            layer_version_name="Chargeback_XLSX_Report_Lambda_Layer",
            description="The AWS Lambda Expense Report Client Layer",
            code=lambda_.Code.from_asset("lambdas/layers/chargeback_xlsxwriter"),
            compatible_runtimes=[common_python_runtime_3_8, common_python_runtime],
            removal_policy=cdk.RemovalPolicy.RETAIN
        )

        