from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,aws_ec2 as ec2_)
from stack.stack_stage.configuration import EnvSpecific    
import aws_cdk as cdk
from constructs import Construct                

#ExpenseReport_lambda_stack
class ExpenseReportLambdaStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,common_stack,raw_config: EnvSpecific, **kwargs):
        super().__init__(scope,construct_id, **kwargs)

         #Vpc
        self.Vpc = ec2_.Vpc.from_vpc_attributes(
            self, 'Vpc',
            vpc_id = raw_config.common_stack_info["vpc_id"],
            availability_zones = raw_config.common_stack_info["availability_zones"],
            private_subnet_ids = raw_config.common_stack_info["private_subnet_ids"],
        )
        #Security Group
        security_group_id = raw_config.common_stack_info["security_group_id"]
        self.Security_Group = ec2_.SecurityGroup.from_security_group_id(self,"ChargeBack Default Security Group",
                                                                       security_group_id=security_group_id,
                                                                       mutable=False)

        ## Creation of monthlyAggregatedReport Lambda
        self.expenseReport = lambda_.Function(self, "Chargeback_expensereport",
                                        code=lambda_.Code.from_asset('lambdas/expenseReport'),
                                        function_name='Chargeback-ExpenseReport',
                                        handler='Chargeback_ExpenseReport.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_8,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["expenseReport_lambda_vars"],
                                        layers=[common_stack.aws_expense_report_layer],
                                        memory_size = raw_config.lambda_env_vars["lambda_memory"],
                                        role=common_stack.expense_report_iam_role
                                        )