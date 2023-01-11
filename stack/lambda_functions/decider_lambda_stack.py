from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_)

import aws_cdk as cdk
from constructs import Construct

from stack.stack_stage.configuration import EnvSpecific


class DeciderLambdaStack(cdk.Stack):
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
        
        ## Creation of decider Lambda
        self.decider = lambda_.Function(self, "Chargeback_decider",
                                        code=lambda_.Code.from_asset('lambdas/decider'),
                                        function_name='Chargeback-Decider',
                                        handler='Chargeback_Decider.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["decider_lambda_vars"],
                                        layers=[common_stack.aws_lambda_layer],
                                        memory_size = raw_config.lambda_env_vars["lambda_memory"]
                                        )

        ## Role creation for Decider:Start
        decider_statement = iam_.PolicyStatement()
        decider_statement.add_actions("states:DescribeStateMachine")
        decider_statement.add_actions("states:DescribeExecution")
        decider_statement.add_actions("states:StartExecution")
        decider_statement.add_resources("*")
        ## Role creation for Decider:End

        ## Assignment of role to decider Lambda
        self.decider.add_to_role_policy(decider_statement)

         ##Exporting lambda arn
        lambda_arn_export = cdk.CfnOutput(self,"chargeback_decider_export",
                                               value = self.decider.function_arn,
                                               export_name = "chargeback-decider-export")
