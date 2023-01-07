from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs)

import aws_cdk as cdk
from constructs import Construct
                     
from stack.stack_stage.configuration import EnvSpecific

class TriggerStateMachineLambdaStack(cdk.Stack):
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

        ## Creation of triggerStateMachine Lambda
        self.triggerStateMachine = lambda_.Function(self, "Chargeback_triggerstatemachine",
                                        code=lambda_.Code.from_asset('lambdas/triggerStateMachine'),
                                        function_name='Chargeback-TriggerStateMachine',
                                        handler='Chargeback_TriggerStateMachine.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["triggerStateMachine_lambda_vars"],
                                        layers=[common_stack.aws_lambda_common_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )
           ## Role creation for trigger statemachine: Start 
        triggerStateMachine_statement = iam_.PolicyStatement()
        triggerStateMachine_statement.add_actions("states:DescribeStateMachine")
        triggerStateMachine_statement.add_actions("states:DescribeExecution")
        triggerStateMachine_statement.add_actions("states:StartExecution")
        triggerStateMachine_statement.add_actions("lambda:InvokeFunction")
        triggerStateMachine_statement.add_actions("dynamodb:PutItem")
        triggerStateMachine_statement.add_resources("*")
        ## Role creation for trigger statemachine: End

        ## Assignment of role to triggerStateMachineLambda
        self.triggerStateMachine.add_to_role_policy(triggerStateMachine_statement)
        
        ##Exporting lambda arn
        lambda_arn_export = cdk.CfnOutput(self,
                                               "Chargeback_triggerStateMachine_export",
                                               value = self.triggerStateMachine.function_arn,
                                               export_name = "Chargeback-triggerStateMachine-export"
                                               )