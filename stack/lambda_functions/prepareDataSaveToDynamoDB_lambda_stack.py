from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs)

import aws_cdk as cdk
from constructs import Construct

from stack.stack_stage.configuration import EnvSpecific

class PrepareDataSaveToDynamoDBLambdaStack(cdk.Stack):
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
        self.Security_Group = ec2_.SecurityGroup.from_security_group_id(self, "Chargeback Default Security Group",
                                                                       security_group_id=security_group_id,
                                                                       mutable=False)

         ## Creation of PrepareDataSaveToDynamoDB Lambda
        self.prepareDataSaveToDynamoDB = lambda_.Function(self, "Chargeback_preparedatasavetodynamodb",
                                        code=lambda_.Code.from_asset('lambdas/prepareDataSaveToDynamoDB'),
                                        function_name='Chargeback-PrepareDataSaveToDynamoDB',
                                        handler='Chargeback_PrepareDataSaveToDynamoDB.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["prepareDataSaveToDynamoDB_lambda_vars"],
                                        layers=[common_stack.aws_lambda_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )
         ##Exporting lambda arn
        lambda_arn_export = cdk.CfnOutput(self,"chargeback_prepareDataSaveToDynamoDB_export",
                                               value = self.prepareDataSaveToDynamoDB.function_arn,
                                               export_name = "chargeback-prepareDataSaveToDynamoDB-export"
                                               )