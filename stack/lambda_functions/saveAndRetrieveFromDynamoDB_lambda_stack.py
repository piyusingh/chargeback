from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs)

import aws_cdk as cdk
from constructs import Construct
from stack.stack_stage.configuration import EnvSpecific

class SaveAndRetrieveFromDynamoDBLambdaStack(cdk.Stack):
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
        

        ## Creation of SaveAndRetrieveFromDynamoDB Lambda
        self.saveAndRetrieveFromDynamoDB = lambda_.Function(self, "Chargeback_saveandretrievefromdynamodb",
                                        code=lambda_.Code.from_asset('lambdas/saveAndRetrieveFromDynamoDB'),
                                        function_name='Chargeback-SaveAndRetrieveFromDynamoDB',
                                        handler='Chargeback_PrepareDataSaveToDynamoDB.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["saveAndRetrieveFromDynamoDB_lambda_vars"],
                                        layers=[common_stack.aws_lambda_common_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )
        ## Role creation for SaveAndRetrieveFromDynamoDB: Start 
        saveAndRetrieveFromDynamoDB_statement = iam_.PolicyStatement()
        saveAndRetrieveFromDynamoDB_statement.add_actions("dynamodb:PutItem")
        saveAndRetrieveFromDynamoDB_statement.add_actions("dynamodb:GetItem")
        saveAndRetrieveFromDynamoDB_statement.add_actions("dynamodb:Query")
        saveAndRetrieveFromDynamoDB_statement.add_actions("lambda:InvokeFunction")
        saveAndRetrieveFromDynamoDB_statement.add_resources("*")
        ## Role creation for SaveAndRetrieveFromDynamoDB: End

        ## Assignment of role to SaveAndRetrieveFromDynamoDBLambda
        self.saveAndRetrieveFromDynamoDB.add_to_role_policy(saveAndRetrieveFromDynamoDB_statement)