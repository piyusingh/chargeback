from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs,
                     aws_dynamodb as dynamodb)
import aws_cdk as cdk
from constructs import Construct
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from stack.stack_stage.configuration import EnvSpecific

class StreamDataToS3LambdaStack(cdk.Stack):
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

        ## Creation of StreamDataToS3 Lambda
        self.streamDataToS3 = lambda_.Function(self, "Chargeback_streamdatatos3",
                                        code=lambda_.Code.from_asset('lambdas/streamDataToS3'),
                                        function_name='Chargeback-StreamDataToS3',
                                        handler='Chargeback_StreamDataToS3.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["streamDataToS3_lambda_vars"],
                                        layers=[common_stack.aws_lambda_common_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )
       ## Role creation for streamDataToS3:Start
        streamDataToS3_statement = iam_.PolicyStatement()
        streamDataToS3_statement.add_actions("s3:PutObject")
        streamDataToS3_statement.add_actions("s3:GetObject")
        streamDataToS3_statement.add_actions("dynamodb:*")
        streamDataToS3_statement.add_resources("*")
        ## Role creation for streamDataToS3:End

        ## Assignment of role to streamDataToS3 Lambda
        self.streamDataToS3.add_to_role_policy(streamDataToS3_statement)
        
      #Adding atrigger from dynamodb streams:
        table_stream_arn = cdk.Fn.import_value(shared_value_to_import="Chargeback-dynamodb-table-stream-arn-export")
        dynamodb_table = dynamodb.Table.from_table_attributes(self,"dynamodb_table",
                                                                table_name = raw_config.dynamo_db_env_vars["dynamodb_table_name"],
                                                                table_stream_arn = table_stream_arn
                                                                )
        self.dynamodb_event_source = DynamoEventSource(dynamodb_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=raw_config.dynamodb_stream_event_batch_parameters["batch_size"],
                bisect_batch_on_error=False
            )
        self.streamDataToS3.add_event_source(self.dynamodb_event_source)