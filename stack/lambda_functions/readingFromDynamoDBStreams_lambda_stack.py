from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs,
                     aws_dynamodb as dynamodb)

import aws_cdk as cdk
from constructs import Construct
from aws_cdk.aws_lambda_event_sources import DynamoEventSource


from stack.stack_stage.configuration import EnvSpecific


class ReadingFromDynamoDBStreamsLambdaStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,common_stack,raw_config: EnvSpecific,**kwargs):
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
        ## Creation of decider Lambda        
        self.readingFromDynamoDBStreams = lambda_.Function(self, "Chargeback_readingfromdynamodbstreams",
                                        code=lambda_.Code.from_asset('lambdas/readingFromDynamoDBStreams'),
                                        function_name='Chargeback-ReadingFromDynamoDBStreams',
                                        handler='Chargeback_ReadingFromDynamoDBStreams.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["readingFromDynamoDBStreams_lambda_vars"],
                                        layers=[common_stack.aws_lambda_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )

        ## Role creation for readingFromDynamoDBStreams:Start
        readingFromDynamoDBStreams_statement = iam_.PolicyStatement()
        readingFromDynamoDBStreams_statement.add_actions("xray:PutTraceSegments")
        readingFromDynamoDBStreams_statement.add_actions("xray:PutTelemetryRecords")
        readingFromDynamoDBStreams_statement.add_actions("states:DescribeStateMachine")
        readingFromDynamoDBStreams_statement.add_actions("states:DescribeExecution")
        readingFromDynamoDBStreams_statement.add_actions("states:StartExecution")
        readingFromDynamoDBStreams_statement.add_actions("kinesis:GetRecords")
        readingFromDynamoDBStreams_statement.add_actions("kinesis:GeShardIterator")
        readingFromDynamoDBStreams_statement.add_actions("kinesis:PutRecord")
        readingFromDynamoDBStreams_statement.add_actions("kinesis:PutRecords")
        readingFromDynamoDBStreams_statement.add_actions("dynamodb:*")
        readingFromDynamoDBStreams_statement.add_resources("*")
        ## Role creation for readingFromDynamoDBStreams:End

        ## Assignment of role to readingFromDynamoDBStreams Lambda
        self.readingFromDynamoDBStreams.add_to_role_policy(readingFromDynamoDBStreams_statement)

        #Adding atrigger from dynamodb streams:

        table_stream_arn = cdk.Fn.import_value(shared_value_to_import="Chargeback-dynamodb-table-stream-arn-export")

        dynamodb_table = dynamodb.Table.from_table_attributes(self,"dynamodb_table",
                                                                table_name = raw_config.dynamo_db_env_vars["dynamodb_table_name"],
                                                                table_stream_arn = table_stream_arn
                                                                )

        self.dynamodb_event_source = DynamoEventSource(dynamodb_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=raw_config.dynamodb_stream_event_batch_parameters["batch_size"],
                bisect_batch_on_error=False,
                
            )
        self.readingFromDynamoDBStreams.add_event_source(self.dynamodb_event_source)


