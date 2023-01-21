from aws_cdk import (
                     aws_iam as iam_,
                     aws_lambda as lambda_,
                     aws_ec2 as ec2_,aws_logs)
from aws_cdk import (aws_kinesis as kinesis)

import aws_cdk as cdk
from constructs import Construct

from stack.stack_stage.configuration import EnvSpecific
from aws_cdk.aws_lambda_event_sources import KinesisEventSource


class ReadingFromKinesisLambdaStack(cdk.Stack):
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
        
        ## Creation of ReadingFromKinesis Lambda        
        self.readingFromKinesis = lambda_.Function(self, "Chargeback_readingFromKinesis",
                                        code=lambda_.Code.from_asset('lambdas/readingFromKinesis'),
                                        function_name= 'Chargeback-ReadingFromKinesis',
                                        handler='Chargeback_ReadingFromKinesis.lambda_handler',
                                        vpc=self.Vpc,
                                        security_groups=[self.Security_Group],
                                        tracing=lambda_.Tracing.ACTIVE,
                                        runtime=lambda_.Runtime.PYTHON_3_7,
                                        timeout=cdk.Duration.seconds(raw_config.lambda_env_vars["lambda_timeouts"]),
                                        environment=raw_config.lambda_env_vars["readingFromKinesis_lambda_vars"],
                                        layers=[common_stack.aws_lambda_layer],
                                        memory_size =raw_config.lambda_env_vars["lambda_memory"],
                                        )

        ## Role creation for ReadingFromKinesis:Start
        readingFromKinesis_statement = iam_.PolicyStatement()
        readingFromKinesis_statement.add_actions("xray:PutTraceSegments")
        readingFromKinesis_statement.add_actions("xray:PutTelemetryRecords")
        readingFromKinesis_statement.add_actions("states:DescribeStateMachine")
        readingFromKinesis_statement.add_actions("states:DescribeExecution")
        readingFromKinesis_statement.add_actions("states:StartExecution")
        readingFromKinesis_statement.add_actions("s3:PutObject")
        readingFromKinesis_statement.add_actions("s3:GetObject")
        readingFromKinesis_statement.add_actions("kinesis:GetRecords")
        readingFromKinesis_statement.add_actions("kinesis:GeShardIterator")
        readingFromKinesis_statement.add_actions("kinesis:PutRecord")
        readingFromKinesis_statement.add_actions("kinesis:PutRecords")
        readingFromKinesis_statement.add_actions("dynamodb:PutItem")
        readingFromKinesis_statement.add_actions("dynamodb:UpdateItem")
        readingFromKinesis_statement.add_actions("dynamodb:GetItem")
        readingFromKinesis_statement.add_actions("dynamodb:Query")

        readingFromKinesis_statement.add_resources("*")
        ## Role creation for ReadingFromKinesis:End

        ## Assignment of role to ReadingFromKinesis Lambda
        self.readingFromKinesis.add_to_role_policy(readingFromKinesis_statement)
        
        # Importing Kinesis stream arn
        kinesis_stream_arn = cdk.Fn.import_value(shared_value_to_import="Chargeback-kinesis-stream-arn-export")
        
        kinesis_stream = kinesis.Stream.from_stream_arn(self,"Chargeback-kinesis-stream-import",
                                                        stream_arn = kinesis_stream_arn)

        #adding kinesis stream event trigger
        self.kinesis_event_source = KinesisEventSource(kinesis_stream,
                batch_size = raw_config.kinesis_event_parameters["batch_size"], 
                starting_position=lambda_.StartingPosition.LATEST,
                max_record_age = cdk.Duration.seconds(raw_config.kinesis_event_parameters["max_record_age"])
        )            
        self.readingFromKinesis.add_event_source(self.kinesis_event_source )
