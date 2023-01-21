

from aws_cdk import (aws_kinesis as kinesis)
import aws_cdk as cdk 
from constructs import Construct



class ChargebackKinesisStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,raw_config, **kwargs):
        super().__init__(scope,construct_id, **kwargs)

        #Creating Kinesis Stream
        self.kinesis_stream = kinesis.Stream(self,"ChargeBackStream",
                    stream_name="ChargeBackStream",
                    shard_count=raw_config.kinesis_info["shard_count"],
                    retention_period=cdk.Duration.hours(raw_config.kinesis_info["retention_period"])
                )
        
        ##Exporting api id and stage
        kinesis_stream_arn = cdk.CfnOutput(self,"Chargeback_kinesis_stream_arn_export",
                                               value = self.kinesis_stream.stream_arn,
                                               export_name = "Chargeback-kinesis-stream-arn-export"
                                               )
