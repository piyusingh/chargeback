import json
import jsii
from aws_cdk import (aws_dynamodb as DynamoDB)
import aws_cdk as cdk
from constructs import Construct


class ChargebackDynamodbStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope,construct_id, **kwargs)
        ## Creation of DynamoDB
        self.ChargeBack = DynamoDB.Table(
            self, "Chargeback",
            table_name = "Chargeback",
            partition_key = DynamoDB.Attribute(
                name ="PK",
                type= DynamoDB.AttributeType.STRING
            ),
            sort_key = DynamoDB.Attribute(
                name="SK",
                type= DynamoDB.AttributeType.STRING

            ),
            stream=DynamoDB.StreamViewType.NEW_AND_OLD_IMAGES,
            billing_mode=DynamoDB.BillingMode.PAY_PER_REQUEST
        )

        ##Exporting api id and stage
        table_stream_arn = cdk.CfnOutput(self,
                                               "Chargeback_dynamodb_table_stream_arn_export",
                                               value = self.ChargeBack.table_stream_arn,
                                               export_name = "Chargeback-dynamodb-table-stream-arn-export"
                                               )
