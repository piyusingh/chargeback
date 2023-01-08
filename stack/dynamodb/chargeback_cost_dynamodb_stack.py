import json
import jsii
from aws_cdk import (aws_dynamodb as DynamoDB)
import aws_cdk as cdk
from constructs import Construct


class ChargebackCostDynamodbStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope,construct_id, **kwargs)
        ## Creation of DynamoDB
        self.ChargebackCostLogger = DynamoDB.Table(
            self,"Chargeback-Cost",
            table_name = "Chargeback-Cost",
            partition_key = DynamoDB.Attribute(
                name ="BaseState",
                type= DynamoDB.AttributeType.STRING
            ),
            sort_key = DynamoDB.Attribute(
                name="ReportType",
                type= DynamoDB.AttributeType.STRING

            ),
            billing_mode=DynamoDB.BillingMode.PAY_PER_REQUEST
        )
        
