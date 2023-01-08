from aws_cdk import (aws_s3 as s3,aws_iam as iam)
import aws_cdk as cdk
from constructs import Construct


class ChargebackS3Stack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        #S3 Bucket Creation
        bucket = s3.Bucket(self, "ChargeBackInsurance",
                bucket_name = "chargebackinsurance",
                )
