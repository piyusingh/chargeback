from aws_cdk import (aws_ec2 as ec2_,aws_lambda as lambda_)
import aws_cdk as cdk
from constructs import Construct


class ChargebackCommonStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,raw_config, **kwargs):
        super().__init__(scope,construct_id, **kwargs)

        self.Vpc = ec2_.Vpc.from_vpc_attributes(
            self, 'Vpc',
            vpc_id = raw_config.common_stack_info["vpc_id"],
            availability_zones = raw_config.common_stack_info["availability_zones"],
            private_subnet_ids = raw_config.common_stack_info["private_subnet_ids"],
        )
        
        security_group_id = raw_config.common_stack_info["security_group_id"]
        
        self.Security_Group = ec2_.SecurityGroup.from_security_group_id(self, "Chargeback Default Security Group",
                                                                       security_group_id=security_group_id,
                                                                       mutable=False)
        common_python_runtime = lambda_.Runtime.PYTHON_3_7

        #Common layer
        self.aws_lambda_layer = lambda_.LayerVersion(
            self,
            "Chargeback_aws_lambda_layer",
            layer_version_name="Chargeback_AWS_Lambda_Layer",
            description="AWS powertools,Boto,Pandas,Pyarrow",
            code=lambda_.Code.from_asset("lambdas/layers/chargeback_layer"),
            compatible_runtimes=[common_python_runtime],
        )
        
        