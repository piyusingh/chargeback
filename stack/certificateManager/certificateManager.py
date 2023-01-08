import json
import jsii
from aws_cdk import (aws_certificatemanager as acm)
from aws_cdk import (aws_route53 as route53)
import aws_cdk as cdk
from constructs import Construct


class CertificateManagerStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str,**kwargs):
        super().__init__(scope,construct_id, **kwargs)
        ## Creation of Certificate
        my_hosted_zone = route53.HostedZone(self, "HostedZone",zone_name="chargeback.com")
        cert = acm.Certificate(self, "Certificate",domain_name="www.chargeback.com",validation=acm.CertificateValidation.from_dns(my_hosted_zone))
        