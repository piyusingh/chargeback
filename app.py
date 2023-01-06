#!/usr/bin/env python3
import os
import aws_cdk as cdk
from constructs import Construct
from pathlib import Path
from stack.pipeline.chargeback_pipeline_stack import ChargebackPipelineStack
from stack.stack_stage.configuration import RawConfig
from config.AccountConfig import AwsConfig

config_file = Path('./env_based_resources.json')
raw_config = RawConfig(config_file)

app = cdk.App()

ChargebackPipelineStack(app, "ChargebackPipelineStack",
    env=cdk.Environment(account=AwsConfig.account_id(), region=AwsConfig.region()),
    raw_config=raw_config)

app.synth()
