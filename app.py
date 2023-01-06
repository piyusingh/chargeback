# #!/usr/bin/env python3
# # import os

# # import aws_cdk as cdk

# # from chargeback.chargeback_stack import ChargebackStack


# # app = cdk.App()
# ChargebackStack(app, "ChargebackStack",
#     # If you don't specify 'env', this stack will be environment-agnostic.
#     # Account/Region-dependent features and context lookups will not work,
#     # but a single synthesized template can be deployed anywhere.

#     # Uncomment the next line to specialize this stack for the AWS Account
#     # and Region that are implied by the current CLI configuration.

#     #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

#     # Uncomment the next line if you know exactly what Account and Region you
#     # want to deploy the stack to. */

#     #env=cdk.Environment(account='123456789012', region='us-east-1'),

#     # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
#     )

# app.synth()


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
