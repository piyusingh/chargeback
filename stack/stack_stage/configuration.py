from dataclasses import dataclass
from typing import Any, Dict, Type, TypeVar
from aws_cdk import Environment, RemovalPolicy
import json

class EnvSpecific:
    def __init__(self, d):
        
        self.env = d["env"]
        self.common_stack_info = d["common_stack_info"]
        self.lambda_env_vars = d["lambda_env_vars"]
        self.dynamo_db_env_vars = d["dynamo_db_env_vars"]
        self.dynamodb_stream_event_batch_parameters = d["dynamodb_stream_event_batch_parameters"]
        self.apigateway_info = d["apigateway_info"]
        self.kinesis_info = d["kinesis_info"]
        self.kinesis_event_parameters = d["kinesis_event_parameters"]
        self.athena_info = d["athena_info"]
        
class RawConfig:
    """
    Raw JSON configuration of the application and of all infrastructure resources for
    each environment and.
    """

    def __init__(self, config_file: str):
        self._all_config: Any = self._read_config(config_file)
        self.development: Any = EnvSpecific(self._all_config['Dev'])
        self.application: Any = self._all_config['application']


    def _read_config(self, config_file):
        with open (config_file, 'r') as f:
            return json.load(f)


