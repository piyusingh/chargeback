from dataclasses import dataclass
from typing import Any, Dict, Type, TypeVar
from aws_cdk import Environment, RemovalPolicy
import json

class EnvSpecific:
    def __init__(self, d):
        
        self.env = d["env"]
        self.common_stack_info = d["common_stack_info"]
        
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

AppConfigClass = TypeVar('AppConfigClass', bound='AppConfig')


@dataclass
class AppConfig:
    """Configuration of the application."""
    application_name: str
    repository_name: str
    branch: str
    build_environment: Environment

    @classmethod
    def from_raw_config(cls: Type[AppConfigClass], raw_config: Dict[str, Any]) -> AppConfigClass:
        """
        Constructor from raw configuration.
        It will perform some conversion to CDK specific types and classes.
        """
        raw_config = cls.convert_to_cdk_constructs(raw_config)

        return cls(**raw_config)

    @staticmethod
    def convert_to_cdk_constructs(raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts raw config to CDK specific constructs when required.
        This method can be used when overwriting the classmethod construct of
        an inherited class in order to preserve the conversions of the parent
        class.
        """
        raw_build_environment = raw_config.pop('build_environment')

        build_environment = Environment(**raw_build_environment)

        raw_config.update(build_environment=build_environment)

        return raw_config
