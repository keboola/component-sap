import dataclasses
import json
from dataclasses import dataclass
from typing import List, Optional

import dataconf


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class Authentication(ConfigurationBase):
    server_url: str
    username: str
    pswd_password: str
    catalog_service_endpoint: Optional[str]


@dataclass
class Source(ConfigurationBase):
    resource_endpoint: Optional[str]


@dataclass
class Destination(ConfigurationBase):
    custom_tag: str = ""
    permanent_files: bool = False


@dataclass
class Configuration(ConfigurationBase):
    authentication: Authentication
    source: Source
