"""
Template Component main class.

"""
import asyncio
import os
import logging
from typing import List
import json

from keboola.component.base import ComponentBase
from keboola.component.sync_actions import SelectElement
from keboola.component.exceptions import UserException

from sap_client.client import SAPClient, SapErpClientException
from configuration import Configuration


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._configuration: Configuration
        self.temp_folder = os.path.join(self.data_folder_path, "temp")

    async def run(self):
        """
        Main execution code
        """
        self._init_configuration()

        server_url = self._configuration.authentication.server_url
        resource_endpoint = self._configuration.source.resource_endpoint
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password

        client = SAPClient(server_url, username, password)

        out_table = self.create_out_table_definition("test")

        if not resource_endpoint:
            raise UserException("You need to specify either Catalog Service url or Resource url.")
        try:
            await client.get_data(resource_endpoint, out_table.full_path)
        except SapErpClientException as e:
            raise UserException(f"Failed to download results from {resource_endpoint}") from e

    def list_metadata(self):
        files = self.get_files_in_folder(self.temp_folder)
        return files

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    @staticmethod
    def create_folder_if_not_exists(folder_path: str):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    @staticmethod
    def get_files_in_folder(folder_path: str) -> list:
        files = []
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                files.append((file_name, file_path))
        return files

    async def execute_action(self):
        """
        Overrides execute_action from component.base.
        Executes action defined in the configuration.
        """
        action = self.configuration.action
        if not action:
            logging.warning("No action defined in the configuration, using the default run action.")
            action = 'run'

        try:
            action_mapping = {
                "run": self.run,
                "listTables": self.list_tables
            }
            action_method = action_mapping[action]
        except (AttributeError, KeyError) as e:
            raise AttributeError(f"The defined action {action} is not implemented!") from e
        await action_method()

    async def list_tables(self) -> List[SelectElement]:
        self._init_configuration()

        self.create_folder_if_not_exists(self.temp_folder)

        catalog_service_endpoint = self._configuration.authentication.catalog_service_endpoint
        if not catalog_service_endpoint:
            raise UserException("listTables function only works when Catalog Service endpoint is defined. "
                                "In case you do not have Catalog Service available, specify the Resource endpoint "
                                "in row configuration.")

        server_url = self._configuration.authentication.server_url
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password

        client = SAPClient(server_url, username, password)

        self.create_folder_if_not_exists(self.temp_folder)
        await client.get_metadata(catalog_service_endpoint, self.temp_folder)
        available_data = self.list_metadata()

        tables = []
        for file_name, file_path in available_data:
            name = file_name.rstrip(".json")
            with open(file_path) as json_file:
                metadata_json = json.load(json_file)
                service_url = metadata_json.get("ServiceUrl")
            tables.append({"table_name": name, "service_url": service_url})

        select_elements = [
            SelectElement(
                label=table["table_name"],
                value=table["service_url"]
            )
            for table in tables
        ]

        return select_elements


"""
        Main entrypoint
"""

if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        asyncio.run(comp.execute_action())
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
