import asyncio
import json
import logging
import os
import shutil
import time

from keboola.component.base import ComponentBase, sync_action
from keboola.csvwriter import ElasticDictWriter
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from sap_client.client import SAPClient, SapClientException
from sap_client.sap_snowflake_mapping import SAP_TO_SNOWFLAKE_MAP
from configuration import Configuration


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._configuration: Configuration

    def run(self):
        """
        Main execution code
        """
        self._init_configuration()

        server_url = self._configuration.authentication.server_url
        resource_alias = self._configuration.source.resource_alias
        limit = self._configuration.source.limit
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password

        temp_dir = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_dir, exist_ok=True)

        client = SAPClient(server_url, username, password, temp_dir, limit, verify=False)

        out_table = self.create_out_table_definition(resource_alias)

        try:
            start_time = time.time()
            asyncio.run(client.fetch(resource_alias))
            end_time = time.time()
            runtime = end_time - start_time
            logging.info(f"Fetched data in {runtime:.2f} seconds")
        except SapClientException as e:
            raise UserException(f"An error occurred while fetching resource: {e}")

        for json_file in os.listdir(temp_dir):
            with ElasticDictWriter(out_table.full_path, []) as wr:
                wr.writeheader()
                json_file_path = os.path.join(temp_dir, json_file)
                with open(json_file_path, 'r') as file:
                    content = json.load(file)
                    for row in content:
                        wr.writerow(row)

        out_table = self.add_column_metadata(client, out_table)

        self.write_manifest(out_table)

        # Clean temp folder (for local runs)
        shutil.rmtree(temp_dir)

    @staticmethod
    def add_column_metadata(client, out_table):
        for column in client.metadata:
            col_md = client.metadata.get(column)
            datatype = SAP_TO_SNOWFLAKE_MAP[col_md.get("TYPE")]
            if datatype in ["STRING", "INTEGER", "NUMERIC"]:
                length = str(col_md.get("LENGTH"))
            else:
                length = None
            out_table.table_metadata.add_column_data_type(column=column,
                                                          data_type=datatype,
                                                          length=length)
        return out_table

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    @sync_action("listResources")
    def list_resources(self) -> list[SelectElement]:
        self._init_configuration()

        server_url = self._configuration.authentication.server_url
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password

        client = SAPClient(server_url, username, password, "", verify=False)
        sources = asyncio.run(client.list_sources())

        return [
            SelectElement(
                label=f"name: {s['SOURCE_TEXT']}, type: {s['SOURCE_TYPE']}",
                value=s['SOURCE_ALIAS']
            )
            for s in sources
        ]


"""
        Main entrypoint
"""

if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
