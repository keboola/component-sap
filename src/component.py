import asyncio
import json
import logging
import os
import shutil

from keboola.component.base import ComponentBase, sync_action
from keboola.csvwriter import ElasticDictWriter
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.component.dao import TableDefinition

from sap_client.client import SAPClient, SapClientException
from sap_client.sap_snowflake_mapping import SAP_TO_SNOWFLAKE_MAP
from configuration import Configuration, SyncActionConfiguration


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._configuration: Configuration
        self.state = None

    def run(self):
        """
        Main execution code
        """
        self._init_configuration()
        self.state = self.get_state_file()

        server_url = self._configuration.authentication.server_url
        resource_alias = self._configuration.source.resource_alias
        limit = self._configuration.source.limit
        batch_size = self._configuration.source.batch_size
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password
        paging_method = self._configuration.source.paging_method
        delta = self._configuration.source.delta

        temp_dir = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_dir, exist_ok=True)

        statefile_columns = self.state.get(resource_alias, {}).get("columns", [])

        previous_delta_max = self._init_delta(delta, resource_alias)

        client = SAPClient(server_url=server_url,
                           username=username,
                           password=password,
                           destination=temp_dir,
                           limit=limit,
                           batch_size=batch_size,
                           delta=previous_delta_max,
                           verify=False)

        out_table = self.create_out_table_definition(resource_alias)

        try:
            asyncio.run(
                client.fetch(resource_alias, paging_method)
            )
        except SapClientException as e:
            raise UserException(f"An error occurred while fetching resource: {e}")

        files = os.listdir(temp_dir)

        if files:
            with ElasticDictWriter(out_table.full_path, statefile_columns) as wr:
                wr.writeheader()
                for json_file in files:
                    json_file_path = os.path.join(temp_dir, json_file)
                    with open(json_file_path, 'r') as file:
                        content = json.load(file)
                        for row in content:
                            wr.writerow(self._ensure_proper_column_names(row))

            out_table = self.add_column_metadata(client, out_table)
            self.write_manifest(out_table)

            self.state.setdefault(resource_alias, {})["columns"] = wr.fieldnames

            # Clean temp folder (for local runs)
            shutil.rmtree(temp_dir)
        else:
            logging.warning(f"No data were fetched for resource {resource_alias}.")

        self.state[resource_alias]["delta_max"] = client.max_delta_pointer
        logging.info(f"Delta pointer for resource {resource_alias} was set to {client.max_delta_pointer}.")

        self.write_state_file(self.state)

    def _init_delta(self, delta: bool, resource_alias: str) -> int:
        previous_delta_max = None
        if delta:
            previous_delta_max = self.state.get(resource_alias, {}).get("delta_max", None)

            if not previous_delta_max:
                logging.warning("Delta sync is enabled, but no previous delta pointer was found in state file. "
                                "Full sync will be performed.")
                previous_delta_max = False

        return previous_delta_max

    @staticmethod
    def add_column_metadata(client: SAPClient, out_table: TableDefinition):
        # TODO: How does adding metadata act when not all columns have metadata set?
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

    def _init_configuration(self, sync_act: bool = False) -> None:
        if not sync_act:
            self._configuration = Configuration.load_from_dict(self.configuration.parameters)
            self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        else:
            self._configuration = SyncActionConfiguration.load_from_dict(self.configuration.parameters)
            self.validate_configuration_parameters(SyncActionConfiguration.get_dataclass_required_parameters())

    @staticmethod
    def _ensure_proper_column_names(original_dict):
        """
        Transforms dictionary keys by removing a leading '/' character and replacing
        other '/' characters with '_'.

        Parameters:
        - original_dict (dict): The original dictionary with keys to transform.

        Returns:
        dict: A new dictionary with transformed keys.
        """
        transformed_dict = {}
        for key, value in original_dict.items():
            new_key = key.lstrip('/').replace('/', '_')
            transformed_dict[new_key] = value
        return transformed_dict

    @sync_action("listResources")
    def list_resources(self) -> list[SelectElement]:
        self._init_configuration(sync_act=True)

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
