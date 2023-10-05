"""
Template Component main class.

"""
import logging

from keboola.component.base import ComponentBase, sync_action
from keboola.csvwriter import ElasticDictWriter
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from sap_client.client import SAPClient, SapClientException
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

        client = SAPClient(server_url, username, password, limit, verify=False)

        try:
            sources = client.list_sources()
        except SapClientException as e:
            raise UserException(f"Cannot list SAP resources, exception: {e}")

        for resource in resource_alias:

            is_source_present = any(s['SOURCE_ALIAS'] == resource for s in sources)

            if is_source_present:
                logging.info(f"{resource} resource will be fetched.")
            else:
                raise UserException(f"{resource_alias} resource is not available.")

            out_table = self.create_out_table_definition(resource)
            with ElasticDictWriter(out_table.full_path, []) as wr:
                try:
                    for page in client.fetch(resource):
                        wr.writerows(page)
                except SapClientException as e:
                    raise UserException(f"Cannot fetch data from resource {resource}, exception: {e}") from e

            self.write_manifest(out_table)

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    @sync_action("listResources")
    def list_resources(self) -> list[SelectElement]:
        self._init_configuration()

        server_url = self._configuration.authentication.server_url
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password

        client = SAPClient(server_url, username, password, verify=False)
        sources = client.list_sources()

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
