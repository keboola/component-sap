import asyncio
import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.csvwriter import ElasticDictWriter

from configuration import Configuration, ConfigurationBase, SyncActionConfiguration
from sap_client.client import SAPClient, SapClientException
from sap_client.sap_snowflake_mapping import SAP_TO_SNOWFLAKE_MAP

# Delta pointer formats that a lookback window can be expressed in. The format is dictated by the
# SAP source, so it is inferred from the pointer already stored in state.
DELTA_POINTER_FORMATS = {8: "%Y%m%d", 14: "%Y%m%d%H%M%S"}
# Guards against sequential id pointers that happen to parse as a date (e.g. 10000101 -> year 1000).
MIN_DELTA_POINTER_YEAR = 1990
MAX_DELTA_LOOKBACK_DAYS = 3650


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
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password
        verify = self._configuration.authentication.verify
        timeout = self._configuration.authentication.timeout
        retries = self._configuration.authentication.retries

        resource_alias = self._configuration.source.resource_alias
        limit = self._configuration.source.limit
        batch_size = self._configuration.source.batch_size
        paging_method = self._configuration.source.paging_method
        sync_type = self._configuration.source.sync_type
        delta_lookback_days = self._validate_delta_lookback_days(self._configuration.source.delta_lookback_days)

        output_table_name = self._configuration.destination.output_table_name
        load_type = self._configuration.destination.load_type
        debug = self._configuration.debug

        temp_dir = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_dir, exist_ok=True)

        statefile_columns = self.state.get(resource_alias, {}).get("columns", [])

        stored_delta_max = self._init_delta(sync_type, resource_alias)

        # The lookback only moves the pointer *sent* to SAP; stored_delta_max stays the floor for
        # what is written back to state at the end of the run.
        previous_delta_max = stored_delta_max
        if stored_delta_max and delta_lookback_days:
            previous_delta_max = self._apply_delta_lookback(stored_delta_max, delta_lookback_days)

        client = SAPClient(
            server_url=server_url,
            username=username,
            password=password,
            destination=temp_dir,
            timeout=timeout,
            retries=retries,
            verify=verify,
            limit=limit,
            batch_size=batch_size,
            delta=previous_delta_max,
            debug=debug,
        )

        output_table_name = output_table_name or resource_alias
        incremental = load_type != "full_load"

        if delta_lookback_days and not incremental:
            logging.warning(
                f"Delta lookback (days) is set to {delta_lookback_days}, but the load type is Full Load, "
                f"so every run overwrites the destination table with just the fetched window. "
                f"Use Incremental Load to keep the previously fetched data."
            )

        out_table = self.create_out_table_definition(name=output_table_name, incremental=incremental)

        try:
            asyncio.run(client.fetch(resource_alias, paging_method))
        except SapClientException as e:
            error_msg = str(e)
            if "TYPE_NOT_FOUND" in error_msg:
                raise UserException(
                    f"Failed to load table {resource_alias} due to invalid data type. "
                    f"Please check if the table structure in SAP is valid."
                )
            else:
                raise UserException(f"An error occurred while fetching resource: {e}")

        files = os.listdir(temp_dir)

        if files:
            with ElasticDictWriter(out_table.full_path, statefile_columns) as wr:
                wr.writeheader()
                for json_file in files:
                    json_file_path = os.path.join(temp_dir, json_file)
                    with open(json_file_path, "r") as file:
                        content = json.load(file)
                        for row in content:
                            wr.writerow(self._ensure_proper_column_names(row))

            out_table = self.add_column_metadata(client, out_table)

            if delta_lookback_days and incremental and not out_table.primary_key:
                raise UserException(
                    f"Delta lookback (days) is set to {delta_lookback_days}, but SAP reports no key columns "
                    f"for resource {resource_alias}. Without a primary key the re-fetched rows would be "
                    f"appended to the destination table instead of updated, creating duplicates. "
                    f"Set Delta lookback (days) to 0 for this source."
                )

            self.write_manifest(out_table)

            self.state.setdefault(resource_alias, {})["columns"] = wr.fieldnames

            # Clean temp folder (for local runs)
            shutil.rmtree(temp_dir)
        else:
            logging.warning(f"No data were fetched for resource {resource_alias}.")

        max_delta_pointer = self._persisted_delta_pointer(client.max_delta_pointer, stored_delta_max)
        if max_delta_pointer:
            self.state.setdefault(resource_alias, {})["delta_max"] = max_delta_pointer
            logging.info(f"Delta pointer for resource {resource_alias} was set to {max_delta_pointer}.")

        self.write_state_file(self.state)

    def _init_delta(self, sync_mode: str, resource_alias: str) -> Union[bool, int, str]:
        """This method initializes delta sync by setting delta pointer to the last value from state file."""
        previous_delta_max = None
        if sync_mode == "incremental_sync":
            previous_delta_max = self.state.get(resource_alias, {}).get("delta_max", False)

            if not previous_delta_max:
                logging.warning(
                    "Delta sync is enabled, but no previous delta pointer was found in state file. "
                    "Full sync will be performed."
                )

        return previous_delta_max

    @staticmethod
    def _validate_delta_lookback_days(lookback_days: int) -> int:
        """Validates the lookback window at the start of the run, before any branch can skip it."""
        if isinstance(lookback_days, bool) or not isinstance(lookback_days, int):
            raise UserException(f"Delta lookback (days) must be a whole number, got '{lookback_days}'.")

        if not 0 <= lookback_days <= MAX_DELTA_LOOKBACK_DAYS:
            raise UserException(
                f"Delta lookback (days) must be between 0 and {MAX_DELTA_LOOKBACK_DAYS}, got {lookback_days}."
            )

        return lookback_days

    @staticmethod
    def _parse_delta_pointer(delta_pointer: Union[int, str], now: datetime) -> Optional[Tuple[datetime, str]]:
        """Parses a delta pointer as a timestamp.

        Returns a (datetime, format) tuple, or None when the pointer is not a timestamp a lookback
        window can be expressed in - a sequential id, for instance. The year range check keeps
        sequential ids that happen to parse as a date (10000101 -> year 1000) out of the timestamp
        branch, where they would be silently corrupted.
        """
        pointer = str(delta_pointer)
        date_format = DELTA_POINTER_FORMATS.get(len(pointer))

        if not date_format or not pointer.isdigit():
            return None

        try:
            parsed = datetime.strptime(pointer, date_format)
        except ValueError:
            return None

        if not MIN_DELTA_POINTER_YEAR <= parsed.year <= now.year + 1:
            return None

        return parsed, date_format

    @classmethod
    def _apply_delta_lookback(
        cls, delta_pointer: Union[int, str], lookback_days: int, now: datetime = None
    ) -> Union[int, str]:
        """Moves the delta pointer sent to SAP back to `now - lookback_days`.

        The pointer returned is never newer than the stored one, so the window fetched is always a
        superset of what the run would fetch without the lookback: a schedule that is behind still
        resumes from where it left off instead of skipping the gap.
        """
        now = now or datetime.now()
        parsed = cls._parse_delta_pointer(delta_pointer, now)

        if parsed is None:
            raise UserException(
                f"Delta lookback (days) is set to {lookback_days}, but the delta pointer stored for this "
                f"source ({delta_pointer}) is not a timestamp. Only YYYYMMDD and YYYYMMDDHHMMSS delta "
                f"pointers can be shifted; set Delta lookback (days) to 0 for this source."
            )

        stored_datetime, date_format = parsed
        shifted = min(stored_datetime, now - timedelta(days=lookback_days))
        new_pointer = shifted.strftime(date_format)

        if shifted == stored_datetime:
            logging.info(
                f"Delta lookback of {lookback_days} day(s) reaches further back than the stored delta "
                f"pointer {delta_pointer}, which is used unchanged so that no data is skipped."
            )
        else:
            logging.info(
                f"Delta lookback of {lookback_days} day(s) moved the delta pointer sent to SAP "
                f"from {delta_pointer} to {new_pointer}."
            )

        return int(new_pointer) if isinstance(delta_pointer, int) else new_pointer

    @staticmethod
    def _persisted_delta_pointer(
        max_delta_pointer: Union[int, str, None], stored_delta_max: Union[int, str, None]
    ) -> Union[int, str, None]:
        """The delta pointer written back to state, floored by the one the run started from.

        A lookback shifts the pointer sent to SAP, and the client seeds its own maximum with that
        shifted value. Without this floor a run that returns no delta pointer of its own (nothing
        changed since the last run) would persist the shifted value, and the next run would shift
        that again - walking the window backwards a little further every run.
        """
        candidates = [value for value in (max_delta_pointer, stored_delta_max) if value]

        if not candidates:
            return None

        return SAPClient.max_timestamp_or_id(candidates)

    @staticmethod
    def add_column_metadata(client: SAPClient, out_table: TableDefinition):
        # TODO: How does adding metadata act when not all columns have metadata set?
        pks = []
        for column in client.metadata:
            col_md = client.metadata.get(column)
            datatype = SAP_TO_SNOWFLAKE_MAP[col_md.get("TYPE")]
            if datatype in ["STRING", "INTEGER", "NUMERIC"]:
                length = str(col_md.get("LENGTH"))
            else:
                length = None
            out_table.table_metadata.add_column_data_type(
                column=column,
                data_type=datatype,
                length=length,
            )

            if col_md.get("KEY"):
                pks.append(column)

        if pks:
            out_table.primary_key = pks
            logging.info(f"Primary key set to {pks}.")

        return out_table

    def _init_configuration(self, sync_act: bool = False) -> None:
        if not sync_act:
            self._configuration = Configuration.load_from_dict(self.configuration.parameters)
            self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        else:
            self._configuration = SyncActionConfiguration.load_from_dict(self.configuration.parameters)
            self.validate_configuration_parameters(SyncActionConfiguration.get_dataclass_required_parameters())

    @staticmethod
    def _ensure_proper_column_names(original_dict: dict):
        """
        Transforms dictionary keys by removing a leading '/' character and replacing
        other '/' characters with '_'.

        Parameters:
        - original_dict: The original dictionary with keys to transform.

        Returns:
        dict: A new dictionary with transformed keys.
        """
        transformed_dict = {}
        for key, value in original_dict.items():
            new_key = key.lstrip("/").replace("/", "_")
            transformed_dict[new_key] = value
        return transformed_dict

    @sync_action("listResources")
    def list_resources(self) -> list[SelectElement]:
        self._init_configuration(sync_act=True)

        server_url = self._configuration.authentication.server_url
        username = self._configuration.authentication.username
        password = self._configuration.authentication.pswd_password
        verify = self._configuration.authentication.verify
        timeout = self._configuration.authentication.timeout
        retries = self._configuration.authentication.retries

        client = SAPClient(
            server_url=server_url,
            username=username,
            password=password,
            destination="",
            timeout=timeout,
            retries=retries,
            verify=verify,
            limit=ConfigurationBase.DEFAULT_LIMIT,
            batch_size=ConfigurationBase.DEFAULT_BATCH_SIZE,
        )

        try:
            sources = asyncio.run(client.list_sources())
        except SapClientException as e:
            raise UserException(f"An error occurred while fetching list of resources: {e}")

        return [
            SelectElement(
                label=f"name: {s['SOURCE_TEXT']}, type: {s['SOURCE_TYPE']}",
                value=s["SOURCE_ALIAS"],
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
