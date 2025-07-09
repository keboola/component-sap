import asyncio
import json
import logging
import os
import uuid
from functools import wraps
from typing import Union

import httpx
from keboola.http_client import AsyncHttpClient

from .data_source_model import DataSource


class SapClientException(Exception):
    pass


def set_timeout(timeout):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            original_timeout = self.client.timeout
            self.client.timeout = timeout
            try:
                result = await func(self, *args, **kwargs)
            finally:
                self.client.timeout = original_timeout
            return result

        return wrapper

    return decorator


class SAPClient(AsyncHttpClient):
    DATA_SOURCES_ENDPOINT = "DATA_SOURCES"
    METADATA_ENDPOINT = "$metadata"

    def __init__(
            self,
            server_url: str,
            username: str,
            password: str,
            destination: str,
            timeout: int,
            retries: int,
            verify: bool,
            limit: int,
            batch_size: int,
            delta: Union[bool, int] = False,
            debug=False,
    ):
        """Implements SAP client for fetching data from SAP Data Sources.
        Args:
            server_url: SAP server url.
            username: Username for authentication.
            password: Password for authentication.
            destination: Destination folder for storing fetched data.
            limit: Limit for one request.
            delta: Delta pointer.
            batch_size: Number of parallel requests.
            verify: Verify SSL certificate.
        """
        auth = (username, password)
        default_headers = {"Accept-Encoding": "gzip, deflate"}

        super().__init__(
            server_url,
            auth=auth,
            default_headers=default_headers,
            retries=retries,
            retry_status_codes=[0, 500, 503],
            timeout=timeout,
            verify_ssl=verify,
        )

        self.destination = destination

        self.timeout = timeout
        self.verify = verify

        self.limit = limit
        self.batch_size = batch_size

        self.delta = delta
        self.delta_values = []
        self.debug = debug

        if self.delta:
            logging.info(f"Delta sync is enabled, delta pointer: {self.delta}.")
            self.delta_values.append(self.delta)

        self.stop = False
        self.metadata = {}

    @set_timeout(5)
    async def list_sources(self):
        try:
            r = await self._fetch(self.DATA_SOURCES_ENDPOINT)
        except (httpx.ConnectError, httpx.ConnectTimeout):
            raise SapClientException("Unable to list sources. Check the connection to the server.")

        sources = r.get("DATA_SOURCES", [])

        if sources:
            sources = [
                {
                    "SOURCE_ALIAS": source["SOURCE_ALIAS"],
                    "SOURCE_TEXT": source["SOURCE_TEXT"],
                    "PAGING": source["PAGING"],
                    "SOURCE_TYPE": source["SOURCE_TYPE"],
                    "DELTA": source["DELTA"],
                }
                for source in sources
            ]

        return sources

    async def fetch(self, resource_alias: str, paging_method: str = "offset"):
        try:
            resource_info = await self._get_resource_metadata(resource_alias)
            data_source = DataSource.from_dict(resource_info)
            self.metadata = data_source.metadata

            await self.check_delta_support(resource_alias, data_source)

            if self.delta:
                await self.fetch_and_store_full(resource_alias)
            elif data_source.PAGING:
                await self.fetch_with_paging(resource_alias, paging_method)
            else:
                await self.fetch_and_store_full(resource_alias)
                logging.info(
                    f"Resource {resource_alias} does not support paging. "
                    f"The component will try to fetch the data in one request."
                )
        except SapClientException as e:
            logging.error(f"Failed to load metadata for table {resource_alias}: {str(e)}")
            raise SapClientException(f"Failed to load metadata for table {resource_alias}: {str(e)}")

    async def check_delta_support(self, resource_alias: str, data_source: DataSource):
        if self.delta and not data_source.DELTA:
            raise SapClientException(f"Resource {resource_alias} does not support delta function.")

    async def fetch_and_store_full(self, resource_alias: str):
        page = await self._fetch_full(resource_alias)
        await self._store_results(page, resource_alias)

    async def fetch_with_paging(self, resource_alias: str, paging_method: str):
        logging.info(f"Resource {resource_alias} supports paging.")

        if paging_method == "offset":
            async for page in self._fetch_paging_offset(resource_alias):
                await self._store_results(page, resource_alias)
        elif paging_method == "key":
            async for page in self._fetch_paging_key(resource_alias):
                await self._store_results(page, resource_alias)
        else:
            raise SapClientException(f"Unsupported paging method: {paging_method}")

    async def _fetch_paging_offset(self, resource_alias: str, page: int = 0):
        params = {"page": page if page else 1, "limit": self.limit}
        tasks = []

        while not self.stop:
            for _ in range(self.batch_size):
                endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
                tasks.append(self._get_and_process(endpoint, params.copy()))
                params["page"] += 1

            # Wait for all tasks to complete and iterate over results.
            results = await asyncio.gather(*tasks)
            for result in results:
                yield result

            tasks.clear()

    async def _fetch_paging_key(self, resource_alias: str):
        params = {"limit": self.limit}

        # get blocks
        endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias, "$key_blocks")
        r = await self._fetch(endpoint, params=params)
        blocks = r.get("DATA_SOURCE", {}).get("KEY_BLOCKS")
        if not blocks:
            raise SapClientException("Unable to obtain key blocks.")  # TODO: fallback to offset paging

        tasks = []

        for block in blocks:
            params = {"key_min": block.get("KEY_MIN"), "key_max": block.get("KEY_MAX")}
            endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
            tasks.append(self._get_and_process(endpoint, params.copy()))

            if len(tasks) == self.batch_size:
                results = await asyncio.gather(*tasks)
                for result in results:
                    yield result
                tasks.clear()

        # Process any remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                yield result

    async def _fetch_full(self, resource_alias: str):
        """Fetches all data from resource_alias. Also takes into account delta pointer if set. In such case only fetches
        data that were changed since last fetch."""

        endpoint = self._get_data_sources_endpoint(resource_alias)
        params = self._get_request_params({})

        response = await self._fetch(endpoint, params=params)
        entities = response.get("DATA_SOURCE", {}).get("ENTITIES", [])

        if entities:
            entity = entities[0]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED

            columns_specification = entity.get("COLUMNS")
            columns = self._get_columns(columns_specification)

            self._set_delta_pointer(entity)

            rows = entity.get("ROWS")
            if rows:
                return self._process_result(rows, columns)

        return []

    def _get_data_sources_endpoint(self, resource_alias: str):
        if self.delta:
            return self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias, "$delta")
        else:
            return self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)

    def _get_request_params(self, params: dict) -> dict:
        if self.delta:
            params["delta_pointer"] = self.delta

        return params

    async def _store_results(self, results: list[dict], name: str) -> None:
        if not self.destination:
            logging.warning("Destination not set, results will not be stored.")
            return

        if results:
            output_filename = os.path.join(self.destination, f"{name}_{uuid.uuid4()}.json")
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)

    async def _get_and_process(self, endpoint, params):
        """Helper method for async processing used with resources that support paging."""
        r = await self._fetch(endpoint, params=params)
        data_source = r.get("DATA_SOURCE", {})
        entities = data_source.get("ENTITIES", [])

        if entities:
            entity = entities[0]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            columns_specification = entity.get("COLUMNS")

            self._set_delta_pointer(entity)

            columns = self._get_columns(columns_specification)
            rows = entity.get("ROWS")
            if rows:
                return self._process_result(rows, columns)
            else:
                self.stop = True

        return None

    def _set_delta_pointer(self, entity: dict) -> None:
        if delta_pointer := entity.get("DELTA_POINTER"):
            logging.debug(f"Delta pointer received: {delta_pointer}")
            try:
                delta_pointer = int(delta_pointer)
            except ValueError:
                try:
                    delta_pointer = float(delta_pointer)
                except ValueError:
                    raise SapClientException(
                        f"Only integer and float {delta_pointer} values are supported. "
                        f"Delta pointer received: {delta_pointer}"
                    )
            self.delta_values.append(delta_pointer)
        else:
            logging.debug("No delta pointer received.")

    async def _get_resource_metadata(self, resource) -> dict:
        try:
            endpoint = f"{self.DATA_SOURCES_ENDPOINT}/{resource}/{self.METADATA_ENDPOINT}"
            r = await self._fetch(endpoint)
            return r.get("DATA_SOURCE")
        except SapClientException as e:
            logging.error(f"Failed to fetch metadata for resource {resource}: {str(e)}")
            raise SapClientException(f"Failed to fetch metadata for resource {resource}: {str(e)}")

    @staticmethod
    def _process_result(rows: list[dict], columns: list):
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _get_columns(columns_specification: list):
        return [item["COLUMN_ALIAS"] for item in sorted(columns_specification, key=lambda x: x["POSITION"])]

    async def _fetch(self, endpoint: str, params=None):
        """Fetches data"""
        if params is None:
            params = {}

        if self.debug:
            # workaround for debug logging not working properly in AsyncClient
            logging.debug(f"Fetching data from {endpoint} with params: {params}")

        try:
            return await self._get(endpoint, params=params)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            raise SapClientException(f"Failed to fetch data from endpoint {endpoint}: {str(e)}")
        except httpx.ReadTimeout as e:
            raise SapClientException(f"Request timed out after all retry attempts for endpoint {endpoint}: {str(e)}")
        except Exception as e:
            raise SapClientException(f"Failed to fetch data from {endpoint}: {str(e)}")

    async def _get(self, endpoint: str, params=None) -> dict:
        return await self.get(endpoint, params=params)

    @staticmethod
    def _join_url_parts(*parts) -> str:
        return "/".join(str(part).strip("/") for part in parts)

    @property
    def max_delta_pointer(self) -> Union[int, str, None]:
        logging.debug(f"Client Delta values: {self.delta_values}")
        return self._max_timestamp_or_id(self.delta_values)

    @staticmethod
    def _max_timestamp_or_id(values: list):
        if not values:
            return None
        # sometimes can come different length of values, so we need to normalize them
        max_length = max(len(str(value)) for value in values)
        normalized_data = [str(value).ljust(max_length, "0") for value in values]
        max_normalized = max(normalized_data)
        max_value = values[normalized_data.index(max_normalized)]
        return max_value
