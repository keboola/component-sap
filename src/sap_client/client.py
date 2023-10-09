import asyncio
import json
import logging
import os
import uuid

import httpx
from keboola.http_client import AsyncHttpClient


class SapClientException(Exception):
    pass


BATCH_SIZE = 5
DEFAULT_LIMIT = 10_000


class SAPClient(AsyncHttpClient):
    DATA_SOURCES_ENDPOINT = "DATA_SOURCES"
    METADATA_ENDPOINT = "$metadata"

    def __init__(self, server_url: str, username: str, password: str, destination: str,  limit: int = DEFAULT_LIMIT,
                 verify: bool = True):
        auth = (username, password)
        default_headers = {'Accept-Encoding': 'gzip, deflate'}

        super().__init__(server_url, auth=auth, default_headers=default_headers, retries=2,
                         retry_status_codes=[503, 500], verify_ssl=verify, max_requests_per_second=BATCH_SIZE)

        self.destination = destination
        self.verify = verify
        self.limit = limit
        self.stop = False
        self.metadata = {}

    async def list_sources(self):
        r = await self._get(self.DATA_SOURCES_ENDPOINT)
        sources = r.get("DATA_SOURCES", None)

        if sources:
            sources = [
                {'SOURCE_ALIAS': source['SOURCE_ALIAS'],
                 'SOURCE_TEXT': source['SOURCE_TEXT'],
                 'PAGING': source['PAGING'],
                 'SOURCE_TYPE': source['SOURCE_TYPE'],
                 'DELTA': source['DELTA']
                 } for source in sources]

        return sources

    async def fetch(self, resource_alias):
        sources = await self.list_sources()
        is_source_present = any(s['SOURCE_ALIAS'] == resource_alias for s in sources)
        if is_source_present:
            logging.info(f"{resource_alias} resource will be fetched.")
        else:
            raise SapClientException(f"{resource_alias} resource is not available.")

        metadata = await self._get_resource_metadata(resource_alias)
        self.metadata = self.parse_metadata(metadata)

        if metadata.get("PAGING") is True:
            logging.info(f"Resource {resource_alias} supports paging.")
            async for page in self._fetch_paging(resource_alias):
                await self._store_results(page, resource_alias)
        else:
            logging.info(f"Resource {resource_alias} does not support paging. "
                         f"The component will try to fetch the data in one request.")
            await self._fetch_full(resource_alias)

    async def _fetch_paging(self, resource_alias: str, page: int = 0):
        params = {
            "page": page if page else 1,
            "limit": self.limit
        }
        tasks = []

        while not self.stop:
            for _ in range(BATCH_SIZE-1):
                endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
                tasks.append(self._get_and_process(endpoint, params.copy()))

                params["page"] += 1

            # Wait for all tasks to complete and iterate over results.
            results = await asyncio.gather(*tasks)
            for result in results:
                yield result

            tasks.clear()

    async def _fetch_full(self, resource_alias: str):
        endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
        r = await self._get(endpoint)
        entities = r.get("DATA_SOURCE", None).get("ENTITIES", [])
        if entities:
            columns_specification = entities[0].get("COLUMNS")
            columns = self._get_columns(columns_specification)
            rows = entities[0].get("ROWS")  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            if rows:
                return self._process_result(rows, columns)
        return []

    async def _store_results(self, results: list[dict], name: str) -> None:
        if not self.destination:
            logging.info("Destination not set, results will not be stored.")
            return

        if results:
            output_filename = os.path.join(self.destination, f"{name}_{uuid.uuid4()}.json")
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)

    async def _get_and_process(self, endpoint, params):
        """Helper method for async processing used with resources that support paging."""
        r = await self._get(endpoint, params=params)
        data_source = r.get("DATA_SOURCE", {})
        entities = data_source.get("ENTITIES", [])

        if entities:
            columns_specification = entities[0].get("COLUMNS")
            columns = self._get_columns(columns_specification)
            rows = entities[0].get("ROWS")  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            if rows:
                return self._process_result(rows, columns)
            else:
                self.stop = True

        return None

    async def _get_resource_metadata(self, resource):
        endpoint = f"{self.DATA_SOURCES_ENDPOINT}/{resource}/{self.METADATA_ENDPOINT}"
        r = await self._get(endpoint)
        return r.get("DATA_SOURCE", None)

    @staticmethod
    def _process_result(rows: list[dict], columns: list):
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _get_columns(columns_specification: list):
        return [item['COLUMN_ALIAS'] for item in sorted(columns_specification, key=lambda x: x['POSITION'])]

    async def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        try:
            return await self.get(endpoint, params=params)
        except httpx.ConnectError as e:
            raise SapClientException(f"Cannot connect to {endpoint}, exception: {e}")

    @staticmethod
    def _join_url_parts(*parts):
        return "/".join(str(part).strip("/") for part in parts)

    @staticmethod
    def get_ordered_columns(columns_specification: list):
        sorted_columns = sorted(columns_specification, key=lambda x: x['POSITION'])
        return {col['COLUMN_ALIAS']: col for col in sorted_columns}

    def parse_metadata(self, metadata: dict):
        entity = metadata['ENTITIES'][0]
        return self.get_ordered_columns(entity['COLUMNS'])
