import asyncio
import json
import logging
import os
import uuid

from keboola.http_client import AsyncHttpClient


class SapClientException(Exception):
    pass


DEFAULT_LIMIT = 10_000


class SAPClient(AsyncHttpClient):
    DATA_SOURCES_ENDPOINT = "DATA_SOURCES"
    METADATA_ENDPOINT = "$metadata"

    def __init__(self, server_url: str, username: str, password: str, destination: str,  limit: int = DEFAULT_LIMIT,
                 verify: bool = True):
        auth = (username, password)
        default_headers = {'Accept-Encoding': 'gzip, deflate'}

        super().__init__(server_url, auth=auth, default_headers=default_headers, retries=2,
                         retry_status_codes=[503, 500], verify_ssl=verify)

        self.destination = destination
        self.verify = verify
        self.limit = limit
        self.stop = False

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
        columns = self._get_columns(metadata)

        if metadata.get("PAGING") is True:

            logging.info(f"Resource {resource_alias} supports paging.")

            async for page in self._fetch_paging(resource_alias, columns):
                await self._store_results(page, resource_alias)
        else:
            await self._fetch_full(resource_alias, columns)

    async def _store_results(self, results: list[dict], name: str) -> None:
        if not self.destination:
            logging.info("Destination not set, results will not be stored.")
            return

        if results:
            output_filename = os.path.join(self.destination, f"{name}_{uuid.uuid4()}.json")
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)

    async def _fetch_paging(self, resource_alias: str, columns: list, page: int = 0):
        params = {
            "page": page if page else 1,
            "limit": self.limit
        }
        tasks = []

        while not self.stop:
            for _ in range(9):
                endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
                tasks.append(self._get_and_process(endpoint, params.copy(), columns))

                params["page"] += 1

            # Wait for all tasks to complete and iterate over results.
            results = await asyncio.gather(*tasks)
            for result in results:
                yield result

            tasks.clear()

    # Separate _get and _process_result logic into its own async function.
    async def _get_and_process(self, endpoint, params, columns):
        r = await self._get(endpoint, params=params)
        entities = r.get("DATA_SOURCE", {}).get("ENTITIES", [])
        if entities:
            rows = entities[0]["ROWS"]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            if rows:
                return await self._process_result(rows, columns)
            else:
                self.stop = True

        return None

    @staticmethod
    async def _process_result(rows: list[dict], columns: list):
        return [dict(zip(columns, row)) for row in rows]

    async def _fetch_full(self, resource_alias: str, columns: list):
        endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
        r = await self._get(endpoint)
        entities = r.get("DATA_SOURCE", None).get("ENTITIES", [])
        if entities:
            rows = entities[0]["ROWS"]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            return [dict(zip(columns, row)) for row in rows]
        return []

    @staticmethod
    def _get_columns(response):
        try:
            columns = response["ENTITIES"][0]["COLUMNS"]
        except KeyError:
            raise SapClientException(f"Column metadata are not available, cannot store data. Response: {response}")

        return [item['COLUMN_ALIAS'] for item in sorted(columns, key=lambda x: x['POSITION'])]

    async def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        return await self.get(endpoint, params=params)

    @staticmethod
    def _join_url_parts(*parts):
        return "/".join(str(part).strip("/") for part in parts)

    async def _get_resource_metadata(self, resource):
        endpoint = f"{self.DATA_SOURCES_ENDPOINT}/{resource}/{self.METADATA_ENDPOINT}"
        r = await self._get(endpoint)
        source = r.get("DATA_SOURCE", None)
        return source
