import asyncio
import csv
import httpx
import logging
import os.path
import xml.dom.minidom
import json

from keboola.http_client import AsyncHttpClient


class SapErpClientException(Exception):
    pass


class SAPClient(AsyncHttpClient):
    PAGE_SIZE = 1000

    def __init__(self, server_url: str, username: str, password: str, catalog_service_url: str = "",
                 resource_url: str = ""):

        auth = (username, password)
        default_headers = {'Accept-Encoding': 'gzip, deflate'}
        super().__init__(server_url, auth=auth, default_headers=default_headers, retries=2,
                         retry_status_codes=[503, 500], max_requests_per_second=30)

    async def get_metadata(self, catalog_service_endpoint: str, temp_folder: str):
        service_collection = await self.get_service_collection(catalog_service_endpoint)
        metadata = await self._get_metadata(service_collection)
        self.extract_metadata(metadata, temp_folder)

    async def get_service_collection(self, catalog_service_endpoint: str) -> dict:
        params = {
            "$format": "json"
        }
        r = await self.get(catalog_service_endpoint, params=params)
        return r["d"]["results"]

    async def _get_metadata(self, service_collection):
        metadata_responses = []

        async def fetch_metadata(metadata_url, result):
            try:
                r = await self.get_raw(metadata_url)
                additional_info = {
                    'Description': result['Description'],
                    'Title': result['Title'],
                    'UpdatedDate': result['UpdatedDate'],
                    'IsSapService': result['IsSapService'],
                    'Author': result['Author'],
                    'TechnicalServiceName': result['TechnicalServiceName'],
                    'ServiceUrl': result['ServiceUrl']
                }
                metadata_responses.append((result['ID'], r.text, additional_info))
            except httpx.HTTPStatusError:
                logging.error(f"Cannot retrieve metadata for {metadata_url}")
            except Exception as e:
                logging.error(f"An error occurred while retrieving metadata for {metadata_url}: {str(e)}")

        tasks = []
        for result in service_collection:
            metadata_url = result['MetadataUrl']
            tasks.append(fetch_metadata(metadata_url, result))

        await asyncio.gather(*tasks)

        return metadata_responses

    @staticmethod
    def extract_metadata(metadata_responses, temp_folder):
        for service_name, metadata, additional_info in metadata_responses:
            xml_dom = xml.dom.minidom.parseString(metadata)
            entity_types = xml_dom.getElementsByTagName('EntityType')

            for entity_type in entity_types:
                entity_type_name = entity_type.getAttribute('Name')

                # Extract primary keys
                key_elements = entity_type.getElementsByTagName('Key')
                primary_keys = []
                if key_elements:
                    key_element = key_elements[0]
                    for key_property_ref in key_element.getElementsByTagName('PropertyRef'):
                        primary_keys.append(key_property_ref.getAttribute('Name'))

                properties = entity_type.getElementsByTagName('Property')

                properties_dict = {}
                for column in properties:
                    name = column.getAttribute('Name')
                    datatype = column.getAttribute('Type')

                    is_primary_key = name in primary_keys
                    properties_dict[name] = {'type': datatype, 'is_primary_key': is_primary_key}

                if additional_info:
                    for key, value in additional_info.items():
                        properties_dict[key] = value

                filename = service_name.replace("/", "__") + "-" + entity_type_name
                json_path = os.path.join(temp_folder, f'{filename}.json')

                with open(json_path, 'w') as f:
                    json.dump(properties_dict, f, indent=2)

    async def get_count(self, resource_endpoint) -> int:
        endpoint = resource_endpoint+"/$count"
        try:
            r = await self.get(endpoint)
        except httpx.ConnectError as e:
            raise SapErpClientException(f"Cannot get count from {endpoint}.") from e
        return r

    async def get_data(self, resource_endpoint, output_file):
        count = await self.get_count(resource_endpoint)
        pages = self.make_pages(count)
        await self.get_pages(resource_endpoint, pages, output_file)

    def make_pages(self, total_items: int) -> list:
        pages = [num for num in range(0, total_items + 1, self.PAGE_SIZE)]
        pages[-1] = max(pages[-2] + 1, total_items)
        return pages

    async def fetch_page(self, endpoint, page_nr):
        params = {
            "$top": self.PAGE_SIZE,
            "$format": "json",
            "skip": page_nr
        }
        try:
            r = await self.get(endpoint, params=params)
            return r
        except httpx.HTTPStatusError as e:
            raise SapErpClientException(e) from e

    async def process_page(self, endpoint, page_nr, filepath):
        result = await self.fetch_page(endpoint, page_nr)
        await self.save_to_csv(result, filepath)

    async def get_pages(self, resource_endpoint: str, pages: list, filepath: str):

        tasks = []
        for page in pages:
            tasks.append(self.process_page(resource_endpoint, page, filepath))

        await asyncio.gather(*tasks)

        print("Data written to CSV.")

    @staticmethod
    async def save_to_csv(result, filepath):
        data = result["d"]["results"]
        fieldnames = [field for field in data[0].keys() if field != '__metadata']

        file_exists = os.path.isfile(filepath)
        mode = "a" if file_exists else "w"

        with open(filepath, mode, newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames)

            if not file_exists:
                writer.writeheader()

            for row in data:
                row.pop('__metadata', None)
                writer.writerow(row)
