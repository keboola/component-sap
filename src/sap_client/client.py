from http.client import HTTPException

from keboola.http_client import HttpClient


class SapClientException(Exception):
    pass


class SAPClient(HttpClient):
    LIMIT = 10000
    DATA_SOURCES_ENDPOINT = "DATA_SOURCES"
    METADATA_ENDPOINT = "$metadata"

    def __init__(self, server_url: str, username: str, password: str, verify: bool = True):
        auth = (username, password)
        default_headers = {'Accept-Encoding': 'gzip, deflate'}

        super().__init__(server_url, auth=auth, default_http_header=default_headers, max_retries=2,
                         status_forcelist=(503, 500))

        self.verify = verify

    def list_sources(self):
        r = self._get(self.DATA_SOURCES_ENDPOINT)
        sources = r.get("DATA_SOURCES", None)
        if sources:
            sources = [{'SOURCE_ALIAS': source['SOURCE_ALIAS'], 'SOURCE_TEXT': source['SOURCE_TEXT'],
                        'PAGING': source['PAGING']} for source in sources]

        return sources

    def fetch(self, resource_alias):
        metadata = self._get_resource_metadata(resource_alias)

        columns = self._get_columns(metadata)
        # The following is just a temporary workaround for PAGING not being displayed right in the metadata
        if metadata.get("PAGING") is True:
            for page in self._fetch_paging(resource_alias, columns):
                yield page
        else:
            yield self._fetch_full(resource_alias, columns)

    def _fetch_paging(self, resource_alias, columns: list, page: int = 0):
        params = {
            "page": page if page else 0,
            "limit": self.LIMIT
        }
        while True:
            endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
            r = self._get(endpoint, params=params)
            entities = r.get("DATA_SOURCE", None).get("ENTITIES", [])
            if entities:
                rows = entities[0]["ROWS"]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
                if rows:
                    yield [dict(zip(columns, row)) for row in rows]
                    params["page"] += 1
                else:
                    break

    def _fetch_full(self, resource_alias: str, columns: list):
        endpoint = self._join_url_parts(self.DATA_SOURCES_ENDPOINT, resource_alias)
        r = self._get(endpoint)
        entities = r.get("DATA_SOURCE", None).get("ENTITIES", [])
        if entities:
            rows = entities[0]["ROWS"]  # ONLY ONE ENTITY FOR ONE DATA SOURCE IS SUPPORTED
            return [dict(zip(columns, row)) for row in rows]
        return []

    def _get_columns(self, response):
        try:
            columns = response["ENTITIES"][0]["COLUMNS"]
        except KeyError:
            raise SapClientException(f"Column metadata are not available, cannot store data. Response: {response}")

        return [item['COLUMN_ALIAS'] for item in sorted(columns, key=lambda x: x['POSITION'])]

    def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        r = self.get_raw(endpoint, params=params, verify=self.verify)
        try:
            r.raise_for_status()
        except HTTPException as e:
            raise SapClientException(f"Request to {endpoint} failed, exception: {e}") from e

        return r.json()

    @staticmethod
    def _join_url_parts(*parts):
        return "/".join(str(part).strip("/") for part in parts)

    def _get_resource_metadata(self, resource):
        endpoint = f"{self.DATA_SOURCES_ENDPOINT}/{resource}/{self.METADATA_ENDPOINT}"
        r = self._get(endpoint)
        source = r.get("DATA_SOURCE", None)
        return source
