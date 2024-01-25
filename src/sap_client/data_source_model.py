from dataclasses import dataclass
from typing import List


@dataclass
class Column:
    POSITION: int
    COLUMN_ALIAS: str
    COLUMN_TEXT: str
    TYPE: str
    LENGTH: int
    DECIMALS: int
    KEY: bool


@dataclass
class Entity:
    ENTITY_ALIAS: str
    ENTITY_TEXT: str
    ENTITY_TYPE: str
    DELTA_POINTER: str
    COLUMNS: List[Column]


@dataclass
class DataSource:
    SOURCE_ALIAS: str
    SOURCE_TEXT: str
    SOURCE_TYPE: str
    PAGING: bool
    DELTA: bool
    ENTITIES: List[Entity]

    @classmethod
    def from_dict(cls, data: dict):
        data_uppercase = {key.upper(): value for key, value in data.items()}
        return cls(**data_uppercase)

    @staticmethod
    def _get_ordered_columns(columns_specification: Entity):
        columns = columns_specification.get('COLUMNS', [])
        sorted_columns = sorted(columns, key=lambda x: x['POSITION'])
        return {col['COLUMN_ALIAS']: col for col in sorted_columns}

    @property
    def metadata(self):
        entity: Entity = self.ENTITIES[0]
        return self._get_ordered_columns(entity)

