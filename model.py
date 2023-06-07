from collections import namedtuple
from typing import List, Literal, Optional, Tuple
from data_reader import ReaderInterface
from data_formatter import FormatterInterface

# DataSrc = namedtuple("SrcPath", ["path", "reader_option", "formatter_option"])
OperationTarget = namedtuple(
    "OperationTarget", ["engine_option", "db_name", "table_name"]
)
Operation = Literal["reload", "truncate", "insert", "upsert", "delete"]


class ColumnSchema:
    def __init__(
        self, name: str, data_type: str, is_nullable: bool, is_primary_key: bool
    ):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.is_primary_key = is_primary_key

    @staticmethod
    def from_dict(data: dict) -> "ColumnSchema":
        return ColumnSchema(
            name=data["name"],
            data_type=data["data_type"],
            is_nullable=data["is_nullable"],
            is_primary_key=data["is_primary_key"],
        )


class TableSchema:
    def __init__(self, table_name: str, column_schemas: List[ColumnSchema]):
        self.name = table_name
        self.column_schemas = column_schemas

    @staticmethod
    def from_dict(data: dict) -> "TableSchema":
        table_name = data["table_name"]
        column_schemas = [
            ColumnSchema.from_dict(schema) for schema in data["column_schemas"]
        ]
        return TableSchema(table_name=table_name, column_schemas=column_schemas)

    def get_column_names(self) -> Tuple[str]:
        return tuple(schema.name for schema in self.column_schemas)

    def get_column_schema(self, column_name: str) -> Optional[ColumnSchema]:
        for s in self.column_schemas:
            if s.name == column_name:
                return s
        return None

    def get_pk_column_names(self) -> Tuple[str]:
        return tuple(
            schema.name for schema in self.column_schemas if schema.is_primary_key
        )


class DataSrc:
    def __init__(
        self,
        location: ReaderInterface,
        formatter: FormatterInterface,
    ):
        self.location = location
        self.format = formatter
