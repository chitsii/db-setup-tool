from enum import Enum, auto

from data_reader import ReaderInterface
from data_formatter import FormatterInterface


class DataSrc:
    def __init__(
        self,
        location: ReaderInterface,
        formatter: FormatterInterface,
    ):
        self.location = location
        self.format = formatter


class OperationTarget:
    def __init__(self, engine_option, db_name, table_name):
        self.engine_option = engine_option
        self.db_name = db_name
        self.table_name = table_name


class OperationType(Enum):
    RELOAD = auto()
    TRUNCATE = auto()
    INSERT = auto()
    UPSERT = auto()
    DELETE = auto()
