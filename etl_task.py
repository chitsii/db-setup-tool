from db_engine import DBFactory

from typing import Dict, List, Optional


from models.operation import DataSrc, OperationTarget, OperationType
from logger import get_logger


class TaskInterface:
    def __init__(
        self,
        target: OperationTarget,
        operaton: OperationType,
        source: Optional[DataSrc] = None,
    ):
        self.source = source
        self.target = target
        self.operation = operaton

    def run(self):
        raise NotImplementedError()


class Task(TaskInterface):
    def __init__(
        self,
        target: OperationTarget,
        operaton: OperationType,
        source: Optional[DataSrc] = None,
    ):
        super().__init__(target, operaton, source)
        self.logger = get_logger(__name__)

    def run(self):
        self.db_engine = DBFactory.get_engine(self.target)
        if self.source:
            self.logger.info(f"read data from {self.source.location}")
            raw_data = self.source.location.read()
            self.data = self.source.format.parse(raw_data)

        # 実行
        match self.operation:
            case OperationType.RELOAD:
                self.reload_table(self.data, self.target.table_name)
            case OperationType.TRUNCATE:
                self.truncate_table(self.target.table_name)
            case OperationType.INSERT:
                self.insert_into_table(self.data, self.target.table_name)
            case OperationType.UPSERT:
                self.upsert_into_table(self.data, self.target.table_name)
            case OperationType.DELETE:
                self.delete_from_table(self.data, self.target.table_name)
            case _:
                raise NotImplementedError()

    def reload_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.truncate(table_name)
            db.insert(table_name, data)
            db.commit()

    def truncate_table(self, table_name):
        with self.db_engine as db:
            db.truncate(table_name)
            db.commit()

    def insert_into_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.insert(table_name, data)
            db.commit()

    def upsert_into_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.upsert(table_name, data)
            db.commit()

    def delete_from_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.delete(table_name, data)
            db.commit()
