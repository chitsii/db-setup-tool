
from db_engine import DBFactory

from typing import Dict, List, Optional


from model import DataSrc, OperationTarget, Operation
from logger import get_logger


class TaskInterface:
    def __init__(
        self,
        priority: int,
        source: Optional[DataSrc],
        target: OperationTarget,
        operaton: Operation,
    ):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()


class Task(TaskInterface):
    def __init__(
        self,
        source: Optional[DataSrc],
        target: OperationTarget,
        operaton: Operation,
    ):
        self.source = source
        self.target = target
        self.operation = operaton
        self.logger = get_logger(__name__)

    def run(self):
        if self.source:
            self.logger.info(f"read data from {self.source.location}")
            raw_data = self.source.location.read()
            self.data = self.source.format.parse(raw_data)
        self.db_engine = DBFactory.get_engine(self.target)

        if self.operation == "reload":
            self.reload_table(self.data, self.target.table_name)
        elif self.operation == "truncate":
            self.truncate_table(self.target.table_name)
        elif self.operation == "insert":
            self.insert_into_table(self.data, self.target.table_name)
        elif self.operation == "upsert":
            self.upsert_into_table(self.data, self.target.table_name)
        elif self.operation == "delete":
            self.delete_from_table(self.data, self.target.table_name)
        else:
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