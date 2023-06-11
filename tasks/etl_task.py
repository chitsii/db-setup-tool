from typing import Dict, List, Optional


from tasks.db_engine import DBFactory
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from utils.logger import get_logger


class TaskInterface:
    def __init__(
        self,
        target: OperationTarget,
        operaton: OperationType,
        source: Optional[DataSrc] = None,
    ):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()


class DDLTask(TaskInterface):
    def __init__(
        self,
        target: OperationTarget
    ):
        self.target = target
        if self.target.table_name is not None:
            self.logger.warning("table_name is ignored when executing DDLTask")
        self.logger = get_logger(__name__)

    def run(self, sqls: List[str]):
        self.db_engine = DBFactory.get_engine(self.target)
        self.logger.info(f"DDL {self.target}")

        with self.db_engine as db:
            for sql in sqls:
                db.execute(sql)
            db.commit()


class DMLTask(TaskInterface):
    def __init__(
        self,
        target: OperationTarget,
        operaton: OperationType,
        source: Optional[DataSrc] = None,
    ):
        self.source = source
        self.target = target
        self.operation = operaton
        self.logger = get_logger(__name__)

        # 厳密にはTruncateはDDLだが、簡便さのためDMLTaskとして扱う
        assert (
            self.source or self.operation == OperationType.TRUNCATE
        ), "source is required when operation is not truncate"

    def run(self):
        self.db_engine = DBFactory.get_engine(self.target)
        if self.source:
            raw_data = self.source.location.read()
            self.data = self.source.format.parse(raw_data)

        # 実行
        self.logger.info(f"{self.operation.name} {self.target}")
        match self.operation:
            case OperationType.TRUNCATE:
                self.__truncate_table(self.target.table_name)
            case OperationType.INSERT:
                self.__insert_into_table(self.data, self.target.table_name)
            case OperationType.UPSERT:
                self.__upsert_into_table(self.data, self.target.table_name)
            case OperationType.DELETE:
                self.__delete_from_table(self.data, self.target.table_name)
            case OperationType.RELOAD:
                self.__reload_table(self.data, self.target.table_name)
            case _:
                raise NotImplementedError()

    def __reload_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.truncate(table_name)
            db.insert(table_name, data)
            db.commit()

    def __truncate_table(self, table_name):
        with self.db_engine as db:
            db.truncate(table_name)
            db.commit()

    def __insert_into_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.insert(table_name, data)
            db.commit()

    def __upsert_into_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.upsert(table_name, data)
            db.commit()

    def __delete_from_table(self, data: Optional[List[Dict]], table_name):
        if not data:
            raise Exception("data is empty")
        with self.db_engine as db:
            db.delete(table_name, data)
            db.commit()
