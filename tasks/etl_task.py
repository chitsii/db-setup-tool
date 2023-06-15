from typing import Dict, List, Optional
from abc import abstractmethod, ABCMeta

from tasks.engines.factory import DBFactory
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from utils.logger import get_logger


class TaskInterface(metaclass=ABCMeta):
    @abstractmethod
    def run(self):
        raise NotImplementedError()


class DDLTask(TaskInterface):
    def __init__(
        self,
        target: OperationTarget
    ):
        self.target = target
        self.logger = get_logger(__name__)
        self.db_engine = DBFactory.get_engine(self.target)

        if self.target.table_name is not None:
            self.logger.warning(f"table_name: {self.target.table_name} is ignored when executing DDLTask")

    def run(self, sqls: List[str]):
        self.logger.info(f"DDL {self.target}")

        with self.db_engine as db:
            for sql in sqls:
                self.logger.debug(f"execute: {sql}")
                db.execute(sql)
            db.commit()

    def purge_binlog(self):
        self.logger.info("purge binlog")
        with self.db_engine as db:
            db.execute("PURGE BINARY LOGS BEFORE NOW()")
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

    def __reload_table(self, data: List[Dict], table_name):
        with self.db_engine as db:
            db.truncate(table_name)
            db.insert(table_name, data)
            db.commit()

    def __truncate_table(self, table_name):
        with self.db_engine as db:
            db.truncate(table_name)
            db.commit()

    def __insert_into_table(self, data: List[Dict], table_name):
        with self.db_engine as db:
            db.insert(table_name, data)
            db.commit()

    def __upsert_into_table(self, data: List[Dict], table_name):
        with self.db_engine as db:
            db.upsert(table_name, data)
            db.commit()

    def __delete_from_table(self, data: List[Dict], table_name):
        with self.db_engine as db:
            db.delete(table_name, data)
            db.commit()
