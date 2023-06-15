from tasks.engines.mysql import MySQLEngine
from tasks.models.operation import OperationTarget

class DBFactory:
    @staticmethod
    def get_engine(db_engine: OperationTarget):
        if db_engine.engine_option == "mysql":
            return MySQLEngine(db_engine.db_name)
        else:
            raise NotImplementedError()
