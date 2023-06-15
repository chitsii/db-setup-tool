from abc import ABCMeta, abstractmethod


class DBEngineInterface(metaclass=ABCMeta):
    @abstractmethod
    def __enter__(self):
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        raise NotImplementedError()

    @abstractmethod
    def execute(self, query):
        raise NotImplementedError()

    @abstractmethod
    def truncate(self, table_name):
        raise NotImplementedError()

    @abstractmethod
    def insert(self, table_name, data):
        raise NotImplementedError()

    @abstractmethod
    def commit(self):
        raise NotImplementedError()
