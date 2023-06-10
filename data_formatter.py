from abc import abstractmethod, ABCMeta
import csv
from typing import Literal, Optional, Union, BinaryIO

import pandas as pd

from logger import get_logger


class FormatterInterface(metaclass=ABCMeta):
    @abstractmethod
    def parse(self, bytes_input: BinaryIO, *args, **kwargs):
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"


class CSVFormatter(FormatterInterface):
    """
    バイト列をCSVとして解釈し、データを取得

    has_header = True: ファイル2行目以降をパース
    has_header = False: 全行をパース。必ずcolumn_namesを指定する必要がある
    column_namesが指定された場合、指定カラム名を使用
    column_namesが指定されなかった場合、1行目をヘッダとして使用
    """

    def __init__(
        self,
    ):
        self.logger = get_logger(__name__)

    def decode(self, bytes_input: BinaryIO, encoding: str = "utf-8"):
        return bytes_input.read().decode(encoding)

    def infer_has_header(self, csv_string: str):
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(csv_string)
        self.logger.info(f"has_header: {has_header}")
        self.has_header = has_header

    def parse(
        self,
        bytes_input: BinaryIO,
        encoding: str = "utf-8",
        has_header: Optional[bool] = None,
        column_names: Union[list, tuple, None] = None,
    ):
        self.logger.info(f"take it as csv. encoding: {encoding}")

        if has_header is None:
            head = bytes_input.read(10_000).decode(encoding=encoding)
            self.infer_has_header(head)
            bytes_input.seek(0)

        if has_header is False:
            # ヘッダなしファイルの場合は指定されたカラム名を使用
            if column_names is None:
                raise ValueError("column_names must be specified when has_header=False")
            df = pd.read_csv(bytes_input, dtype="str", names=column_names, header=None, encoding=encoding)
        else:
            # ヘッダありファイルの場合は先頭行をカラム名として使用
            # 指定があった場合は指定カラムを代わりに使用
            df = pd.read_csv(bytes_input, dtype="str", encoding=encoding)
            if column_names:
                df.columns = column_names
        df = df.fillna("") # NaNを空文字に置換
        res = df.to_dict("records")
        return res


class ParquetFormatter(FormatterInterface):
    """
    バイト列をParquetの形式として解釈し、データを取得
    """
    def __init__(
        self,
    ):
        self.logger = get_logger(__name__)

    def parse(
        self,
        bytes_input: BinaryIO
    ):
        self.logger.info("take it as parquet.")

        df = pd.read_parquet(bytes_input)
        df = df.fillna("") # NaNを空文字に置換
        res = df.to_dict("records")
        return res


def get_formatter(option: Literal["csv", "parquet"], **kwargs) -> FormatterInterface:
    if option == "csv":
        return CSVFormatter()
    elif option == "parquet":
        return ParquetFormatter()
    else:
        raise Exception
