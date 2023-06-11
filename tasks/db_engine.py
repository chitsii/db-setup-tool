import time
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional

import pymysql
from pymysql.cursors import DictCursor

from utils.config import config, MySQLAccessInfo
from utils.logger import get_logger
from tasks.constant import MySQLConstant
from tasks.models.operation import OperationTarget
from tasks.models.model import TableSchema, ColumnSchema


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


class MySQLEngine(DBEngineInterface):
    def __init__(self, db_name: str, access_info: MySQLAccessInfo = config["mysql"]):
        self.db_name = db_name
        self.logger = get_logger(__name__)
        self.connection = pymysql.connect(
            db=self.db_name,
            charset="utf8mb4",
            cursorclass=DictCursor,
            **access_info
        )
        self.last_ping_time = time.time()

    def __enter__(self):
        self._ping()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    @staticmethod
    def rollback_on_fail(func):
        """クエリ失敗時、DBロールバックを行うデコレータ"""

        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.connection.rollback()
                raise e

        return wrapper

    def get_primary_key(self, table_name: str) -> List[str]:
        """テーブルのプライマリーキーを取得する"""
        cursor = self.connection.cursor()
        cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
        res = cursor.fetchall()
        primary_key_names = [d["Column_name"] for d in res]
        return primary_key_names

    def get_table_schema(self, table_name: str) -> TableSchema:
        """cursor.descriptionを使ってテーブルのスキーマを取得する
        例:
            (
                ('id', <class 'int'>, None, 11, None, None, True),
                ('name', <class 'str'>, None, 255, None, None, True),
                ('email', <class 'str'>, None, 255, None, None, False)
            )
        タプルの要素は以下の通り
            0 name: カラム名
            1 type_code: カラムのデータ型コード
            2 display_size: カラムの表示幅
            3 internal_size: カラムの内部的なサイズ
            4 precision: カラムの桁数（数値型の場合）
            5 scale: カラムの小数点以下の桁数（数値型の場合）
            6 null_ok: カラムがNULL許容かどうかを示すフラグ
        source: https://peps.python.org/pep-0249/#type-objects
        """
        cursor = self.connection.cursor()
        # LIMIT 0を指定することで、実際にデータを取得せずにカラム情報のみを取得する
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        column_schemas = [
            {
                "name": d[0],
                "data_type": d[1],
                "is_nullable": d[6],
                "is_primary_key": False,
            }
            for d in cursor.description
        ]
        # プライマリーキーの情報を追加
        for cs in column_schemas:
            if cs["name"] in self.get_primary_key(table_name):
                cs["is_primary_key"] = True

        return TableSchema.from_dict(
            {"table_name": table_name, "column_schemas": column_schemas}
        )

    def _repr_for_empty_value(self, column_schema: ColumnSchema):
        """挿入対象の値が空文字であるとき、
        挿入先カラムの型に応じてクエリ中での値の表現を返す
        """
        type_name = self.get_mysql_type_name(column_schema.data_type)
        if column_schema.is_nullable:
            return None
        else:
            if type_name in MySQLConstant.string_types:
                return ""
            elif type_name in MySQLConstant.numeric_types:
                return "0"
            elif type_name in MySQLConstant.datetime_types:
                self.logger.debug(f"{column_schema.name} is not NULLABLE, so set default datetime value 1970-01-01")
                return "1970-01-01 09:00:01"
                # データが空文字の場合、実際のデータがNoneなのか空文字か区別できないのが問題の本質なので、本来この操作は正しくない
                # しかしINSERTがCSVのカラムをValuesとして使用する限り、Default値が設定されたNOT NULLカラムにインサートすると厳密なSQLモードの制約に引っかかる
                # TODO: 本来はデフォルト値を使うようINSERT時に値をスキップする方が正しい
            elif type_name in ('year'):
                self.logger.debug(f"{column_schema.name} is not NULLABLE, so set default year value 1970")
                return "1970"
            else:
                # TODO: interval, geometry, json型をサポートする
                msg = f"data_type: {type_name} is not supported"
                raise Exception(msg)

    def _value_repr(self, column_schema: ColumnSchema, value: str):
        if value == "":
            return self._repr_for_empty_value(column_schema)
        return value

    @staticmethod
    def get_mysql_type_name(type_code):
        """MySQLのtype_codeから型名を取得する"""
        d = {
            getattr(pymysql.FIELD_TYPE, k): k
            for k in dir(pymysql.FIELD_TYPE)
            if not k.startswith("_")
        }
        return d[type_code].lower()

    @rollback_on_fail
    def execute(self, query):
        cursor = self.connection.cursor()
        affected_rows = cursor.execute(query)
        res = cursor.fetchall()
        return affected_rows, res

    @staticmethod
    def _escape(value):
        return pymysql.converters.escape_string(value)

    def validate_affected_count(self, affected_cnt: Optional[int], data: List[Dict]):
        match affected_cnt:
            case None:
                if len(data) != 0:
                    raise Exception(
                        "affected count is not matched. "
                        f"expected: {len(data)}, actual: {affected_cnt}"
                    )
            case _:
                if affected_cnt != len(data):
                    raise Exception(
                        "affected count is not matched. "
                        f"expected: {len(data)}, actual: {affected_cnt}"
                    )

    @rollback_on_fail
    def insert(self, table_name: str, data: List[Dict]):
        cursor = self.connection.cursor()

        # 対象テーブルのカラム名からクエリを作成
        table_schema = self.get_table_schema(table_name)
        tgt_columns = table_schema.get_column_names()
        sql = """
        INSERT INTO {table_name} ({column_names}) VALUES ({values})
        """.format(
            table_name=self._escape(table_name),
            column_names=",".join([self._escape(k) for k in tgt_columns]),
            values=",".join(["%s"] * len(tgt_columns)),
        )
        # 挿入する値を用意
        values = [
            [
                self._value_repr(table_schema.get_column_schema(col), row[col]) # type: ignore
                for col in tgt_columns
            ]
            for row in data
        ]
        self.logger.debug(f"{cursor.mogrify(sql, values[0])}")
        affected_rows = cursor.executemany(sql, values)

        self.validate_affected_count(affected_rows, data)

        return affected_rows

    @rollback_on_fail
    def upsert(self, table_name: str, data: List[Dict]):
        self.logger.info(f"start upsert {table_name}")
        cursor = self.connection.cursor()

        # 対象テーブルのカラム名からクエリを作成
        table_schema = self.get_table_schema(table_name)
        tgt_columns = table_schema.get_column_names()

        sql = """
        INSERT INTO {table_name} ({column_names}) VALUES ({values}) as r
        ON DUPLICATE KEY UPDATE {update_values}
        """.format(
            table_name=self._escape(table_name),
            column_names=",".join([self._escape(k) for k in tgt_columns]),
            values=",".join(["%s"] * len(tgt_columns)),
            update_values=",".join([f"{k}=r.{k}" for k in tgt_columns]),
        )

        # 挿入する値を用意
        values = [
            [
                self._value_repr(table_schema.get_column_schema(col), row[col]) # type: ignore
                for col in tgt_columns
            ]
            for row in data
        ]
        self.logger.debug(f"{cursor.mogrify(sql, values[0])}")
        affected_rows = cursor.executemany(sql, values)
        return affected_rows

    @rollback_on_fail
    def delete(self, table_name: str, data: List[Dict]):
        self.logger.info(f"start delete {table_name}")
        cursor = self.connection.cursor()
        table_schema = self.get_table_schema(table_name)
        primary_keys = table_schema.get_pk_column_names()
        sql = """
        DELETE FROM {table_name} WHERE {where}
        """.format(
            table_name=table_name,
            where=" AND ".join([f"{key}=%s" for key in primary_keys]),
        )
        values = [[row[col] for col in primary_keys] for row in data]
        self.logger.debug(f"{cursor.mogrify(sql, values[0])}")
        affected_rows = cursor.executemany(sql, values)

        self.validate_affected_count(affected_rows, data)
        return affected_rows

    @rollback_on_fail
    def truncate(self, table_name: str):
        cursor = self.connection.cursor()
        # 外部キーを一時的に無視してデータを削除
        cursor.execute("SET SESSION FOREIGN_KEY_CHECKS=0")
        affected_rows = cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute("SET SESSION FOREIGN_KEY_CHECKS=1")
        return affected_rows

    def commit(self):
        self._ping()
        self.connection.commit()

    def _ping(self):
        """前回チェックから3分以上経っていたらタイムアウトかを確認して、必要なら再接続"""
        current_time = time.time()
        if current_time - self.last_ping_time > 60 * 3:
            self.connection.ping(reconnect=True)
            self.last_ping_time = current_time


class DBFactory:
    @staticmethod
    def get_engine(db_engine: OperationTarget):
        if db_engine.engine_option == "mysql":
            return MySQLEngine(db_engine.db_name)
        else:
            raise NotImplementedError()
