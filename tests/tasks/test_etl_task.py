import pytest
from unittest.mock import patch
from moto import mock_s3
import boto3

from tasks.etl_task import DMLTask, DDLTask
from tasks.data_formatter import CSVFormatter, ParquetFormatter
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from tasks.data_reader import LocalReader, AWSS3Reader

from tasks.engines.factory import DBFactory

class TestTask:
    ddl_queries = [
        """drop database if exists dev""",
        """create database dev""",
        """use dev""",
        """
        CREATE TABLE `country` (
        `Code` char(3) NOT NULL DEFAULT '',
        `Name` char(52) NOT NULL DEFAULT '',
        `Continent` enum('Asia','Europe','North America','Africa','Oceania','Antarctica','South America') NOT NULL DEFAULT 'Asia',
        `Region` char(26) NOT NULL DEFAULT '',
        `SurfaceArea` decimal(10,2) NOT NULL DEFAULT '0.00',
        `IndepYear` smallint DEFAULT NULL,
        `Population` int NOT NULL DEFAULT '0',
        `LifeExpectancy` decimal(3,1) DEFAULT NULL,
        `GNP` decimal(10,2) DEFAULT NULL,
        `GNPOld` decimal(10,2) DEFAULT NULL,
        `LocalName` char(45) NOT NULL DEFAULT '',
        `GovernmentForm` char(45) NOT NULL DEFAULT '',
        `HeadOfState` char(60) DEFAULT NULL,
        `Capital` int DEFAULT NULL,
        `Code2` char(2) NOT NULL DEFAULT '',
        -- 日付型INSERTのテスト用カラム
        `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `time` time NOT NULL,
        `date` date NOT NULL,
        `year` year NOT NULL,
        `datetime` datetime NOT NULL,
        PRIMARY KEY (`Code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """,
        """
        CREATE TABLE `city` (
        `ID` int NOT NULL AUTO_INCREMENT,
        `Name` char(35) NOT NULL DEFAULT '',
        `CountryCode` char(3) NOT NULL DEFAULT '',
        `District` char(20) NOT NULL DEFAULT '',
        `Population` int NOT NULL DEFAULT '0',
        PRIMARY KEY (`ID`),
        KEY `CountryCode` (`CountryCode`),
        CONSTRAINT `city_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `country` (`Code`)
        ) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """,
    ]

    @pytest.fixture
    def mock_config(self):
        # テスト用DBへの接続情報を設定
        with patch("utils.config.config") as mock_config:
            mock_config.return_value = {
                "mysql": {
                    "dev": {
                        "host": "localhost",
                        "port": 3306,
                        "user": "root",
                        "password": "password",
                        "database": "test",
                    }
                },
                "logging": {
                    "log_level": "DEBUG",
                },
            }
            yield mock_config

    @pytest.mark.integration
    @pytest.mark.normal
    @mock_s3
    def test_mysql_結合テスト(self, mock_config):
        """MySqlに対して各種操作を行う"""
        # 準備: S3にデータを配置
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket="mybucket")  # type: ignore
        s3.Object("mybucket", "country.parquet").put(Body=open("tests/data/mysql/parquet/country.parquet", "rb"))  # type: ignore
        s3.Object("mybucket", "csv/city.csv").put(Body=open("tests/data/mysql/csv/city.csv", "rb"))  # type: ignore

        # 実行
        DDLTask(
            target=OperationTarget("mysql", "dev", "ignored"), # DDLTask実行時、対象テーブル名は無視される
        ).run(self.ddl_queries)

        # S3からCSV/Parquetのデータ取得して投入
        DMLTask(
            target=OperationTarget("mysql", "dev", "country"),
            operaton=OperationType.INSERT,
            source=DataSrc(AWSS3Reader("s3://mybucket/country.parquet"), ParquetFormatter()),
        ).run()

        # DMLTask(
        #     target=OperationTarget("mysql", "dev", "country"),
        #     operaton=OperationType.INSERT,
        #     source=DataSrc(LocalReader("tests/data/mysql/csv/country.csv"), CSVFormatter()),
        # ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.RELOAD,
            source=DataSrc(AWSS3Reader("s3://mybucket/csv/city.csv"), CSVFormatter()),
        ).run()

        # ID 1-3のレコードを変更
        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.UPSERT,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/normal/01_city_upsert.csv"), CSVFormatter()
            ),
        ).run()

        # ID 4-6のレコードを削除
        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.DELETE,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/normal/02_city_delete.csv"),
                CSVFormatter(
                    encoding="utf-8",
                    has_header=True,
                    column_names=["ID","Name","CountryCode","District","Population",],
                ),
            ),
        ).run()

        # ID 7-9のレコードを変更 (ヘッダなし)
        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.UPSERT,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/normal/03_city_no_header.csv"),
                CSVFormatter(
                    encoding="utf-8",
                    has_header=False,
                    column_names=["ID","Name","CountryCode","District","Population",],
                ),
            ),
        ).run()

        # ID 10-12のレコードを変更（余計なカラムを含む）
        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.UPSERT,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/normal/04_city_has_extra_column.csv"),
                CSVFormatter(
                    encoding="utf-8",
                    has_header=True,
                ),
            ),
        ).run()


        # 検証
        target = OperationTarget("mysql", "dev", None)
        with DBFactory.get_engine(target) as db:
            cnt, res = db.execute("SELECT * FROM city WHERE ID <= 12;")
            assert cnt == 9
            assert res == [
                # ---------------------------
                {"ID": 1, "Name": "UPSERT_1", "CountryCode": "AFG", "District": "Kabol", "Population": 1780000},
                {"ID": 2, "Name": "UPSERT_2", "CountryCode": "AFG", "District": "Qandahar", "Population": 237500},
                {"ID": 3, "Name": "UPSERT_3", "CountryCode": "AFG", "District": "Herat", "Population": 186800},
                # ---------------------------
                # ID 4-6は削除済みで存在しない
                # ---------------------------
                {"ID": 7, "Name": "NO_HEADER_1", "CountryCode": "NLD", "District": "Zuid-Holland", "Population": 440900},
                {"ID": 8, "Name": "NO_HEADER_2", "CountryCode": "NLD", "District": "Utrecht", "Population": 234323},
                {"ID": 9, "Name": "NO_HEADER_3", "CountryCode": "NLD", "District": "Noord-Brabant", "Population": 201843},
                # ---------------------------
                {"ID": 10, "Name": "EXTRA_COL_1", "CountryCode": "NLD", "District": "Noord-Brabant", "Population": 193238},
                {"ID": 11, "Name": "EXTRA_COL_2", "CountryCode": "NLD", "District": "Groningen", "Population": 172701},
                {"ID": 12, "Name": "EXTRA_COL_3", "CountryCode": "NLD", "District": "Noord-Brabant", "Population": 160398},
            ]

        # データ削除を実行
        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.TRUNCATE,
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "country"),
            operaton=OperationType.TRUNCATE,
        ).run()

        # 検証
        with DBFactory.get_engine(target) as db:
            cnt, res = db.execute("SELECT * FROM city WHERE ID <= 12;")
            assert cnt == 0
            assert len(res) == 0

            cnt, res = db.execute("SELECT * FROM country;")
            assert cnt == 0
            assert len(res) == 0

        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).purge_binlog()


    @pytest.mark.integration
    @pytest.mark.abnormal
    def test_ヘッダなしのCSVをカラム名情報無しで扱う場合(self, mock_config):
        # DDL実行
        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).run(self.ddl_queries)

        with pytest.raises(ValueError):
            # ヘッダなしのCSVをカラム無指定で扱う
            DMLTask(
                target=OperationTarget("mysql", "dev", "city"),
                operaton=OperationType.UPSERT,
                source=DataSrc(
                    LocalReader("tests/data/mysql/csv/normal/03_city_no_header.csv"),
                    CSVFormatter(
                        encoding="utf-8",
                        has_header=False,
                        column_names=None, # カラム指定なし
                    ),
                ),
            ).run()


    @pytest.mark.integration
    @pytest.mark.abnormal
    def test_テーブルが必要とするカラムを欠いたデータを扱う場合(self, mock_config):
        # DDL実行
        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).run(self.ddl_queries)

        with pytest.raises(KeyError):
            # ヘッダなしのCSVをカラム無指定で扱う
            DMLTask(
                target=OperationTarget("mysql", "dev", "city"),
                operaton=OperationType.UPSERT,
                source=DataSrc(
                    LocalReader("tests/data/mysql/csv/abnormal/01.city_lacks_needed_column.csv"),
                    CSVFormatter(encoding="utf-8", has_header=True),
                ),
            ).run()


    @pytest.mark.integration
    @pytest.mark.abnormal
    @mock_s3
    def test_S3のURIが誤りの場合(self, mock_config):
        # DDL実行
        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).run(self.ddl_queries)

        with pytest.raises(Exception):
            DMLTask(
                target=OperationTarget("mysql", "dev", "city"),
                operaton=OperationType.UPSERT,
                source=DataSrc(
                    AWSS3Reader("s3://dummy-bucket/invalid-key.csv"),
                    CSVFormatter(encoding="utf-8", has_header=True),
                ),
            ).run()

    @pytest.mark.integration
    @pytest.mark.abnormal
    @mock_s3
    def test_S3のURIの形式が不正の場合(self, mock_config):
        # DDL実行
        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).run(self.ddl_queries)

        with pytest.raises(ValueError):
            DMLTask(
                target=OperationTarget("mysql", "dev", "city"),
                operaton=OperationType.UPSERT,
                source=DataSrc(
                    AWSS3Reader("invalid://dummy-bucket/invalid-key.csv"),
                    CSVFormatter(encoding="utf-8", has_header=True),
                ),
            ).run()