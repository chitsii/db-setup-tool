import pytest
from unittest.mock import patch
from moto import mock_s3
import boto3

from tasks.etl_task import DMLTask, DDLTask
from tasks.data_formatter import CSVFormatter, ParquetFormatter
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from tasks.data_reader import LocalReader, AWSS3Reader


class TestTask:
    @pytest.fixture
    def mock_config(self):
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
    def test_mysql(self, mock_config):
        """MySqlに対して各種操作を行う"""
        # 準備: S3にデータを配置
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")  # type: ignore
        s3.Object("mybucket", "country.parquet").put(Body=open("tests/data/mysql/parquet/country.parquet", "rb"))  # type: ignore
        s3.Object("mybucket", "csv/city.csv").put(Body=open("tests/data/mysql/csv/city.csv", "rb"))  # type: ignore

        # 実行
        DDLTask(
            target=OperationTarget("mysql", None, None),
        ).run(
            [
                """drop database if exists dev;""",
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
                PRIMARY KEY (`Code`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                """,
                """
                    CREATE TABLE `countrylanguage` (
                    `CountryCode` char(3) NOT NULL DEFAULT '',
                    `Language` char(30) NOT NULL DEFAULT '',
                    `IsOfficial` enum('T','F') NOT NULL DEFAULT 'F',
                    `Percentage` decimal(4,1) NOT NULL DEFAULT '0.0',
                    PRIMARY KEY (`CountryCode`,`Language`),
                    KEY `CountryCode` (`CountryCode`),
                    CONSTRAINT `countryLanguage_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `country` (`Code`)
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
        )

        DMLTask(
            target=OperationTarget("mysql", "dev", "countrylanguage"),
            operaton=OperationType.TRUNCATE,
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.TRUNCATE,
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "country"),
            operaton=OperationType.TRUNCATE,
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "country"),
            operaton=OperationType.INSERT,
            source=DataSrc(
                AWSS3Reader("s3://mybucket/country.parquet"), ParquetFormatter()
            ),
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.INSERT,
            source=DataSrc(AWSS3Reader("s3://mybucket/csv/city.csv"), CSVFormatter()),
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.UPSERT,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()
            ),
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "countrylanguage"),
            operaton=OperationType.INSERT,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/countrylanguage.csv"), CSVFormatter()
            ),
        ).run()

        DMLTask(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.DELETE,
            source=DataSrc(
                LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()
            ),
        ).run()
