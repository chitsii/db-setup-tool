from tasks.etl_task import Task
from utils.logger import get_logger
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from tasks.data_reader import LocalReader, AWSS3Reader
from tasks.data_formatter import CSVFormatter, ParquetFormatter

logger = get_logger(__name__)


if __name__ == "__main__":
    # sample etl job for mysql

    Task(
        target=OperationTarget("mysql", "dev", "countrylanguage"),
        operaton=OperationType.TRUNCATE,
    ).run()

    Task(
        target=OperationTarget("mysql", "dev", "city"),
        operaton=OperationType.TRUNCATE,
    ).run()

    Task(
        target=OperationTarget("mysql", "dev", "country"),
        operaton=OperationType.TRUNCATE,
    ).run()

    from moto import mock_s3
    import boto3
    with mock_s3():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket") # type: ignore
        s3.Object('mybucket', 'country.parquet').put(Body=open('tests/data/mysql/parquet/country.parquet', 'rb')) # type: ignore

        Task(
            target=OperationTarget("mysql", "dev", "country"),
            operaton=OperationType.INSERT,
            source=DataSrc(AWSS3Reader("s3://mybucket/country.parquet"), ParquetFormatter()),
        ).run()


        s3.Object('mybucket', 'csv/city.csv').put(Body=open('tests/data/mysql/csv/city.csv', 'rb')) # type: ignore

        Task(
            target=OperationTarget("mysql", "dev", "city"),
            operaton=OperationType.INSERT,
            source=DataSrc(AWSS3Reader("s3://mybucket/csv/city.csv"), CSVFormatter()),
        ).run()

    Task(
        target=OperationTarget("mysql", "dev", "city"),
        operaton=OperationType.UPSERT,
        source=DataSrc(LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()),
    ).run()

    Task(
        target=OperationTarget("mysql", "dev", "countrylanguage"),
        operaton=OperationType.INSERT,
        source=DataSrc(LocalReader("tests/data/mysql/csv/countrylanguage.csv"), CSVFormatter()),
    ).run()

    Task(
        target=OperationTarget("mysql", "dev", "city"),
        operaton=OperationType.DELETE,
        source=DataSrc(LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()),
    ).run()
