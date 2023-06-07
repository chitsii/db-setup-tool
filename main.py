from etl_task import Task
from logger import get_logger
from model import DataSrc, OperationTarget
from data_reader import LocalReader, AWSS3Reader
from data_formatter import CSVFormatter, ParquetFormatter

logger = get_logger(__name__)


if __name__ == "__main__":
    # sample etl job for mysql

    logger.info("truncate lang")
    truncate_lang = Task(
        source=None,
        target=OperationTarget("mysql", "dev", "countrylanguage"),
        operaton="truncate",
    )
    truncate_lang.run()

    logger.info("truncate_city")
    truncate_city = Task(
        source=None,
        target=OperationTarget("mysql", "dev", "city"),
        operaton="truncate",
    )
    truncate_city.run()

    logger.info("truncate country")
    truncate_country = Task(
        source=None,
        target=OperationTarget("mysql", "dev", "country"),
        operaton="truncate",
    )
    truncate_country.run()

    from moto import mock_s3
    import boto3
    with mock_s3():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket") # type: ignore
        s3.Object('mybucket', 'country.parquet').put(Body=open('tests/data/mysql/parquet/country.parquet', 'rb')) # type: ignore

        logger.info("insert country")
        insert_country = Task(
            source=DataSrc(AWSS3Reader("s3://mybucket/country.parquet"), ParquetFormatter()),
            target=OperationTarget("mysql", "dev", "country"),
            operaton="insert",
        )
        insert_country.run()


        s3.Object('mybucket', 'csv/city.csv').put(Body=open('tests/data/mysql/csv/city.csv', 'rb')) # type: ignore

        logger.info("insert_city")
        insert_city = Task(
            source=DataSrc(AWSS3Reader("s3://mybucket/csv/city.csv"), CSVFormatter()),
            target=OperationTarget("mysql", "dev", "city"),
            operaton="insert",
        )
        insert_city.run()

    logger.info("upsert city")
    upsert_city = Task(
        source=DataSrc(LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()),
        target=OperationTarget("mysql", "dev", "city"),
        operaton="upsert",
    )
    upsert_city.run()

    insert_lang = Task(
        source=DataSrc(LocalReader("tests/data/mysql/csv/countrylanguage.csv"), CSVFormatter()),
        target=OperationTarget("mysql", "dev", "countrylanguage"),
        operaton="insert",
    )
    insert_lang.run()

    logger.info("delete city")
    delete_city = Task(
        source=DataSrc(LocalReader("tests/data/mysql/csv/city_upsert.csv"), CSVFormatter()),
        target=OperationTarget("mysql", "dev", "city"),
        operaton="delete",
    )
    delete_city.run()

    logger.info("main finish")
