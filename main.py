from tasks.etl_task import DMLTask
from utils.logger import get_logger
from tasks.models.operation import DataSrc, OperationTarget, OperationType
from tasks.data_reader import LocalReader, AWSS3Reader
from tasks.data_formatter import CSVFormatter, ParquetFormatter

logger = get_logger(__name__)


if __name__ == "__main__":
    pass
