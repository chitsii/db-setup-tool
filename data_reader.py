from abc import abstractmethod, ABCMeta
from pathlib import Path
import boto3
from urllib.parse import urlsplit
from typing import BinaryIO
from io import BytesIO

from logger import get_logger


class ReaderInterface(metaclass=ABCMeta):
    def __init__(self):
        self.logger = get_logger(__name__)

    @abstractmethod
    def read(self):
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class LocalReader(ReaderInterface):
    def __init__(self, path: str):
        self.logger = get_logger(__name__)
        self.path = path

    def read(self) -> BinaryIO:
        self.logger.info(f"filepath: {self.path}")
        path = Path(self.path)
        assert path.exists(), f"指定ファイルが存在しません: {path}"
        with path.open("rb") as fb:
            res = BytesIO(fb.read())
        return res


class AWSS3Reader(ReaderInterface):
    def __init__(self, s3_uri: str):
        self.logger = get_logger(__name__)
        self.uri = s3_uri

    def read(self) -> BytesIO:
        self.logger.info(f"s3_uri: {self.uri}")
        bucket_name, prefix = self._parse_s3_uri(self.uri)
        self.s3 = boto3.resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)  # type: ignore
        self.prefix = prefix

        obj = self.s3.meta.client.get_object(Bucket=bucket_name, Key=prefix)  # type: ignore
        content = BytesIO(obj["Body"].read())
        return content

    def _parse_s3_uri(self, s3_uri: str):
        """S3 URI(s3://bucket/key)からバケット名とプレフィクスを取得"""

        parsed_url = urlsplit(s3_uri)
        if parsed_url.scheme != "s3":
            raise ValueError("Invalid S3 URI scheme")

        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip("/")
        return bucket_name, key
