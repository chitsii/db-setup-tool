import tomllib
from typing import TypedDict


class Logging(TypedDict):
    level: str


class MySQLAccessInfo(TypedDict):
    host: str
    password: str
    port: int
    user: str


class ConfigStructure(TypedDict):
    mysql: MySQLAccessInfo
    logging: Logging


def get_config() -> ConfigStructure:
    with open("config.toml", mode="rb") as f:
        config = tomllib.load(f)
    return ConfigStructure(**config)


config = get_config()