import tomllib
from typing import TypedDict


class Logging(TypedDict):
    level: str


class AccessInfo(TypedDict):
    host: str
    password: str
    port: int
    user: str


class Credentials(TypedDict):
    mysql: AccessInfo


class ConfigStructure(TypedDict):
    credentials: Credentials
    logging: Logging


def get_config() -> ConfigStructure:
    with open("config.toml", mode="rb") as f:
        config = tomllib.load(f)
    return ConfigStructure(**config)


config = get_config()