from pydantic import Field, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict


class Cli(BaseSettings):
    model_config = SettingsConfigDict(cli_parse_args=True)

    config: FilePath = Field(init=False)
