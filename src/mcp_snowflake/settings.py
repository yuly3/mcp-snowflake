from types import TracebackType
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from snowflake.connector.network import DEFAULT_AUTHENTICATOR


class SnowflakeSettings(BaseModel):
    account: str = Field(init=False)
    role: str = Field(init=False)
    warehouse: str = Field(init=False)
    user: str = Field(init=False)
    password: SecretStr = Field(init=False)
    authenticator: Literal["SNOWFLAKE", "externalbrowser"] = Field(
        DEFAULT_AUTHENTICATOR,
        init=False,
    )


class Settings(BaseSettings):
    snowflake: SnowflakeSettings = Field(init=False)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            TomlConfigSettingsSource(settings_cls),
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    class _Builder:
        def __init__(self, model_config: SettingsConfigDict) -> None:
            self.model_config = model_config
            self._model_config_default = Settings.model_config

        def __enter__(self) -> "Settings":
            Settings.model_config = self.model_config
            return Settings()

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            Settings.model_config = self._model_config_default

    @classmethod
    def build(cls, model_config: SettingsConfigDict) -> "Settings":
        with cls._Builder(model_config) as settings:
            return settings
