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


class ToolsSettings(BaseModel):
    """Settings for enabling/disabling individual tools."""

    analyze_table_statistics: bool = Field(True, init=False)
    describe_table: bool = Field(True, init=False)
    execute_query: bool = Field(True, init=False)
    list_schemas: bool = Field(True, init=False)
    list_tables: bool = Field(True, init=False)
    list_views: bool = Field(True, init=False)
    sample_table_data: bool = Field(True, init=False)

    def enabled_tool_names(self) -> set[str]:
        """Return the set of enabled tool names."""
        enabled_tools: set[str] = set()
        if self.analyze_table_statistics:
            enabled_tools.add("analyze_table_statistics")
        if self.describe_table:
            enabled_tools.add("describe_table")
        if self.execute_query:
            enabled_tools.add("execute_query")
        if self.list_schemas:
            enabled_tools.add("list_schemas")
        if self.list_tables:
            enabled_tools.add("list_tables")
        if self.list_views:
            enabled_tools.add("list_views")
        if self.sample_table_data:
            enabled_tools.add("sample_table_data")
        return enabled_tools


class Settings(BaseSettings):
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    tools: ToolsSettings = Field(default_factory=ToolsSettings)

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
