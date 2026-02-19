from types import TracebackType
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator
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
    password: SecretStr | None = Field(default=None, init=False)
    secondary_roles: list[str] | None = Field(default=None, init=False)
    authenticator: Literal["SNOWFLAKE", "externalbrowser"] = Field(
        DEFAULT_AUTHENTICATOR,
        init=False,
    )
    client_store_temporary_credential: bool = Field(True, init=False)

    @model_validator(mode="after")
    def validate_authentication_fields(self) -> "SnowflakeSettings":
        """Validate auth method specific required fields."""
        if self.authenticator == "SNOWFLAKE" and self.password is None:
            raise ValueError("snowflake.password is required when authenticator is SNOWFLAKE")

        if self.secondary_roles is not None:
            if len(self.secondary_roles) == 0:
                raise ValueError("snowflake.secondary_roles must not be empty when provided")

            normalized = [role.strip() for role in self.secondary_roles]
            if any(role == "" for role in normalized):
                raise ValueError("snowflake.secondary_roles must not contain empty values")

            keywords = {"ALL", "NONE"}
            if any(role.upper() in keywords for role in normalized):
                if len(normalized) != 1:
                    raise ValueError("snowflake.secondary_roles must contain exactly one value when using ALL or NONE")
                normalized = [normalized[0].upper()]

            self.secondary_roles = normalized

        return self


class ToolsSettings(BaseModel):
    """Settings for enabling/disabling individual tools."""

    analyze_table_statistics: bool = Field(True, init=False)
    describe_table: bool = Field(True, init=False)
    execute_query: bool = Field(True, init=False)
    list_schemas: bool = Field(True, init=False)
    list_tables: bool = Field(True, init=False)
    list_views: bool = Field(True, init=False)
    profile_semi_structured_columns: bool = Field(True, init=False)
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
        if self.profile_semi_structured_columns:
            enabled_tools.add("profile_semi_structured_columns")
        if self.sample_table_data:
            enabled_tools.add("sample_table_data")
        return enabled_tools


class ExecuteQuerySettings(BaseModel):
    """Settings for execute_query tool behavior."""

    timeout_seconds_default: int = Field(30, ge=1, le=3600, init=False)
    timeout_seconds_max: int = Field(300, ge=1, le=3600, init=False)

    @model_validator(mode="after")
    def validate_timeout_relationship(self) -> "ExecuteQuerySettings":
        """Validate default timeout does not exceed max timeout."""
        if self.timeout_seconds_default > self.timeout_seconds_max:
            raise ValueError(
                "execute_query.timeout_seconds_default "
                + "must be less than or equal to "
                + "execute_query.timeout_seconds_max"
            )
        return self


class AnalyzeTableStatisticsSettings(BaseModel):
    """Settings for analyze_table_statistics tool behavior."""

    query_timeout_seconds: int = Field(60, ge=1, le=3600, init=False)


class ProfileSemiStructuredColumnsSettings(BaseModel):
    """Settings for profile_semi_structured_columns tool behavior."""

    base_query_timeout_seconds: int = Field(90, ge=1, le=3600, init=False)
    path_query_timeout_seconds: int = Field(180, ge=1, le=3600, init=False)

    @model_validator(mode="after")
    def validate_timeout_relationship(self) -> "ProfileSemiStructuredColumnsSettings":
        """Validate path timeout is not shorter than base timeout."""
        if self.path_query_timeout_seconds < self.base_query_timeout_seconds:
            raise ValueError(
                "profile_semi_structured_columns.path_query_timeout_seconds "
                + "must be greater than or equal to "
                + "profile_semi_structured_columns.base_query_timeout_seconds"
            )
        return self


class Settings(BaseSettings):
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    tools: ToolsSettings = Field(default_factory=ToolsSettings)
    analyze_table_statistics: AnalyzeTableStatisticsSettings = Field(default_factory=AnalyzeTableStatisticsSettings)
    execute_query: ExecuteQuerySettings = Field(default_factory=ExecuteQuerySettings)
    profile_semi_structured_columns: ProfileSemiStructuredColumnsSettings = Field(
        default_factory=ProfileSemiStructuredColumnsSettings
    )

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
