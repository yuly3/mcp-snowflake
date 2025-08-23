"""
Snowflake connection provider implementation.
"""

from typing import Any

from snowflake.connector import SnowflakeConnection


class SnowflakeConnectionProvider:
    """
    Snowflake connection provider and manager.

    This class encapsulates Snowflake connection parameters and provides
    a factory method to create new connections with consistent configuration.

    Parameters
    ----------
    account : str
        Snowflake account identifier
    user : str
        Username for authentication
    password : str
        Password for authentication
    warehouse : str | None, optional
        Default warehouse to use
    database : str | None, optional
        Default database to use
    schema : str | None, optional
        Default schema to use
    role : str | None, optional
        Default role to use
    **kwargs : Any
        Additional connection parameters to pass to SnowflakeConnection
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Snowflake connection settings."""
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.extra_params = kwargs

    def get_connection(self) -> SnowflakeConnection:
        """
        Create and return a new Snowflake connection.

        Each call to this method creates a fresh connection instance.
        The caller is responsible for properly closing the connection
        when it's no longer needed.

        Returns
        -------
        SnowflakeConnection
            A new Snowflake connection configured with the provider's settings

        Raises
        ------
        DatabaseError
            If connection cannot be established due to authentication
            or network issues
        """
        params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
        }
        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        if self.role:
            params["role"] = self.role
        params.update(self.extra_params)

        return SnowflakeConnection(
            connection_name=None,
            connections_file_path=None,
            **params,
        )
