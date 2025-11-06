#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database connection management for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import cast, Optional

# external dependencies:
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError, DatabaseError
import userprovided

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection management for the exoskeleton framework using SQLAlchemy."""

    def __init__(self,
                 database_settings: dict) -> None:
        """
        Initialize database connection using SQLAlchemy.

        Args:
            database_settings: Dictionary containing database credentials
                Required keys: 'database', 'username'
                Optional keys: 'host', 'port', 'passphrase'
        """
        if database_settings is None:
            raise ValueError('You must supply database credentials.')

        # Are the parameters valid?
        userprovided.parameters.validate_dict_keys(
            dict_to_check=database_settings,
            allowed_keys={'host', 'port', 'database', 'username', 'passphrase'},
            necessary_keys={'database', 'username'},
            dict_name='database_settings')

        # Necessary settings (existence ensured, but must not be None)
        self.db_name: str = database_settings['database']
        if not self.db_name:
            raise ValueError('You must provide the name of the database.')
        self.db_username: str = database_settings['username']
        if not self.db_username:
            raise ValueError('You must provide a database user.')

        # Check settings and fallback to default if necessary:
        self.db_host: str = cast(str, database_settings.get('host', None))
        if not self.db_host:
            logger.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port: int = cast(int, database_settings.get('port', None))
        if not self.db_port:
            logger.info('No port number supplied: will try port 3306.')
            self.db_port = 3306
        elif not userprovided.parameters.is_port(self.db_port):
            raise ValueError('Database port outside valid range!')

        self.db_passphrase: str = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logger.warning(
                'No database passphrase provided. Trying to connect without.')

        # Establish the database connection using SQLAlchemy
        self.engine: Optional[Engine] = None
        self.Session: Optional[sessionmaker] = None
        self._session: Optional[Session] = None
        self.establish_db_connection()

    def __del__(self) -> None:
        """Cleanup: close session and dispose engine."""
        try:
            if self._session:
                self._session.close()
            if self.engine:
                self.engine.dispose()
        except Exception:  # pylint: disable=broad-except
            pass

    def establish_db_connection(self) -> None:
        """Establish a connection to MariaDB using SQLAlchemy."""
        try:
            logger.debug('Trying to connect to database.')

            # Build connection URL
            # Using pymysql as the driver for MariaDB
            connection_url = (
                f"mysql+pymysql://{self.db_username}:{self.db_passphrase}@"
                f"{self.db_host}:{self.db_port}/{self.db_name}"
                "?charset=utf8mb4"
            )

            # Create engine with connection pooling
            self.engine = create_engine(
                connection_url,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False,          # Set to True for SQL debugging
            )

            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Create session factory
            self.Session = sessionmaker(bind=self.engine, autoflush=True, autocommit=False)

            logger.info('Successfully established database connection.')

        except OperationalError:
            logger.exception(
                "Cannot connect to DBMS. Did you forget a parameter?",
                exc_info=True)
            raise
        except DatabaseError:
            logger.exception('Database related exception', exc_info=True)
            raise
        except Exception:
            logger.exception(
                'Exception while connecting to the DBMS.', exc_info=True)
            raise

    def get_session(self) -> Session:
        """
        Get a SQLAlchemy session for database operations.

        Returns:
            Session: A SQLAlchemy session object
        """
        if self._session is None or not self._session.is_active:
            if self.Session is None:
                logger.info("Lost database connection. Trying to reconnect...")
                self.establish_db_connection()
            if self.Session:
                self._session = self.Session()
        return self._session  # type: ignore

    def execute_raw(self, query: str, params: Optional[dict] = None):
        """
        Execute raw SQL (for stored procedures and complex queries).

        Args:
            query: SQL query string
            params: Optional parameters dictionary

        Returns:
            Result proxy object
        """
        session = self.get_session()
        try:
            if params:
                result = session.execute(text(query), params)
            else:
                result = session.execute(text(query))
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise

    def call_procedure(self, procedure_name: str, params: Optional[tuple] = None):
        """
        Call a stored procedure.

        Args:
            procedure_name: Name of the stored procedure
            params: Optional tuple of parameters

        Returns:
            Result from the stored procedure
        """
        param_placeholders = ", ".join([":param" + str(i) for i in range(len(params or []))])
        query = f"CALL {procedure_name}({param_placeholders})"

        param_dict = {}
        if params:
            param_dict = {f"param{i}": val for i, val in enumerate(params)}

        return self.execute_raw(query, param_dict)
