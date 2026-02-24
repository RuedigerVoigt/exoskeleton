"""
Check the database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

import logging

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
import importlib.metadata

from exoskeleton import database_connection
from exoskeleton import err
from exoskeleton import models

logger = logging.getLogger(__name__)


class DatabaseSchemaCheck:
    "Check the database schema for exoskeleton."
    # pylint: disable=too-few-public-methods

    # Tables are now dynamically retrieved from SQLAlchemy models
    # This ensures they stay in sync with models.py
    TABLES = [table.name for table in models.Base.metadata.sorted_tables]

    # Stored procedures and functions expected in the database.
    # Empty by default (framework no longer uses stored procedures),
    # but kept as mutable lists so tests can append entries to verify
    # that the check methods raise InvalidDatabaseSchemaError when
    # expected entries are missing from the DB.
    PROCEDURES: list = []
    FUNCTIONS: list = []

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        "Sets defaults"
        self.db_connection = db_connection
        self.session: Session = db_connection.get_session()
        self.db_name: str = db_connection.db_name

        # Create per-instance copies of the class-level lists so that
        # mutations (e.g. appending 'foo' in tests) don't leak to other instances.
        self.TABLES = list(type(self).TABLES)
        self.PROCEDURES = list(type(self).PROCEDURES)
        self.FUNCTIONS = list(type(self).FUNCTIONS)

        self.check_db_schema()

    def __initialize_schema_version(self) -> None:
        """
        Initialize the schema version in exoInfo table.

        This is called after creating tables to ensure the schema version
        is set properly. Uses ORM merge for idempotent upsert.
        """
        try:
            # Use merge for upsert behavior (insert or update)
            schema_info = models.ExoInfo(exoKey='schema', exoValue='2.0.0')
            self.session.merge(schema_info)
            self.session.commit()
            logger.info('Schema version initialized to 2.0.0')
        except Exception as e:
            logger.warning('Could not initialize schema version: %s', str(e))
            self.session.rollback()

    def __check_stored_procedures(self) -> None:
        """Check if all expected stored procedures exist in the database.

        Raises InvalidDatabaseSchemaError if any procedure in PROCEDURES is absent."""
        if not self.PROCEDURES:
            return

        result = self.session.execute(
            text("SELECT ROUTINE_NAME FROM information_schema.ROUTINES "
                 "WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = :db"),
            {'db': self.db_name}
        )
        existing = {row[0] for row in result}

        missing = [p for p in self.PROCEDURES if p not in existing]
        if missing:
            msg = f'Missing stored procedures: {", ".join(missing)}'
            logger.error(msg)
            raise err.InvalidDatabaseSchemaError(msg)

    def __check_functions(self) -> None:
        """Check if all expected database functions exist in the database.

        Raises InvalidDatabaseSchemaError if any function in FUNCTIONS is absent."""
        if not self.FUNCTIONS:
            return

        result = self.session.execute(
            text("SELECT ROUTINE_NAME FROM information_schema.ROUTINES "
                 "WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = :db"),
            {'db': self.db_name}
        )
        existing = {row[0] for row in result}

        missing = [f for f in self.FUNCTIONS if f not in existing]
        if missing:
            msg = f'Missing database functions: {", ".join(missing)}'
            logger.error(msg)
            raise err.InvalidDatabaseSchemaError(msg)

    def __check_table_existence(self) -> bool:
        """
        Check if all expected tables exist, and create missing ones using ORM.

        Tables are dynamically retrieved from SQLAlchemy models (models.py),
        ensuring this check stays in sync with the ORM definitions.

        If tables are missing, they are automatically created using SQLAlchemy's
        create_all() method, which is idempotent (only creates missing tables).
        After the creation attempt, missing tables that could not be created
        (e.g. not defined in ORM) raise InvalidDatabaseSchemaError.

        Note: User might have custom tables, so we only verify expected tables exist,
        not that ONLY expected tables exist.
        """
        assert self.db_connection.engine is not None, "Database engine not initialized"
        inspector = inspect(self.db_connection.engine)
        tables_found_lower = [t.lower() for t in inspector.get_table_names()]

        missing_tables = [t for t in self.TABLES if t.lower() not in tables_found_lower]

        if not tables_found_lower:
            # No tables at all - create all tables from ORM models
            logger.warning('No tables found in database. Creating all tables from ORM models...')
            models.Base.metadata.create_all(self.db_connection.engine)
            logger.info('Successfully created all tables from ORM models.')
            self.__initialize_schema_version()
        elif missing_tables:
            logger.warning(
                'Missing %d tables: %s. Creating them from ORM models...',
                len(missing_tables),
                ', '.join(missing_tables)
            )
            models.Base.metadata.create_all(self.db_connection.engine)
            logger.info('Successfully created missing tables.')
            if 'exoinfo' in [t.lower() for t in missing_tables]:
                self.__initialize_schema_version()

        if missing_tables:
            # Re-verify: some tables may not be in ORM and thus can't be auto-created
            inspector = inspect(self.db_connection.engine)
            tables_after_lower = [t.lower() for t in inspector.get_table_names()]
            still_missing = [t for t in self.TABLES if t.lower() not in tables_after_lower]
            if still_missing:
                msg = f'Missing tables even after creation attempt: {", ".join(still_missing)}'
                logger.error(msg)
                raise err.InvalidDatabaseSchemaError(msg)

        logger.debug('Database schema: found all expected tables.')
        return True

    def __check_reference_data(self) -> None:
        """
        Check if required reference data exists in the database and add missing entries.

        This method intelligently populates the actions, errorType, and storageTypes
        tables with required preset data. It only adds missing entries without
        overwriting existing data or user customizations.

        Required data:
        - actions: IDs 1-4 (download file, save page code, page to PDF, save page text)
        - errorType: HTTP error codes and special error types
        - storageTypes: IDs 1-3 (disk, database, PDF)
        """
        logger.debug('Checking reference data...')

        # Define required actions
        required_actions = [
            {'id': 1, 'description': 'Download file'},
            {'id': 2, 'description': 'Save page code (HTML source)'},
            {'id': 3, 'description': 'Save page as PDF'},
            {'id': 4, 'description': 'Get text from page'}
        ]

        # Check and add missing actions
        for action_data in required_actions:
            existing = self.session.query(models.Action).filter(
                models.Action.id == action_data['id']
            ).first()

            if not existing:
                new_action = models.Action(
                    id=action_data['id'],
                    description=action_data['description']
                )
                self.session.add(new_action)
                logger.info('Added missing action: %s (id=%d)',
                           action_data['description'], action_data['id'])

        # Define required error types
        # permanent=True means permanent error, permanent=False means temporary/retryable
        required_errors = [
            # Special error codes
            {'id': 3, 'short': 'gave_up', 'description': 'Gave up after too many retries', 'permanent': True},
            # Permanent errors (HTTP status codes)
            {'id': 400, 'short': 'bad_request', 'description': 'Bad Request', 'permanent': True},
            {'id': 401, 'short': 'unauthorized', 'description': 'Unauthorized', 'permanent': True},
            {'id': 402, 'short': 'payment_required', 'description': 'Payment Required', 'permanent': True},
            {'id': 403, 'short': 'forbidden', 'description': 'Forbidden', 'permanent': True},
            {'id': 404, 'short': 'not_found', 'description': 'Not Found', 'permanent': True},
            {'id': 407, 'short': 'proxy_auth', 'description': 'Proxy Authentication Required', 'permanent': True},
            {'id': 410, 'short': 'gone', 'description': 'Gone', 'permanent': True},
            {'id': 451, 'short': 'unavailable_legal', 'description': 'Unavailable For Legal Reasons', 'permanent': True},
            {'id': 501, 'short': 'not_implemented', 'description': 'Not Implemented', 'permanent': True},
            # Temporary errors
            {'id': 408, 'short': 'timeout', 'description': 'Request Timeout', 'permanent': False},
            {'id': 429, 'short': 'rate_limit', 'description': 'Too Many Requests', 'permanent': False},
            {'id': 500, 'short': 'server_error', 'description': 'Internal Server Error', 'permanent': False},
            {'id': 502, 'short': 'bad_gateway', 'description': 'Bad Gateway', 'permanent': False},
            {'id': 503, 'short': 'service_unavailable', 'description': 'Service Unavailable', 'permanent': False},
            {'id': 504, 'short': 'gateway_timeout', 'description': 'Gateway Timeout', 'permanent': False},
            {'id': 509, 'short': 'bandwidth_exceeded', 'description': 'Bandwidth Limit Exceeded', 'permanent': False},
            {'id': 529, 'short': 'site_overloaded', 'description': 'Site is overloaded', 'permanent': False},
            {'id': 598, 'short': 'network_read_timeout', 'description': 'Network read timeout error', 'permanent': False}
        ]

        # Check and add missing error types
        for error_data in required_errors:
            existing_error = self.session.query(models.ErrorType).filter(
                models.ErrorType.id == error_data['id']
            ).first()

            if not existing_error:
                new_error = models.ErrorType(
                    id=error_data['id'],
                    short=error_data['short'],
                    description=error_data['description'],
                    permanent=error_data['permanent']
                )
                self.session.add(new_error)
                logger.info('Added missing error type: %s (id=%d)',
                           error_data['description'], error_data['id'])

        # Define required storage types
        required_storage_types = [
            {'id': 1, 'shortName': 'disk', 'fullName': 'Stored as file on disk'},
            {'id': 2, 'shortName': 'database', 'fullName': 'Stored in database'},
            {'id': 3, 'shortName': 'pdf', 'fullName': 'Stored as PDF file'}
        ]

        # Check and add missing storage types
        for storage_data in required_storage_types:
            existing_storage = self.session.query(models.StorageType).filter(
                models.StorageType.id == storage_data['id']
            ).first()

            if not existing_storage:
                new_storage = models.StorageType(
                    id=storage_data['id'],
                    shortName=storage_data['shortName'],
                    fullName=storage_data['fullName']
                )
                self.session.add(new_storage)
                logger.info('Added missing storage type: %s (id=%d)',
                           storage_data['shortName'], storage_data['id'])

        # Commit all changes
        try:
            self.session.commit()
            logger.debug('Reference data check complete.')
        except Exception as e:
            logger.error('Error adding reference data: %s', str(e))
            self.session.rollback()
            raise

    def __check_schema_version(self) -> None:
        "Check if the database schema is a compatible version."
        # Other methods check for the existence of stored procedures, functions
        # and tables. However, those might have changed fields. Therefore this
        # version check.

        # Use ORM instead of database function for database portability
        schema_info = self.session.query(models.ExoInfo).filter(
            models.ExoInfo.exoKey == 'schema'
        ).first()

        if not schema_info:
            msg = 'Schema version info not found in exoInfo table.'
            logger.exception(msg)
            raise RuntimeError(msg)

        db_schema = schema_info.exoValue

        # Not taking the comparison value from package metadata as multiple versions
        # of exoskeleton might share the same database schema.
        if db_schema == '2.0.0':
            logger.info('Database schema matches version of exoskeleton.')
        else:
            raise err.InvalidDatabaseSchemaError(
                "Mismatch between version of exoskeleton " +
                f"({importlib.metadata.version('exoskeleton')}) and version of the database " +
                f"schema ({db_schema}).")

    def check_db_schema(self) -> None:
        """Check whether all expected tables are available in the database,
           ensure reference data is populated, and validate the schema version."""
        self.__check_table_existence()
        self.__check_stored_procedures()
        self.__check_functions()
        self.__check_reference_data()
        self.__check_schema_version()
