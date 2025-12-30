"""
Check the database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

import logging
import pathlib
import re

from sqlalchemy import text, inspect
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

    PROCEDURES = ['delete_all_versions_SP',
                  'insert_content_SP',
                  'insert_file_SP',
                  'label_define_or_update_SP',
                  'labels_filemaster_by_url_SP',
                  'labels_version_by_id_SP',
                  'next_queue_object_SP',
                  'remove_labels_from_uuid_SP']

    # Database functions - now empty as all functions migrated to ORM/Inspector
    FUNCTIONS: list[str] = []

    @staticmethod
    def _parse_sql_schema_file() -> tuple[set[str], set[str]]:
        """
        Parse the SQL schema file to extract procedure and function names.

        This provides validation that hardcoded lists match the actual SQL schema.
        Returns a tuple of (procedures, functions) as sets.

        Returns:
            Tuple of (set of procedure names, set of function names)
        """
        # Find the SQL schema file
        current_dir = pathlib.Path(__file__).parent
        sql_file = current_dir.parent / 'Database-Scripts' / 'Create-Stored-Procedures-MariaDB.sql'

        if not sql_file.exists():
            logger.warning(
                'SQL schema file not found at %s. Skipping schema validation.',
                sql_file
            )
            return (set(), set())

        procedures = set()
        functions = set()

        try:
            content = sql_file.read_text(encoding='utf-8')

            # Extract procedure names: CREATE PROCEDURE name (
            proc_pattern = re.compile(r'CREATE\s+PROCEDURE\s+(\w+)\s*\(', re.IGNORECASE)
            procedures = set(proc_pattern.findall(content))

            # Extract function names: CREATE FUNCTION name (
            func_pattern = re.compile(r'CREATE\s+FUNCTION\s+(\w+)\s*\(', re.IGNORECASE)
            functions = set(func_pattern.findall(content))

            logger.debug(
                'Parsed SQL schema: %d procedures, %d functions',
                len(procedures), len(functions)
            )

        except Exception as e:
            logger.warning(
                'Failed to parse SQL schema file: %s. Skipping validation.',
                str(e)
            )

        return (procedures, functions)

    @classmethod
    def validate_hardcoded_lists(cls) -> None:
        """
        Validate that hardcoded PROCEDURES and FUNCTIONS lists match the SQL schema file.

        This is a development/maintenance helper to ensure the hardcoded lists
        stay in sync with the actual SQL schema definitions.
        """
        sql_procedures, sql_functions = cls._parse_sql_schema_file()

        if not sql_procedures and not sql_functions:
            logger.debug('No SQL schema file found - skipping validation')
            return

        code_procedures = set(cls.PROCEDURES)
        code_functions = set(cls.FUNCTIONS)

        # Check for discrepancies
        missing_procedures = sql_procedures - code_procedures
        extra_procedures = code_procedures - sql_procedures
        missing_functions = sql_functions - code_functions
        extra_functions = code_functions - sql_functions

        if missing_procedures:
            logger.warning(
                'Procedures in SQL schema but missing from hardcoded list: %s',
                ', '.join(sorted(missing_procedures))
            )

        if extra_procedures:
            logger.warning(
                'Procedures in hardcoded list but not in SQL schema: %s',
                ', '.join(sorted(extra_procedures))
            )

        if missing_functions:
            logger.warning(
                'Functions in SQL schema but missing from hardcoded list: %s',
                ', '.join(sorted(missing_functions))
            )

        if extra_functions:
            logger.warning(
                'Functions in hardcoded list but not in SQL schema: %s',
                ', '.join(sorted(extra_functions))
            )

        if not any([missing_procedures, extra_procedures,
                    missing_functions, extra_functions]):
            logger.debug('Hardcoded lists match SQL schema perfectly')

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        "Sets defaults"
        self.db_connection = db_connection
        self.session: Session = db_connection.get_session()
        self.db_name: str = db_connection.db_name

        # Validate hardcoded lists against SQL schema (development helper)
        self.validate_hardcoded_lists()

        self.check_db_schema()

    def __initialize_schema_version(self) -> None:
        """
        Initialize the schema version in exoInfo table.

        This is called after creating tables to ensure the schema version
        is set properly. Uses INSERT ... ON DUPLICATE KEY UPDATE to be idempotent.
        """
        try:
            self.session.execute(text(
                "INSERT INTO exoInfo (exoKey, exoValue) "
                "VALUES ('schema', '2.0.0') "
                "ON DUPLICATE KEY UPDATE exoValue = '2.0.0'"
            ))
            self.session.commit()
            logger.info('Schema version initialized to 2.0.0')
        except Exception as e:
            logger.warning('Could not initialize schema version: %s', str(e))
            self.session.rollback()

    def __check_table_existence(self) -> bool:
        """
        Check if all expected tables exist, and create missing ones using ORM.

        Tables are dynamically retrieved from SQLAlchemy models (models.py),
        ensuring this check stays in sync with the ORM definitions.

        If tables are missing, they are automatically created using SQLAlchemy's
        create_all() method, which is idempotent (only creates missing tables).

        Note: User might have custom tables, so we only verify expected tables exist,
        not that ONLY expected tables exist.
        """
        result = self.session.execute(text('SHOW TABLES'))
        tables = result.fetchall()

        if not tables:
            # No tables at all - create all tables from ORM models
            logger.warning('No tables found in database. Creating all tables from ORM models...')
            assert self.db_connection.engine is not None, "Database engine not initialized"
            models.Base.metadata.create_all(self.db_connection.engine)
            logger.info('Successfully created all tables from ORM models.')
            # Initialize schema version after creating tables
            self.__initialize_schema_version()
            return True

        tables_found = [item[0] for item in tables]
        # Make lowercase version for case-insensitive comparison
        # (MariaDB/MySQL may return tables in lowercase)
        tables_found_lower = [t.lower() for t in tables_found]
        missing_tables = []

        for table in self.TABLES:
            if table.lower() not in tables_found_lower:
                missing_tables.append(table)
                logger.warning('Table %s not found.', table)

        if missing_tables:
            # Some tables are missing - create them
            logger.warning(
                'Missing %d tables: %s. Creating them from ORM models...',
                len(missing_tables),
                ', '.join(missing_tables)
            )
            assert self.db_connection.engine is not None, "Database engine not initialized"
            models.Base.metadata.create_all(self.db_connection.engine)
            logger.info('Successfully created missing tables.')
            # Initialize schema version if it's missing (exoInfo might be one of the missing tables)
            if 'exoInfo' in [t.lower() for t in missing_tables]:
                self.__initialize_schema_version()

        logger.debug('Database schema: found all expected tables.')
        return True

    def __check_stored_procedures(self) -> bool:
        """Check if all expected stored procedures exist and if the user
           is allowed to execute them. Uses SQLAlchemy Inspector for
           database portability. """
        # Get inspector to introspect database
        inspector = inspect(self.db_connection.engine)

        # Get list of stored procedures using information_schema
        # This standard schema works across MySQL/MariaDB/PostgreSQL
        try:
            assert self.db_connection.engine is not None, "Database engine not initialized"
            dialect = self.db_connection.engine.dialect.name

            if dialect in ('mysql', 'mariadb'):
                # MySQL/MariaDB: filter by database name
                result = self.session.execute(
                    text("SELECT routine_name FROM information_schema.routines "
                         "WHERE routine_schema = :db_name AND routine_type = 'PROCEDURE'"),
                    {"db_name": self.db_name}
                )
            elif dialect == 'postgresql':
                # PostgreSQL: filter by schema (typically 'public')
                result = self.session.execute(
                    text("SELECT routine_name FROM information_schema.routines "
                         "WHERE routine_schema = 'public' AND routine_type = 'PROCEDURE'")
                )
            else:
                logger.warning(
                    'Stored procedure validation not supported for %s dialect. Skipping.',
                    dialect
                )
                return True

            procedures_found = [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.warning('Could not check stored procedures: %s', str(e))
            return True

        if not procedures_found:
            msg = 'No procedures found in database: Run generator script!'
            logger.exception(msg)
            raise RuntimeError(msg)

        count = 0
        for procedure in self.PROCEDURES:
            if procedure not in procedures_found:
                # Do not raise the exception just now.
                # Log all missing procedures first
                logger.error(
                    'Stored Procedure %s is missing or user lacks permissions.',
                    procedure)
            else:
                count += 1
        if count != len(self.PROCEDURES):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Stored Procedures!')
        logger.debug('Database schema: found all expected stored procedures.')
        return True

    def __check_functions(self) -> bool:
        """Check if all expected database functions exist and if the user
           is allowed to execute them. Uses SQLAlchemy Inspector for
           database portability. """
        # Skip function check if FUNCTIONS list is empty
        if not self.FUNCTIONS:
            logger.debug('No database functions to validate.')
            return True

        # Get inspector to introspect database
        inspector = inspect(self.db_connection.engine)

        # Get list of functions using information_schema
        # This standard schema works across MySQL/MariaDB/PostgreSQL
        try:
            assert self.db_connection.engine is not None, "Database engine not initialized"
            dialect = self.db_connection.engine.dialect.name

            if dialect in ('mysql', 'mariadb'):
                # MySQL/MariaDB: filter by database name
                result = self.session.execute(
                    text("SELECT routine_name FROM information_schema.routines "
                         "WHERE routine_schema = :db_name AND routine_type = 'FUNCTION'"),
                    {"db_name": self.db_name}
                )
            elif dialect == 'postgresql':
                # PostgreSQL: filter by schema (typically 'public')
                result = self.session.execute(
                    text("SELECT routine_name FROM information_schema.routines "
                         "WHERE routine_schema = 'public' AND routine_type = 'FUNCTION'")
                )
            else:
                logger.warning(
                    'Function validation not supported for %s dialect. Skipping.',
                    dialect
                )
                return True

            functions_found = [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.warning('Could not check database functions: %s', str(e))
            return True

        if not functions_found:
            msg = 'No functions found in database: Run generator script!'
            logger.exception(msg)
            raise RuntimeError(msg)

        count = 0
        for function in self.FUNCTIONS:
            if function not in functions_found:
                logger.error(
                    'Function %s is missing or user lacks permissions.',
                    function)
            else:
                count += 1
        if count != len(self.FUNCTIONS):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Functions!')
        logger.debug('Database schema: found all expected functions.')
        return True

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
        """Check whether all expected tables, stored procedures and functions
           are available in the database. Then look for a version string."""
        self.__check_table_existence()
        self.__check_stored_procedures()
        self.__check_functions()
        self.__check_schema_version()
