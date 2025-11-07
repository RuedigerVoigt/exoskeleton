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

from sqlalchemy import text
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

    # Stored procedures - hardcoded as they're not part of ORM
    # Note: Includes db_check_* procedures which are used to verify other procedures/functions
    PROCEDURES = ['add_crawl_delay_SP',
                  'add_rate_limit_SP',
                  'add_to_queue_SP',
                  'block_fqdn_SP',
                  'db_check_all_functions_SP',
                  'db_check_all_procedures_SP',
                  'define_new_job_SP',
                  'delete_all_versions_SP',
                  'delete_from_queue_SP',
                  'forget_all_errors_SP',
                  'forget_all_rate_limits_SP',
                  'forget_error_group_SP',
                  'forget_specific_error_type_SP',
                  'forget_specific_rate_limit_SP',
                  'increment_num_tries_SP',
                  'insert_content_SP',
                  'insert_file_SP',
                  'job_get_current_url_SP',
                  'job_mark_as_finished_SP',
                  'job_update_current_url_SP',
                  'label_define_or_update_SP',
                  'labels_filemaster_by_url_SP',
                  'labels_version_by_id_SP',
                  'mark_permanent_error_SP',
                  'next_queue_object_SP',
                  'remove_labels_from_uuid_SP',
                  'truncate_blocklist_SP',
                  'unblock_fqdn_SP',
                  'update_host_stats_SP']

    FUNCTIONS = ['exo_schema_version',
                 'fqdn_on_blocklist',
                 'get_filemaster_id',
                 'num_items_with_permanent_error',
                 'num_items_with_temporary_errors',
                 'num_tasks_in_queue_without_error',
                 'num_tasks_with_active_rate_limit']

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
        sql_file = current_dir.parent / 'Database-Scripts' / 'Generate-Database-Schema-MariaDB.sql'

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

    def __check_table_existence(self) -> bool:
        """
        Check if all expected tables exist.

        Tables are dynamically retrieved from SQLAlchemy models (models.py),
        ensuring this check stays in sync with the ORM definitions.

        Note: User might have custom tables, so we only verify expected tables exist,
        not that ONLY expected tables exist.
        """
        result = self.session.execute(text('SHOW TABLES'))
        tables = result.fetchall()
        if not tables:
            msg = 'No tables found in database: Run generator script!'
            logger.exception(msg)
            raise err.InvalidDatabaseSchemaError(msg)

        tables_found = [item[0] for item in tables]
        tables_count = 0
        for table in self.TABLES:
            if table in tables_found:
                tables_count += 1
            else:
                logger.error('Table %s not found.', table)

        if tables_count != len(self.TABLES):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Tables!')

        logger.debug('Database schema: found all expected tables.')
        return True

    def __check_stored_procedures(self) -> bool:
        """Check if all expected stored procedures exist and if the user
           is allowed to execute them. """
        result = self.db_connection.call_procedure('db_check_all_procedures_SP', (self.db_name,))
        procedures = result.fetchall()
        if not procedures:
            msg = 'No procedures found in database: Run generator script!'
            logger.exception(msg)
            raise RuntimeError(msg)

        procedures_found = [item[0] for item in procedures]
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
           is allowed to execute them. """
        result = self.db_connection.call_procedure('db_check_all_functions_SP', (self.db_name,))
        functions = result.fetchall()
        if not functions:
            msg = 'No functions found in database: Run generator script!'
            logger.exception(msg)
            raise RuntimeError(msg)

        functions_found = [item[0] for item in functions]
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
        result = self.session.execute(text('SELECT exo_schema_version() AS version'))
        schema = result.fetchone()
        if not schema:
            msg = 'Schema version info not found.'
            logger.exception(msg)
            raise RuntimeError(msg)

        db_schema = schema[0]

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
