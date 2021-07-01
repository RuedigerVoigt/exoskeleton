#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Check the database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""

import logging

import pymysql

from exoskeleton import _version as version
from exoskeleton import database_connection
from exoskeleton import err


class DatabaseSchemaCheck:
    "Check the database schema for exoskeleton."
    # pylint: disable=too-few-public-methods

    PROCEDURES = ['add_rate_limit_SP',
                  'add_crawl_delay_SP',
                  'add_to_queue_SP',
                  'block_fqdn_SP',
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

    TABLES = ['actions',
              'blockList',
              'errorType',
              'exoInfo',
              'fileContent',
              'fileMaster',
              'fileVersions',
              'jobs',
              'labels',
              'labelToMaster',
              'labelToVersion',
              'queue',
              'rateLimits',
              'statisticsHosts',
              'storageTypes']

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        "Sets defaults"
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        self.db_name: str = db_connection.db_name
        self.check_db_schema()

    def __check_table_existence(self) -> bool:
        "Check if all expected tables exist."
        # Merely comparing the length of the result set to the number of
        # expected tables is not sufficient. The user might have added
        # custom tables.
        # Therefore check for each expected table if it is in the result.
        self.cur.execute('SHOW TABLES;')
        tables = self.cur.fetchall()
        if not tables:
            msg = 'No tables found in database: Run generator script!'
            logging.exception(msg)
            raise err.InvalidDatabaseSchemaError(msg)

        tables_found = [item[0] for item in tables]  # type: ignore[index]
        tables_count = 0
        for table in self.TABLES:
            if table in tables_found:
                tables_count += 1
            else:
                logging.error('Table %s not found.', table)

        if tables_count != len(self.TABLES):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Tables!')

        logging.debug('Database schema: found all expected tables.')
        return True

    def __check_stored_procedures(self) -> bool:
        """Check if all expected stored procedures exist and if the user
           is allowed to execute them. """
        self.cur.callproc('db_check_all_procedures_SP', (self.db_name, ))
        procedures = self.cur.fetchall()
        if not procedures:
            msg = 'No procedures found in database: Run generator script!'
            logging.exception(msg)
            raise RuntimeError(msg)

        procedures_found = [item[0] for item in procedures]  # type: ignore[index]
        count = 0
        for procedure in self.PROCEDURES:
            if procedure not in procedures_found:
                # Do not raise the exception just now.
                # Log all missing procedures first
                logging.error(
                    'Stored Procedure %s is missing or user lacks permissions.',
                    procedure)
            else:
                count += 1
        if count != len(self.PROCEDURES):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Stored Procedures!')
        logging.debug('Database schema: found all expected stored procedures.')
        return True

    def __check_functions(self) -> bool:
        """Check if all expected database functions exist and if the user
           is allowed to execute them. """
        self.cur.callproc('db_check_all_functions_SP', (self.db_name, ))
        functions = self.cur.fetchall()
        if not functions:
            msg = 'No functions found in database: Run generator script!'
            logging.exception(msg)
            raise RuntimeError(msg)

        functions_found = [item[0] for item in functions]  # type: ignore[index]
        count = 0
        for function in self.FUNCTIONS:
            if function not in functions_found:
                logging.error(
                    'Function %s is missing or user lacks permissions.',
                    function)
            else:
                count += 1
        if count != len(self.FUNCTIONS):
            raise err.InvalidDatabaseSchemaError(
                'Database Schema Incomplete: Missing Functions!')
        logging.debug('Database schema: found all expected functions.')
        return True

    def __check_schema_version(self) -> None:
        "Check if the database schema is a compatible version."
        # Other methods check for the existence of stored procedures, functions
        # and tables. However, those might have changed fields. Therefore this
        # version check.
        self.cur.execute('SELECT exo_schema_version() AS version;')
        schema = self.cur.fetchone()
        if not schema:
            msg = 'Schema version info not found.'
            logging.exception(msg)
            raise RuntimeError(msg)

        db_schema = schema[0]  # type: ignore[index]

        # Not taking the comparison value from _version as multiple versions
        # of exoskeleton might share the same database schema.
        if db_schema == '2.0.0':
            logging.info('Database schema matches version of exoskeleton.')
        else:
            raise err.InvalidDatabaseSchemaError(
                "Mismatch between version of exoskeleton " +
                f"({version.__version__}) and version of the database " +
                f"schema ({db_schema}).")

    def check_db_schema(self) -> None:
        """Check whether all expected tables, stored procedures and functions
           are available in the database. Then look for a version string."""
        self.__check_table_existence()
        self.__check_stored_procedures()
        self.__check_functions()
        self.__check_schema_version()
