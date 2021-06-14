#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database connection management for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
from collections import defaultdict  # noqa # pylint: disable=unused-import
import logging

# external dependencies:
import pymysql
import userprovided

from exoskeleton import _version as version


class DatabaseConnection:
    """Database connection management for the exoskeleton framework."""

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
                  'labels_filemaster_by_id_SP',
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
                 database_settings: dict) -> None:
        """Sets defaults"""

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
        self.db_host: str = database_settings.get('host', None)
        if not self.db_host:
            logging.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port: int = database_settings.get('port', None)
        if not self.db_port:
            logging.info(
                'No port number supplied: will try standard port 3306.')
            self.db_port = 3306
        elif not userprovided.parameters.is_port(self.db_port):
            raise ValueError('Database port outside valid range!')

        self.db_passphrase: str = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logging.warning(
                'No database passphrase provided. Trying to connect without.')

        # Establish the database connection
        self.connection = None
        self.establish_db_connection()
        # Add ignore for mypy as it cannot be None at this point, because
        # establish_db_connection would have failed before:
        self.cur = self.connection.cursor()  # type: ignore

        self.check_db_schema()

    def __del__(self) -> None:
        # make sure the connection is closed instead of waiting for timeout:
        self.connection.close()  # type: ignore[attr-defined]

    def establish_db_connection(self) -> None:
        """Establish a connection to MariaDB """
        try:
            logging.debug('Trying to connect to database.')
            self.connection = pymysql.connect(host=self.db_host,
                                              port=self.db_port,
                                              database=self.db_name,
                                              user=self.db_username,
                                              password=self.db_passphrase,
                                              autocommit=True
                                              )  # type: ignore[assignment]

            logging.info('Succesfully established database connection.')

        except pymysql.InterfaceError:
            logging.exception('Exception related to the database *interface*.',
                              exc_info=True)
            raise
        except pymysql.DatabaseError:
            logging.exception('Exception related to the database.',
                              exc_info=True)
            raise
        except Exception:
            logging.exception(
                'Unknown exception while trying to connect to the DBMS.',
                exc_info=True)
            raise

    def __check_table_existence(self) -> bool:
        "Check if all expected tables exist."
        tables_count = 0
        self.cur.execute('SHOW TABLES;')
        tables = self.cur.fetchall()
        if not tables:
            logging.error('The database exists, but no tables found!')
            raise OSError('No tables found in database: Run generator script!')

        tables_found = [item[0] for item in tables]
        for table in self.TABLES:
            if table in tables_found:
                tables_count += 1
            else:
                logging.error('Table %s not found.', table)

        if tables_count != len(self.TABLES):
            raise RuntimeError('Database Schema Incomplete: Missing Tables!')

        logging.debug('Database schema: found all expected tables.')
        return True

    def __check_stored_procedures(self) -> bool:
        """Check if all expected stored procedures exist and if the user
           is allowed to execute them. """
        self.cur.callproc('db_check_all_procedures_SP', (self.db_name, ))
        procedures = self.cur.fetchall()
        procedures_found = [item[0] for item in procedures]
        count = 0
        for procedure in self.PROCEDURES:
            if procedure not in procedures_found:
                # Do not raise the exception just now.
                # Log all missing procedures first
                logging.error('Stored Procedure %s is missing (create it ' +
                              'with the database script) or user lacks ' +
                              'permissions to use it.', procedure)
            else:
                count += 1
        if count != len(self.PROCEDURES):
            raise RuntimeError(
                'Database Schema Incomplete: Missing Stored Procedures!')
        logging.debug('Database schema: found all expected stored procedures.')
        return True

    def __check_functions(self) -> bool:
        """Check if all expected database functions exist and if the user
           is allowed to execute them. """
        self.cur.callproc('db_check_all_functions_SP', (self.db_name, ))
        functions = self.cur.fetchall()
        functions_found = [item[0] for item in functions]
        count = 0
        for function in self.FUNCTIONS:
            if function not in functions_found:
                logging.error(
                    'Function %s is missing (create it with the database ' +
                    'script) or user lacks permissions to use it.', function)
            else:
                count += 1
        if count != len(self.FUNCTIONS):
            raise RuntimeError(
                'Database Schema Incomplete: Missing Functions!')
        logging.debug('Database schema: found all expected functions.')
        return True

    def __check_schema_version(self) -> None:
        """Check if the database schema is compatible with this version
           of exoskeleton"""
        # Other methods check for the existence of stored procedures, functions
        # and tables. However, those might have changed. Foremost: this gives
        # the user a hint how much has changed!
        try:
            self.cur.execute('SELECT version FROM exo_schema_version();')
            schema = self.cur.fetchone()
            if not schema:
                logging.error(
                    'Found no version information for the database schema.')
            elif schema[0] == '1.4.0':
                logging.info('Database schema matches version of exoskeleton.')
            else:
                logging.warning("Mismatch between version of exoskeleton " +
                                f"({version.__version__}) and version of " +
                                f"the database schema ({schema[0]}).")
        except pymysql.ProgrammingError:
            # means: the table does not exist (i.e. before version 1.1.0)
            logging.error('Found no information about the version of the ' +
                          'database schema. Please ensure your version of ' +
                          'exoskeleton and the database script match!')

    def check_db_schema(self) -> None:
        """Check whether all expected tables, stored procedures and functions
           are available in the database. Then look for a version string."""
        self.__check_schema_version()
        self.__check_table_existence()
        self.__check_stored_procedures()
        self.__check_functions()

    def get_cursor(self) -> pymysql.cursors.Cursor:
        """Make the database cursor accessible from outside the class.
        Try to reconnect if the connection is lost."""
        if self.cur:
            return self.cur
        logging.info("Lost database connection. Trying to reconnect...")
        self.establish_db_connection()
        self.cur = self.connection.cursor()  # type: ignore[attr-defined]
        return self.cur
