#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database connection management for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
from collections import defaultdict
import logging


# external dependencies:
import pymysql
import userprovided


class DatabaseConnection:
    u"""Database connection management for the exoskeleton framework."""

    def __init__(self,
                 database_settings: dict):
        u"""Sets defaults"""

        if database_settings is None:
            raise ValueError('You must supply database credentials for' +
                             'exoskeleton to work.')

        # Are the parameters valid?
        userprovided.parameters.validate_dict_keys(
            database_settings,
            {'host', 'port', 'database', 'username', 'passphrase'},
            {'database'},
            'database_settings')

        # Check settings and fallback to default if necessary:
        self.db_host: str = database_settings.get('host', None)
        if not self.db_host:
            logging.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port: int = database_settings.get('port', None)
        if not self.db_port:
            logging.info('No port number supplied. ' +
                         'Will try standard port 3306 instead.')
            self.db_port = 3306
        elif not userprovided.port.port_in_range(self.db_port):
            raise ValueError('Database port outside valid range!')

        self.db_name: str = database_settings.get('database', None)
        if not self.db_name:
            raise ValueError('You must provide the name of the database.')

        self.db_username: str = database_settings.get('username', None)
        if not self.db_username:
            raise ValueError('You must provide a database user.')

        self.db_passphrase: str = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logging.warning('No database passphrase provided. ' +
                            'Will try to connect without.')

        # Establish the database connection
        self.connection = None
        self.establish_db_connection()
        # Add ignore for mypy as it cannot be None at this point, because
        # establish_db_connection would have failed before:
        self.cur = self.connection.cursor()  # type: ignore

        # Check the schema:
        self.check_table_existence(self.cur)
        self.check_stored_procedures(self.cur, self.db_name)

    def __del__(self):
        # make sure the connection is closed instead of waiting for timeout:
        self.connection.close()

    def establish_db_connection(self):
        u"""Establish a connection to MariaDB """
        try:
            logging.debug('Trying to connect to database.')
            self.connection = pymysql.connect(host=self.db_host,
                                              port=self.db_port,
                                              database=self.db_name,
                                              user=self.db_username,
                                              password=self.db_passphrase,
                                              autocommit=True)

            logging.info('Established database connection.')

        except pymysql.InterfaceError:
            logging.exception('Exception related to the database ' +
                              '*interface*.', exc_info=True)
            raise
        except pymysql.DatabaseError:
            logging.exception('Exception related to the database.',
                              exc_info=True)
            raise
        except Exception:
            logging.exception('Unknown exception while ' +
                              'trying to connect to the DBMS.',
                              exc_info=True)
            raise

    def check_table_existence(self,
                              db_cursor) -> bool:
        u"""Check if all expected tables exist."""
        logging.debug('Checking if the database table structure is complete.')
        expected_tables = ['actions',
                           'blockList',
                           'errorType',
                           'fileContent',
                           'fileMaster',
                           'fileVersions',
                           'jobs',
                           'labels',
                           'labelToMaster',
                           'labelToVersion',
                           'queue',
                           'statisticsHosts',
                           'storageTypes']
        tables_count = 0

        db_cursor.execute('SHOW TABLES;')
        tables = db_cursor.fetchall()
        if not tables:
            logging.error('The database exists, but no tables found!')
            raise OSError('Database table structure missing. ' +
                          'Run generator script!')
        else:
            tables_found = [item[0] for item in tables]
            for table in expected_tables:
                if table in tables_found:
                    tables_count += 1
                    logging.debug('Found table %s', table)
                else:
                    logging.error('Table %s not found.', table)

        if tables_count != len(expected_tables):
            raise RuntimeError('Database Schema Incomplete: Missing Tables!')

        logging.info("Found all expected tables.")
        return True

    def check_stored_procedures(self,
                                db_cursor,
                                db_name: str) -> bool:
        u"""Check if all expected stored procedures exist and if the user
        is allowed to execute them. """
        logging.debug('Checking if stored procedures exist.')
        expected_procedures = ['delete_all_versions_SP',
                               'delete_from_queue_SP',
                               'insert_content_SP',
                               'insert_file_SP',
                               'next_queue_object_SP']

        procedures_count = 0
        db_cursor.execute('SELECT SPECIFIC_NAME ' +
                          'FROM INFORMATION_SCHEMA.ROUTINES ' +
                          'WHERE ROUTINE_SCHEMA = %s;',
                          db_name)
        procedures = db_cursor.fetchall()
        procedures_found = [item[0] for item in procedures]
        for procedure in expected_procedures:
            if procedure in procedures_found:
                procedures_count += 1
                logging.debug('Found stored procedure %s', procedure)
            else:
                logging.error('Stored Procedure %s is missing (create it ' +
                              'with the database script) or the user lacks ' +
                              'permissions to use it.', procedure)

        if procedures_count != len(expected_procedures):
            raise RuntimeError('Database Schema Incomplete: ' +
                               'Missing Stored Procedures!')

        logging.info("Found all expected stored procedures.")
        return True

    def get_cursor(self):
        u"""Make the database cursor accessible from outside the class.
            Try to reconnect if the connection is lost."""
        if self.cur:
            return self.cur
        else:
            logging.info("Lost database connection. Trying to reconnect...")
            self.establish_db_connection()
            self.cur = self.connection.cursor()
            return self.cur
