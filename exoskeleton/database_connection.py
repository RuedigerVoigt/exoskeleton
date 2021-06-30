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


class DatabaseConnection:
    """Database connection management for the exoskeleton framework."""

    def __init__(self,
                 database_settings: dict) -> None:
        "Checks connection settings and set defaults"

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
            logging.info('No port number supplied: will try port 3306.')
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

    def __del__(self) -> None:
        # make sure the connection is closed instead of waiting for timeout:
        try:
            self.connection.close()  # type: ignore[attr-defined]
        except (Exception, pymysql.Error):  # pylint: disable=broad-except
            pass

    def establish_db_connection(self) -> None:
        "Establish a connection to MariaDB "
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

        except pymysql.OperationalError:
            logging.exception(
                "Cannot connect to DBMS. Did you forget a parameter?",
                exc_info=True)
            raise
        except (pymysql.InterfaceError, pymysql.DatabaseError):
            logging.exception('Database related exception', exc_info=True)
            raise
        except (pymysql.Error, Exception):
            logging.exception(
                'Exception while connecting to the DBMS.', exc_info=True)
            raise

    def get_cursor(self) -> pymysql.cursors.Cursor:
        """Make the database cursor accessible from outside the class.
           Try to reconnect if the connection is lost."""
        if self.cur:
            return self.cur
        logging.info("Lost database connection. Trying to reconnect...")
        self.establish_db_connection()
        self.cur = self.connection.cursor()  # type: ignore[attr-defined]
        return self.cur
