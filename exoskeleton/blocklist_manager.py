#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class BlocklistManager manages the host blocklist
for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import Optional

# external dependencies:
import pymysql


from exoskeleton import database_connection


class BlocklistManager:
    "Manage the host blocklist for the exoskeleton framework"
    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection
            ) -> None:
        self.db_connection = db_connection
        self.cur: pymysql.cursors.Cursor = self.db_connection.get_cursor()

    @staticmethod
    def __check_fqdn(fqdn: str) -> str:
        "Remove whitespace and check if it can be a FQDN"
        fqdn = fqdn.strip()
        if len(fqdn) > 255:
            raise ValueError(
                'Not a valid FQDN. Exoskeleton blocks on the hostname level ' +
                '- not specific URLs.')
        return fqdn

    def check_blocklist(self,
                        fqdn: str) -> bool:
        "Check if a specific FQDN is on the blocklist."
        fqdn = self.__check_fqdn(fqdn)
        self.cur.execute('SELECT fqdn_on_blocklist(%s);', (fqdn, ))
        response = self.cur.fetchone()
        return bool(response[0]) if response else False  # type: ignore[index]

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None) -> None:
        """Add a specific fully qualified domain name (fqdn)
           - like www.example.com - to the blocklist. Does not handle URLs."""
        fqdn = self.__check_fqdn(fqdn)
        try:
            self.cur.callproc('block_fqdn_SP', (fqdn, comment))
        except pymysql.err.IntegrityError:
            # Just log, do not raise as it does not matter.
            logging.info("FQDN {fqdn} already on blocklist.")

    def unblock_fqdn(self,
                     fqdn: str) -> None:
        "Remove a specific FQDN from the blocklist."
        fqdn = self.__check_fqdn(fqdn)
        self.cur.callproc('unblock_fqdn_SP', (fqdn, ))

    def truncate_blocklist(self) -> None:
        "Remove *all* entries from the blocklist."
        self.cur.callproc('truncate_blocklist_SP')
        logging.info("Truncated the blocklist.")
