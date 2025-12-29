"""
The class BlocklistManager manages the host blocklist
for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
import logging
from hashlib import sha256
from typing import Optional

# external dependencies:
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from exoskeleton import database_connection
from exoskeleton import models

logger = logging.getLogger(__name__)


class BlocklistManager:
    "Manage the host blocklist for the exoskeleton framework"
    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection
            ) -> None:
        self.db_connection = db_connection
        self.session: Session = self.db_connection.get_session()

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
        # Calculate FQDN hash the same way the database does (SHA256)
        fqdn_hash = sha256(fqdn.encode('utf-8')).hexdigest()

        count = self.session.query(models.BlockList).filter(
            models.BlockList.fqdnHash == fqdn_hash
        ).count()

        return count > 0

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None) -> None:
        """Add a specific fully qualified domain name (fqdn)
           - like www.example.com - to the blocklist. Does not handle URLs."""
        fqdn = self.__check_fqdn(fqdn)
        try:
            self.db_connection.call_procedure('block_fqdn_SP', (fqdn, comment))
        except IntegrityError:
            # Just log, do not raise as it does not matter.
            logger.info(f"FQDN {fqdn} already on blocklist.")

    def unblock_fqdn(self,
                     fqdn: str) -> None:
        "Remove a specific FQDN from the blocklist."
        fqdn = self.__check_fqdn(fqdn)
        self.db_connection.call_procedure('unblock_fqdn_SP', (fqdn,))

    def truncate_blocklist(self) -> None:
        "Remove *all* entries from the blocklist."
        self.db_connection.call_procedure('truncate_blocklist_SP')
        logger.info("Truncated the blocklist.")
