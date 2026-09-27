"""
The class DataIntegrityChecker looks for inconsistencies that foreign keys
and ORM cascades cannot catch on their own.
~~~~~~~~~~~~~~~~~~~~~
Most orphan scenarios are already prevented by foreign-key constraints and
SQLAlchemy ``cascade="all, delete-orphan"``. This module covers the three
remaining application-level invariants:

1. Orphaned ``labelToMaster`` rows -- ``urlHash`` has no foreign key to
   ``fileMaster`` (impossible, since it is not that table's primary key),
   so a label association can outlive the file it points at.
2. Storage type vs. content consistency -- a version whose ``storageTypeID``
   is ``DATABASE`` must have a matching ``fileContent`` row, and a version
   stored anywhere else must not. No foreign key can express this.
3. URL hash consistency -- the stored ``urlHash`` must equal the SHA-256 of
   the stored ``url``. Drift only happens through a bug or a direct edit.

Checks are read-only by default. Passing ``fix=True`` performs only repairs
that are unambiguously safe (deleting orphaned label associations); the other
two findings are reported but never auto-repaired, because there is no safe
way to invent missing content or to decide which side of a hash mismatch is
correct.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from dataclasses import dataclass, field
import logging

# external dependencies:
from sqlalchemy.orm import Session

from exoskeleton import database_connection
from exoskeleton import exo_url
from exoskeleton import models
from exoskeleton.action_types import StorageTypeId

logger = logging.getLogger(__name__)


@dataclass
class IntegrityReport:
    """Result of a data integrity check.

    Each list holds the primary keys of the offending rows so a caller can
    investigate them. ``repaired`` counts rows that ``fix=True`` changed.
    """

    # labelToMaster.id values whose urlHash matches no fileMaster row:
    orphaned_label_associations: list[int] = field(default_factory=list)
    # fileVersions.id values with DATABASE storage but no fileContent row:
    missing_database_content: list[str] = field(default_factory=list)
    # fileVersions.id values not stored in the database yet having content:
    unexpected_database_content: list[str] = field(default_factory=list)
    # fileMaster.id values whose urlHash does not match SHA-256 of their url:
    url_hash_mismatches: list[int] = field(default_factory=list)
    # Count of rows changed by fix=True, keyed by repair name:
    repaired: dict[str, int] = field(default_factory=dict)

    @property
    def is_clean(self) -> bool:
        "True if no inconsistency was found."
        return not (self.orphaned_label_associations
                    or self.missing_database_content
                    or self.unexpected_database_content
                    or self.url_hash_mismatches)

    def summary(self) -> str:
        "One-line human readable summary of the findings."
        if self.is_clean:
            return "Data integrity check passed: no issues found."
        return (
            "Data integrity issues found: "
            f"{len(self.orphaned_label_associations)} orphaned label "
            f"association(s), {len(self.missing_database_content)} version(s) "
            "missing database content, "
            f"{len(self.unexpected_database_content)} version(s) with "
            f"unexpected database content, {len(self.url_hash_mismatches)} "
            "URL hash mismatch(es).")


class DataIntegrityChecker:
    "Check the database for inconsistencies FKs and cascades cannot catch."

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection
    ) -> None:
        self.db_connection = db_connection

    def check(self, fix: bool = False) -> IntegrityReport:
        """Run all integrity checks in a single transaction.

        Args:
            fix: If True, delete orphaned label associations. The other two
                 findings are reported but never auto-repaired.

        Returns:
            An IntegrityReport describing every inconsistency found.
        """
        report = IntegrityReport()
        with self.db_connection.session_scope() as session:
            report.orphaned_label_associations = \
                self._find_orphaned_label_associations(session)
            report.missing_database_content, report.unexpected_database_content = \
                self._check_storage_content_consistency(session)
            report.url_hash_mismatches = \
                self._find_url_hash_mismatches(session)

            if fix and report.orphaned_label_associations:
                deleted = self._delete_orphaned_label_associations(
                    session, report.orphaned_label_associations)
                report.repaired['orphaned_label_associations'] = deleted

        if report.is_clean:
            logger.info(report.summary())
        else:
            logger.warning(report.summary())
            if report.missing_database_content or report.unexpected_database_content:
                logger.warning(
                    "Storage/content mismatches are never auto-repaired: "
                    "fix them manually after investigating each version.")
            if report.url_hash_mismatches:
                logger.warning(
                    "URL hash mismatches are never auto-repaired: a wrong "
                    "hash is also the join key to labels and the queue.")
        return report

    @staticmethod
    def _find_orphaned_label_associations(session: Session) -> list[int]:
        "labelToMaster rows whose urlHash matches no fileMaster row."
        existing_hashes = session.query(models.FileMaster.urlHash)
        orphans = session.query(models.LabelToMaster.id).filter(
            models.LabelToMaster.urlHash.notin_(existing_hashes)
        ).all()
        return [row.id for row in orphans]

    @staticmethod
    def _check_storage_content_consistency(
            session: Session) -> tuple[list[str], list[str]]:
        """Compare each version's storage type with the presence of content.

        Returns:
            (missing, unexpected) where missing are DATABASE versions without
            a fileContent row and unexpected are non-DATABASE versions that
            nonetheless have one.
        """
        missing = [
            row.id for row in session.query(models.FileVersion.id).outerjoin(
                models.FileContent,
                models.FileVersion.id == models.FileContent.versionID
            ).filter(
                models.FileVersion.storageTypeID == StorageTypeId.DATABASE,
                models.FileContent.versionID.is_(None)
            ).all()
        ]
        unexpected = [
            row.id for row in session.query(models.FileVersion.id).join(
                models.FileContent,
                models.FileVersion.id == models.FileContent.versionID
            ).filter(
                models.FileVersion.storageTypeID != StorageTypeId.DATABASE
            ).all()
        ]
        return missing, unexpected

    @staticmethod
    def _find_url_hash_mismatches(session: Session) -> list[int]:
        "fileMaster rows whose urlHash does not match SHA-256 of their url."
        mismatches = []
        rows = session.query(
            models.FileMaster.id,
            models.FileMaster.url,
            models.FileMaster.urlHash
        ).yield_per(1000)
        for row in rows:
            expected = exo_url.ExoUrl.generate_sha256_hash(row.url)
            if expected != row.urlHash:
                mismatches.append(row.id)
        return mismatches

    @staticmethod
    def _delete_orphaned_label_associations(
            session: Session, ids: list[int]) -> int:
        "Delete the given labelToMaster rows. Returns the number deleted."
        deleted = session.query(models.LabelToMaster).filter(
            models.LabelToMaster.id.in_(ids)
        ).delete(synchronize_session=False)
        logger.info(f"Deleted {deleted} orphaned label association(s).")
        return int(deleted)
