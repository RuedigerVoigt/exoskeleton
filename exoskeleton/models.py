"""
SQLAlchemy models for the exoskeleton framework database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

from datetime import datetime

from sqlalchemy import (
    String, Integer, Text, TIMESTAMP, Boolean,
    ForeignKey, func
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, validates
)
from sqlalchemy.dialects.mysql import TINYINT, MEDIUMTEXT, CHAR


def _validate_sha256_hex(value: str, field: str) -> str:
    """Raise ValueError if value is not a 64-character lowercase hex string."""
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError(f"{field} must be a 64-character SHA-256 hex string")
    if not all(c in '0123456789abcdef' for c in value):
        raise ValueError(f"{field} must contain only lowercase hex characters")
    return value


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


class Queue(Base):
    """Queue table - manages the download queue."""
    __tablename__ = 'queue'

    id: Mapped[str] = mapped_column(CHAR(32), primary_key=True)
    action: Mapped[int] = mapped_column(TINYINT(unsigned=True), ForeignKey('actions.id'), nullable=False)
    prettifyHtml: Mapped[int | None] = mapped_column(TINYINT(unsigned=True), default=0)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    urlHash: Mapped[str] = mapped_column(CHAR(64), nullable=False, index=True)
    fqdnHash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    addedToQueue: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    causesError: Mapped[int | None] = mapped_column(Integer, ForeignKey('errorType.id'), nullable=True, index=True)
    numTries: Mapped[int | None] = mapped_column(Integer, default=0)
    delayUntil: Mapped[datetime | None] = mapped_column(TIMESTAMP, nullable=True, index=True)
    # Lease timestamp for atomic task claiming: a worker sets this when it
    # claims the task so concurrent workers skip it until the lease expires.
    lockedUntil: Mapped[datetime | None] = mapped_column(TIMESTAMP, nullable=True, index=True)

    # Relationships
    action_rel = relationship("Action", back_populates="queue_items")
    error_rel = relationship("ErrorType", back_populates="queue_items")

    @validates('urlHash')
    def validate_url_hash(self, key: str, value: str) -> str:
        return _validate_sha256_hex(value, 'Queue.urlHash')

    @validates('fqdnHash')
    def validate_fqdn_hash(self, key: str, value: str) -> str:
        return _validate_sha256_hex(value, 'Queue.fqdnHash')


class Job(Base):
    """Jobs table - manages multi-page traversal jobs."""
    __tablename__ = 'jobs'

    jobName: Mapped[str] = mapped_column(String(127), primary_key=True)
    created: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    finished: Mapped[datetime | None] = mapped_column(TIMESTAMP, nullable=True, index=True)
    startUrl: Mapped[str] = mapped_column(Text, nullable=False)
    startUrlHash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    currentUrl: Mapped[str | None] = mapped_column(Text, nullable=True)


class Action(Base):
    """Actions table - defines available actions."""
    __tablename__ = 'actions'

    id: Mapped[int] = mapped_column(TINYINT(unsigned=True), primary_key=True)
    description: Mapped[str] = mapped_column(String(256), nullable=False)

    # Relationships
    queue_items = relationship("Queue", back_populates="action_rel")
    file_versions = relationship("FileVersion", back_populates="action_applied")


class ErrorType(Base):
    """ErrorType table - defines error types and their permanence."""
    __tablename__ = 'errorType'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    short: Mapped[str | None] = mapped_column(String(31))
    description: Mapped[str | None] = mapped_column(String(255))
    permanent: Mapped[bool | None] = mapped_column(Boolean, index=True)

    # Relationships
    queue_items = relationship("Queue", back_populates="error_rel")


class StorageType(Base):
    """StorageTypes table - defines file storage locations."""
    __tablename__ = 'storageTypes'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    shortName: Mapped[str | None] = mapped_column(String(15))
    fullName: Mapped[str | None] = mapped_column(String(63))

    # Relationships
    file_versions = relationship("FileVersion", back_populates="storage_type")


class FileMaster(Base):
    """FileMaster table - master list of all files."""
    __tablename__ = 'fileMaster'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    urlHash: Mapped[str] = mapped_column(CHAR(64), nullable=False, unique=True)
    numVersions_t: Mapped[int | None] = mapped_column(Integer, default=0)

    @validates('urlHash')
    def validate_url_hash(self, key: str, value: str) -> str:
        return _validate_sha256_hex(value, 'FileMaster.urlHash')

    # Relationships
    versions = relationship(
        "FileVersion", back_populates="file_master", cascade="all, delete-orphan")
    labels = relationship("LabelToMaster",
                          back_populates="file_master",
                          foreign_keys="[LabelToMaster.urlHash]",
                          primaryjoin="FileMaster.urlHash==LabelToMaster.urlHash")


class FileVersion(Base):
    """FileVersions table - stores file version information."""
    __tablename__ = 'fileVersions'

    id: Mapped[str] = mapped_column(CHAR(32), primary_key=True)
    fileMasterID: Mapped[int] = mapped_column(Integer, ForeignKey('fileMaster.id'), nullable=False, index=True)
    storageTypeID: Mapped[int] = mapped_column(Integer, ForeignKey('storageTypes.id'), nullable=False, index=True)
    actionAppliedID: Mapped[int] = mapped_column(TINYINT(unsigned=True), ForeignKey('actions.id'), nullable=False)
    fileName: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mimeType: Mapped[str | None] = mapped_column(String(127), nullable=True)
    pathOrBucket: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    versionTimestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hashMethod: Mapped[str | None] = mapped_column(String(6), nullable=True)
    hashValue: Mapped[str | None] = mapped_column(String(512), nullable=True)
    comment: Mapped[str | None] = mapped_column(String(256), nullable=True)

    @validates('hashMethod')
    def validate_hash_method(self, key: str, value: str) -> str:
        if value is not None and value != 'sha256':
            raise ValueError(f"Unsupported hashMethod: {value!r}")
        return value

    # Relationships
    file_master = relationship("FileMaster", back_populates="versions")
    storage_type = relationship("StorageType", back_populates="file_versions")
    action_applied = relationship("Action", back_populates="file_versions")
    content = relationship(
        "FileContent", back_populates="version", uselist=False, cascade="all, delete-orphan")
    labels = relationship("LabelToVersion", back_populates="version")


class FileContent(Base):
    """FileContent table - stores page content in database."""
    __tablename__ = 'fileContent'

    versionID: Mapped[str] = mapped_column(CHAR(32), ForeignKey('fileVersions.id'), primary_key=True)
    pageContent: Mapped[str] = mapped_column(MEDIUMTEXT, nullable=False)

    # Relationships
    version = relationship("FileVersion", back_populates="content")


class StatisticsHost(Base):
    """StatisticsHosts table - tracks statistics per host."""
    __tablename__ = 'statisticsHosts'

    fqdnHash: Mapped[str] = mapped_column(CHAR(64), primary_key=True)
    fqdn: Mapped[str] = mapped_column(String(255), nullable=False)
    firstSeen: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    lastSeen: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(), index=True)
    successfulRequests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    temporaryProblems: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    permamentErrors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hitRateLimit: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Label(Base):
    """Labels table - defines available labels."""
    __tablename__ = 'labels'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shortName: Mapped[str] = mapped_column(String(63), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    @validates('shortName')
    def validate_short_name(self, key: str, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Label shortName cannot be empty")
        if len(value) > 63:
            raise ValueError(
                f"Label shortName exceeds 63 characters: {value!r}")
        return value

    # Relationships
    master_associations = relationship("LabelToMaster", back_populates="label")
    version_associations = relationship("LabelToVersion", back_populates="label")


class LabelToMaster(Base):
    """LabelToMaster table - links labels to file master entries."""
    __tablename__ = 'labelToMaster'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    labelID: Mapped[int] = mapped_column(Integer, ForeignKey('labels.id'), nullable=False, index=True)
    urlHash: Mapped[str] = mapped_column(CHAR(64), nullable=False, index=True)

    @validates('urlHash')
    def validate_url_hash(self, key: str, value: str) -> str:
        return _validate_sha256_hex(value, 'LabelToMaster.urlHash')

    # Relationships
    label = relationship("Label", back_populates="master_associations")
    file_master = relationship("FileMaster",
                               back_populates="labels",
                               foreign_keys="[LabelToMaster.urlHash]",
                               primaryjoin="FileMaster.urlHash==LabelToMaster.urlHash")


class LabelToVersion(Base):
    """LabelToVersion table - links labels to specific file versions."""
    __tablename__ = 'labelToVersion'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    labelID: Mapped[int] = mapped_column(Integer, ForeignKey('labels.id'), nullable=False, index=True)
    versionUUID: Mapped[str] = mapped_column(
        CHAR(32), ForeignKey('fileVersions.id'), nullable=False, index=True)

    # Relationships
    label = relationship("Label", back_populates="version_associations")
    version = relationship("FileVersion", back_populates="labels")


class BlockList(Base):
    """BlockList table - stores blocked domains."""
    __tablename__ = 'blockList'

    fqdnHash: Mapped[str] = mapped_column(CHAR(64), primary_key=True, unique=True)
    fqdn: Mapped[str] = mapped_column(String(255), nullable=False)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)


class RateLimit(Base):
    """RateLimits table - tracks rate limiting per host."""
    __tablename__ = 'rateLimits'

    fqdnHash: Mapped[str] = mapped_column(CHAR(64), primary_key=True)
    fqdn: Mapped[str] = mapped_column(String(255), nullable=False)
    hitRateLimitAt: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    noContactUntil: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, index=True)


class ExoInfo(Base):
    """ExoInfo table - stores installation metadata."""
    __tablename__ = 'exoInfo'

    exoKey: Mapped[str] = mapped_column(CHAR(32), primary_key=True)
    exoValue: Mapped[str] = mapped_column(String(64), nullable=False)
