"""
SQLAlchemy models for the exoskeleton framework database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

from sqlalchemy import (
    Column, String, Integer, Text, TIMESTAMP, Boolean,
    ForeignKey, func
)
from sqlalchemy.orm import DeclarativeBase, relationship, validates
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

    id = Column(CHAR(32), primary_key=True)
    action = Column(TINYINT(unsigned=True), ForeignKey('actions.id'), nullable=False)
    prettifyHtml = Column(TINYINT(unsigned=True), default=0)
    url = Column(Text, nullable=False)
    urlHash = Column(CHAR(64), nullable=False, index=True)
    fqdnHash = Column(CHAR(64), nullable=False)
    addedToQueue = Column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    causesError = Column(Integer, ForeignKey('errorType.id'), nullable=True, index=True)
    numTries = Column(Integer, default=0)
    delayUntil = Column(TIMESTAMP, nullable=True, index=True)
    # Lease timestamp for atomic task claiming: a worker sets this when it
    # claims the task so concurrent workers skip it until the lease expires.
    lockedUntil = Column(TIMESTAMP, nullable=True, index=True)

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

    jobName = Column(String(127), primary_key=True)
    created = Column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    finished = Column(TIMESTAMP, nullable=True, index=True)
    startUrl = Column(Text, nullable=False)
    startUrlHash = Column(CHAR(64), nullable=False)
    currentUrl = Column(Text, nullable=True)


class Action(Base):
    """Actions table - defines available actions."""
    __tablename__ = 'actions'

    id = Column(TINYINT(unsigned=True), primary_key=True)
    description = Column(String(256), nullable=False)

    # Relationships
    queue_items = relationship("Queue", back_populates="action_rel")
    file_versions = relationship("FileVersion", back_populates="action_applied")


class ErrorType(Base):
    """ErrorType table - defines error types and their permanence."""
    __tablename__ = 'errorType'

    id = Column(Integer, primary_key=True)
    short = Column(String(31))
    description = Column(String(255))
    permanent = Column(Boolean, index=True)

    # Relationships
    queue_items = relationship("Queue", back_populates="error_rel")


class StorageType(Base):
    """StorageTypes table - defines file storage locations."""
    __tablename__ = 'storageTypes'

    id = Column(Integer, primary_key=True, autoincrement=False)
    shortName = Column(String(15))
    fullName = Column(String(63))

    # Relationships
    file_versions = relationship("FileVersion", back_populates="storage_type")


class FileMaster(Base):
    """FileMaster table - master list of all files."""
    __tablename__ = 'fileMaster'

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    urlHash = Column(CHAR(64), nullable=False, unique=True)
    numVersions_t = Column(Integer, default=0)

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

    id = Column(CHAR(32), primary_key=True)
    fileMasterID = Column(Integer, ForeignKey('fileMaster.id'), nullable=False, index=True)
    storageTypeID = Column(Integer, ForeignKey('storageTypes.id'), nullable=False, index=True)
    actionAppliedID = Column(TINYINT(unsigned=True), ForeignKey('actions.id'), nullable=False)
    fileName = Column(String(255), nullable=True)
    mimeType = Column(String(127), nullable=True)
    pathOrBucket = Column(String(2048), nullable=True)
    versionTimestamp = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    size = Column(Integer, nullable=True)
    hashMethod = Column(String(6), nullable=True)
    hashValue = Column(String(512), nullable=True)
    comment = Column(String(256), nullable=True)

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

    versionID = Column(CHAR(32), ForeignKey('fileVersions.id'), primary_key=True)
    pageContent = Column(MEDIUMTEXT, nullable=False)

    # Relationships
    version = relationship("FileVersion", back_populates="content")


class StatisticsHost(Base):
    """StatisticsHosts table - tracks statistics per host."""
    __tablename__ = 'statisticsHosts'

    fqdnHash = Column(CHAR(64), primary_key=True)
    fqdn = Column(String(255), nullable=False)
    firstSeen = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    lastSeen = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(),
                      onupdate=func.current_timestamp(), index=True)
    successfulRequests = Column(Integer, nullable=False, default=0)
    temporaryProblems = Column(Integer, nullable=False, default=0)
    permamentErrors = Column(Integer, nullable=False, default=0)
    hitRateLimit = Column(Integer, nullable=False, default=0)


class Label(Base):
    """Labels table - defines available labels."""
    __tablename__ = 'labels'

    id = Column(Integer, primary_key=True, autoincrement=True)
    shortName = Column(String(63), nullable=False, unique=True)
    description = Column(Text, nullable=True)

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    labelID = Column(Integer, ForeignKey('labels.id'), nullable=False, index=True)
    urlHash = Column(CHAR(64), nullable=False, index=True)

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    labelID = Column(Integer, ForeignKey('labels.id'), nullable=False, index=True)
    versionUUID = Column(CHAR(32), ForeignKey('fileVersions.id'),
                         nullable=False, index=True)

    # Relationships
    label = relationship("Label", back_populates="version_associations")
    version = relationship("FileVersion", back_populates="labels")


class BlockList(Base):
    """BlockList table - stores blocked domains."""
    __tablename__ = 'blockList'

    fqdnHash = Column(CHAR(64), primary_key=True, unique=True)
    fqdn = Column(String(255), nullable=False)
    comment = Column(Text, nullable=True)


class RateLimit(Base):
    """RateLimits table - tracks rate limiting per host."""
    __tablename__ = 'rateLimits'

    fqdnHash = Column(CHAR(64), primary_key=True)
    fqdn = Column(String(255), nullable=False)
    hitRateLimitAt = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    noContactUntil = Column(TIMESTAMP, nullable=False, index=True)


class ExoInfo(Base):
    """ExoInfo table - stores installation metadata."""
    __tablename__ = 'exoInfo'

    exoKey = Column(CHAR(32), primary_key=True)
    exoValue = Column(String(64), nullable=False)
