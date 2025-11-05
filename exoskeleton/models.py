#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SQLAlchemy models for the exoskeleton framework database schema.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt:
Released under the Apache License 2.0
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, String, Integer, Text, TIMESTAMP, Boolean,
    ForeignKey, Index, SmallInteger, func
)
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.dialects.mysql import TINYINT, MEDIUMTEXT, CHAR


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
    addedToQueue = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    causesError = Column(Integer, ForeignKey('errorType.id'), nullable=True, index=True)
    numTries = Column(Integer, default=0)
    delayUntil = Column(TIMESTAMP, nullable=True, index=True)

    # Relationships
    action_rel = relationship("Action", back_populates="queue_items")
    error_rel = relationship("ErrorType", back_populates="queue_items")


class Job(Base):
    """Jobs table - manages multi-page traversal jobs."""
    __tablename__ = 'jobs'

    jobName = Column(String(127), primary_key=True)
    created = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
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

    # Relationships
    versions = relationship("FileVersion", back_populates="file_master", cascade="all, delete-orphan")
    labels = relationship("LabelToMaster", back_populates="file_master")


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

    # Relationships
    file_master = relationship("FileMaster", back_populates="versions")
    storage_type = relationship("StorageType", back_populates="file_versions")
    action_applied = relationship("Action", back_populates="file_versions")
    content = relationship("FileContent", back_populates="version", uselist=False, cascade="all, delete-orphan")
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

    # Relationships
    master_associations = relationship("LabelToMaster", back_populates="label")
    version_associations = relationship("LabelToVersion", back_populates="label")


class LabelToMaster(Base):
    """LabelToMaster table - links labels to file master entries."""
    __tablename__ = 'labelToMaster'

    id = Column(Integer, primary_key=True, autoincrement=True)
    labelID = Column(Integer, ForeignKey('labels.id'), nullable=False, index=True)
    urlHash = Column(CHAR(64), nullable=False, index=True)

    # Relationships
    label = relationship("Label", back_populates="master_associations")
    file_master = relationship("FileMaster", back_populates="labels")


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
