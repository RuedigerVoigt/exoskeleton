#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This manages labels for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""

import logging
from typing import Optional, Union

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import userprovided

from exoskeleton import database_connection
from exoskeleton import exo_url
from exoskeleton import models

logger = logging.getLogger(__name__)


class LabelManager:
    "Manage labels and their association"
    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection) -> None:
        self.db_connection = db_connection
        self.session: Session = self.db_connection.get_session()

    # #########################################################################
    # CREATING LABELS
    # #########################################################################

    @staticmethod
    def __shortname_ok(shortname: str) -> bool:
        "Check if the label's shortname does not exceed 31 characters."
        if len(shortname) > 31:
            logger.error(
                "Cannot add labelname: exceeding max length of 31 characters.")
            return False
        return True

    def define_new_label(self,
                         shortname: str,
                         description: Optional[str] = None) -> None:
        """If the label is not already used, define a new label and description.
           In case the label already exists, do not update the description."""
        if not self.__shortname_ok(shortname):
            return
        try:
            query = """
                INSERT INTO labels (shortName, description)
                VALUES (:shortname, :description)
            """
            self.session.execute(text(query), {"shortname": shortname, "description": description})
            self.session.commit()
            logger.debug('Added label to the database.')
        except IntegrityError:
            self.session.rollback()
            logger.debug('Label already existed.')

    def define_or_update_label(self,
                               shortname: str,
                               description: Optional[str] = None) -> None:
        """ Insert a new label into the database or update its description
            in case it already exists.
            Use __define_new_label if an update has to be avoided. """
        if not self.__shortname_ok(shortname):
            return
        self.db_connection.call_procedure('label_define_or_update_SP',
                                        (shortname, description))

    # #########################################################################
    # ASSIGNING LABELS
    # #########################################################################

    def assign_labels_to_master(self,
                                url: Union[exo_url.ExoUrl, str],
                                labels: set) -> None:
        """ Assigns one or multiple labels to the *fileMaster* entry.
            Removes duplicates and adds new labels to the label list
            if necessary."""
        if not labels:
            return None

        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            # Make sure all labels are in the database table.
            # -> If they already exist or are malformed, the command
            # will be ignored by the DBMS.
            self.define_new_label(label)

        # Get all label-ids
        id_list = self.get_label_ids(label_set)

        # Check whether some labels are already associated
        # with the fileMaster entry.
        query = """
            SELECT labelID
            FROM labelToMaster
            WHERE urlHash = SHA2(:url, 256)
        """
        result = self.session.execute(text(query), {"url": str(url)})
        ids_found = result.fetchall()
        ids_associated = set()
        if ids_found:
            ids_associated = set(ids_found)

        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels
            # Add those associations
            for label_id in remaining_ids:
                insert_query = """
                    INSERT IGNORE INTO labelToMaster (labelID, urlHash)
                    VALUES (:label_id, SHA2(:url, 256))
                """
                self.session.execute(text(insert_query), {"label_id": label_id, "url": str(url)})
            self.session.commit()
        return None

    def assign_labels_to_uuid(self,
                              uuid_string: str,
                              labels: set) -> None:
        """Assigns one or multiple labels to a specific version of a file.
            Removes duplicates and adds new labels if necessary."""
        if not labels:
            return

        # Using a set to avoid duplicates. However, users might provide
        # a string or a list type.
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            # Make sure all labels are in the database table.
            # -> If they already exist or are malformed, the command
            # will be ignored by the DBMS.
            self.define_new_label(label)

        # Get all label-ids
        id_list = self.get_label_ids(label_set)

        # Check if there are already labels assigned with the version
        query = """
            SELECT labelID
            FROM labelToVersion
            WHERE versionUUID = :uuid
        """
        result = self.session.execute(text(query), {"uuid": uuid_string})
        ids_found = result.fetchall()
        ids_associated = set(ids_found) if ids_found else set()
        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels
            for label_id in remaining_ids:
                insert_query = """
                    INSERT IGNORE INTO labelToVersion (labelID, versionUUID)
                    VALUES (:label_id, :uuid)
                """
                self.session.execute(text(insert_query), {"label_id": label_id, "uuid": uuid_string})
            self.session.commit()

    # #########################################################################
    # QUERY LABELS
    # #########################################################################

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with a specific
           version identified by its UUID."""
        query = "SELECT get_filemaster_id(:uuid)"
        result = self.session.execute(text(query), {"uuid": version_uuid})
        filemaster_id = result.fetchone()
        if not filemaster_id:
            raise ValueError("Invalid filemaster ID")
        return str(filemaster_id[0])

    def filemaster_labels_by_url(self,
                                 url: Union[exo_url.ExoUrl, str]) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           filemaster entry using the URL associated."""
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        result = self.db_connection.call_procedure('labels_filemaster_by_url_SP', (str(url),))
        labels = result.fetchall()
        return {(label[0]) for label in labels} if labels else set()

    def version_labels_by_uuid(self,
                               version_uuid: str) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           version of a file. Does not include labels attached to the
           filemaster entry."""
        result = self.db_connection.call_procedure('labels_version_by_id_SP', (version_uuid,))
        labels = result.fetchall()
        return {(label[0]) for label in labels} if labels else set()

    def all_labels_by_uuid(self,
                           version_uuid: str) -> set:
        """Get a set of ALL label names (not id numbers!) attached
           to a specific version of a file AND its filemaster entry."""
        version_labels = self.version_labels_by_uuid(version_uuid)
        filemaster_id = self.get_filemaster_id(version_uuid)
        query = "SELECT url FROM fileMaster WHERE id = :id"
        result = self.session.execute(text(query), {"id": filemaster_id})
        filemaster_url = result.fetchone()
        filemaster_labels = set()
        if filemaster_url:
            filemaster_labels = self.filemaster_labels_by_url(filemaster_url[0])
        joined_set = version_labels | filemaster_labels
        return joined_set

    def get_label_ids(self,
                      label_set: Union[set, str]) -> set:
        """ Given a set of labels, this returns the corresponding ids
            in the labels table. """
        if not label_set:
            logger.error('No labels provided to get_label_ids().')
            return set()

        label_set = userprovided.parameters.convert_to_set(label_set)
        # Use SQLAlchemy's bindparam for safe IN clause handling
        # Convert the label set to a tuple for proper parameter binding
        label_tuple = tuple(label_set)
        query = """
            SELECT id
            FROM labels
            WHERE shortName IN :labels
        """
        result = self.session.execute(text(query), {"labels": label_tuple})
        label_id = result.fetchall()
        return {(id[0]) for id in label_id} if label_id else set()

    def version_uuids_by_label(self,
                               single_label: str,
                               processed_only: bool = False) -> set:
        """Get a list of UUIDs (in this context file versions) which have
            *one* specific label attached to them.
            If processed_only is set to True only UUIDs of already processed
            tasks are returned. Otherwise it contains queue objects with that
            label."""
        returned_set = self.get_label_ids(single_label)
        if returned_set == set():
            raise ValueError('Unknown label. Check for typo.')

        label_id: str = returned_set.pop()
        if processed_only:
            query = """
                SELECT versionUUID
                FROM labelToVersion AS lv
                WHERE labelID = :label_id AND
                EXISTS (
                    SELECT fv.id FROM fileVersions AS fv
                    WHERE fv.id = lv.versionUUID
                )
            """
        else:
            query = """
                SELECT versionUUID
                FROM labelToVersion
                WHERE labelID = :label_id
            """
        result = self.session.execute(text(query), {"label_id": label_id})
        version_ids = result.fetchall()
        return {(uuid[0]) for uuid in version_ids} if version_ids else set()

    # #########################################################################
    # REMOVING LABELS
    # #########################################################################

    def remove_labels_from_uuid(self,
                                uuid: str,
                                labels_to_remove: set) -> None:
        "Detaches a label / a set of labels from a UUID / version."

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        labels_to_remove = userprovided.parameters.convert_to_set(
            labels_to_remove)

        # Get all label-ids
        id_list = self.get_label_ids(labels_to_remove)

        for label_id in id_list:
            self.db_connection.call_procedure('remove_labels_from_uuid_SP', (label_id, uuid))
