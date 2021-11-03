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

import userprovided
import pymysql

from exoskeleton import database_connection
from exoskeleton import exo_url


class LabelManager:
    "Manage labels and their association"
    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection) -> None:
        self.db_connection = db_connection
        self.cur: pymysql.cursors.Cursor = self.db_connection.get_cursor()

    # #########################################################################
    # CREATING LABELS
    # #########################################################################

    @staticmethod
    def __shortname_ok(shortname: str) -> bool:
        "Check if the label's shortname does not exceed 31 characters."
        if len(shortname) > 31:
            logging.error(
                "Cannot add labelname: exceeding max length of 31 characters.")
            return False
        return True

    def define_new_label(self,
                         shortname: str,
                         description: str = None) -> None:
        """If the label is not already used, define a new label and description.
           In case the label already exists, do not update the description."""
        if not self.__shortname_ok(shortname):
            return
        try:
            self.cur.execute('INSERT INTO labels (shortName, description) ' +
                             'VALUES (%s, %s);',
                             (shortname, description))
            logging.debug('Added label to the database.')
        except pymysql.err.IntegrityError:
            logging.debug('Label already existed.')

    def define_or_update_label(self,
                               shortname: str,
                               description: str = None) -> None:
        """ Insert a new label into the database or update its description
            in case it already exists.
            Use __define_new_label if an update has to be avoided. """
        if not self.__shortname_ok(shortname):
            return
        self.cur.callproc('label_define_or_update_SP',
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
        self.cur.execute('SELECT labelID ' +
                         'FROM labelToMaster ' +
                         'WHERE urlHash = SHA2(%s,256);', (url, ))
        ids_found: Optional[tuple] = self.cur.fetchall()
        ids_associated = set()
        if ids_found:
            ids_associated = set(ids_found)

        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels
            # Convert into a format to INSERT with executemany
            insert_list = [(id, url) for id in remaining_ids]
            # Add those associatons
            self.cur.executemany('INSERT IGNORE INTO labelToMaster ' +
                                 '(labelID, urlHash) ' +
                                 'VALUES (%s, SHA2(%s,256));',
                                 insert_list)
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
        self.cur.execute('SELECT labelID ' +
                         'FROM labelToVersion ' +
                         'WHERE versionUUID = %s;', (uuid_string, ))
        ids_found = self.cur.fetchall()
        ids_associated = set(ids_found) if ids_found else set()
        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels
            # Convert into a format to INSERT with executemany
            insert_list = [(id, uuid_string) for id in remaining_ids]
            self.cur.executemany('INSERT IGNORE INTO labelToVersion ' +
                                 '(labelID, versionUUID) ' +
                                 'VALUES (%s, %s);', insert_list)

    # #########################################################################
    # QUERY LABELS
    # #########################################################################

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with a specific
           version identified by its UUID."""
        self.cur.execute('SELECT get_filemaster_id(%s);', (version_uuid, ))
        filemaster_id = self.cur.fetchone()
        if not filemaster_id:
            raise ValueError("Invalid filemaster ID")
        return filemaster_id[0]  # type: ignore[index]

    def filemaster_labels_by_url(self,
                                 url: Union[exo_url.ExoUrl, str]) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           filemaster entry using the URL associated."""
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        self.cur.callproc('labels_filemaster_by_url_SP', (url, ))
        labels = self.cur.fetchall()
        return {(label[0]) for label in labels} if labels else set()  # type: ignore[index]

    def version_labels_by_uuid(self,
                               version_uuid: str) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           version of a file. Does not include labels attached to the
           filemaster entry."""
        self.cur.callproc('labels_version_by_id_SP', (version_uuid, ))
        labels = self.cur.fetchall()
        return {(label[0]) for label in labels} if labels else set()  # type: ignore[index]

    def all_labels_by_uuid(self,
                           version_uuid: str) -> set:
        """Get a set of ALL label names (not id numbers!) attached
           to a specific version of a file AND its filemaster entry."""
        version_labels = self.version_labels_by_uuid(version_uuid)
        filemaster_id = self.get_filemaster_id(version_uuid)
        self.cur.execute('SELECT url FROM fileMaster WHERE id = %s;',
                         (filemaster_id, ))
        filemaster_url = self.cur.fetchone()
        filemaster_labels = set()
        if filemaster_url:
            filemaster_labels = self.filemaster_labels_by_url(filemaster_url[0])  # type: ignore
        joined_set = version_labels | filemaster_labels
        return joined_set

    def get_label_ids(self,
                      label_set: Union[set, str]) -> set:
        """ Given a set of labels, this returns the corresponding ids
            in the labels table. """
        if not label_set:
            logging.error('No labels provided to get_label_ids().')
            return set()

        label_set = userprovided.parameters.convert_to_set(label_set)
        # The IN-Operator makes it necessary to construct the command
        # every time, so input gets escaped. See the accepted answer here:
        # https://stackoverflow.com/questions/14245396/using-a-where-in-statement
        query = ("SELECT id " +
                 "FROM labels " +
                 "WHERE shortName " +
                 "IN ({0});".format(', '.join(['%s'] * len(label_set))))
        self.cur.execute(query, tuple(label_set))
        label_id = self.cur.fetchall()
        return {(id[0]) for id in label_id} if label_id else set()  # type: ignore[index]

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
            self.cur.execute("SELECT versionUUID " +
                             "FROM labelToVersion AS lv " +
                             "WHERE labelID = %s AND " +
                             "EXISTS ( " +
                             "    SELECT fv.id FROM fileVersions AS fv " +
                             "    WHERE fv.id = lv.versionUUID);",
                             (label_id, ))
        else:
            self.cur.execute("SELECT versionUUID " +
                             "FROM labelToVersion " +
                             "WHERE labelID = %s;",
                             (label_id, ))
        version_ids = self.cur.fetchall()
        return {(uuid[0]) for uuid in version_ids} if version_ids else set()  # type: ignore[index, assignment]

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
            self.cur.callproc('remove_labels_from_uuid_SP', (label_id, uuid))
