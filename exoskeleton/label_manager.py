"""
This manages labels for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

import logging
from typing import Optional, Union

from sqlalchemy import select
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
        "Check if the label's shortname does not exceed 63 characters."
        if len(shortname) > 63:
            logger.error(
                "Cannot add labelname: exceeding max length of 63 characters.")
            return False
        return True

    def define_new_label(self,
                         shortname: str,
                         description: Optional[str] = None) -> None:
        """If the label is not already used, define a new label and description.
           In case the label already exists, do not update the description."""
        shortname = userprovided.parameters.clean_trim(shortname) or ''
        if not shortname or not self.__shortname_ok(shortname):
            return
        # Pre-check existence to avoid IntegrityError+rollback, which would
        # corrupt the outer transaction's flushed-but-uncommitted objects.
        existing = self.session.query(models.Label).filter(
            models.Label.shortName == shortname
        ).first()
        if existing:
            logger.debug('Label already existed.')
            return
        new_label = models.Label(shortName=shortname, description=description)
        self.session.add(new_label)
        self.session.commit()
        logger.debug('Added label to the database.')

    def define_or_update_label(self,
                               shortname: str,
                               description: Optional[str] = None) -> None:
        """ Insert a new label into the database or update its description
            in case it already exists.
            Use __define_new_label if an update has to be avoided. """
        shortname = userprovided.parameters.clean_trim(shortname) or ''
        if not shortname or not self.__shortname_ok(shortname):
            return

        # Query for existing label by shortName
        existing_label = self.session.query(models.Label).filter(
            models.Label.shortName == shortname
        ).first()

        if existing_label:
            # Update existing label's description
            existing_label.description = description  # type: ignore[assignment]
        else:
            # Create new label
            new_label = models.Label(
                shortName=shortname,
                description=description
            )
            self.session.add(new_label)

        self.session.commit()

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

        # Check whether some labels are already associated with the fileMaster entry
        existing_associations = self.session.query(models.LabelToMaster.labelID).filter(
            models.LabelToMaster.urlHash == url.hash
        ).all()

        # Extract the IDs from the result (each row is a tuple with one element)
        ids_associated = (
            {assoc[0] for assoc in existing_associations}
            if existing_associations else set()
        )

        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels - add those associations using ORM
            for label_id in remaining_ids:
                new_association = models.LabelToMaster(
                    labelID=label_id,
                    urlHash=url.hash
                )
                self.session.add(new_association)

            try:
                self.session.commit()
            except IntegrityError:
                # This shouldn't happen since we filter existing associations,
                # but handle gracefully in case of race conditions
                self.session.rollback()
                logger.debug('Some label associations already existed.')
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

        # Check if there are already labels assigned with the version using ORM
        existing_associations = self.session.query(models.LabelToVersion.labelID).filter(
            models.LabelToVersion.versionUUID == uuid_string
        ).all()

        # Extract the IDs from the result (each row is a tuple with one element)
        ids_associated = (
            {assoc[0] for assoc in existing_associations}
            if existing_associations else set()
        )

        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels - add those associations using ORM
            for label_id in remaining_ids:
                new_association = models.LabelToVersion(
                    labelID=label_id,
                    versionUUID=uuid_string
                )
                self.session.add(new_association)

            try:
                self.session.commit()
            except IntegrityError:
                # Handle gracefully in case of race conditions
                self.session.rollback()
                logger.debug('Some label-to-version associations already existed.')

    # #########################################################################
    # QUERY LABELS
    # #########################################################################

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with a specific
           version identified by its UUID."""
        file_version = self.session.query(models.FileVersion.fileMasterID).filter(
            models.FileVersion.id == version_uuid
        ).first()

        if not file_version or file_version[0] is None:
            raise ValueError("Invalid filemaster ID")
        return str(file_version[0])

    def filemaster_labels_by_url(self,
                                 url: Union[exo_url.ExoUrl, str]) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           filemaster entry using the URL associated."""
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)

        labels = self.session.query(models.Label.shortName).join(
            models.LabelToMaster,
            models.Label.id == models.LabelToMaster.labelID
        ).filter(
            models.LabelToMaster.urlHash == url.hash
        ).distinct().all()

        return {label[0] for label in labels} if labels else set()

    def version_labels_by_uuid(self,
                               version_uuid: str) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           version of a file. Does not include labels attached to the
           filemaster entry."""

        labels = self.session.query(models.Label.shortName).join(
            models.LabelToVersion,
            models.Label.id == models.LabelToVersion.labelID
        ).filter(
            models.LabelToVersion.versionUUID == version_uuid
        ).distinct().all()

        return {label[0] for label in labels} if labels else set()

    def all_labels_by_uuid(self,
                           version_uuid: str) -> set:
        """Get a set of ALL label names (not id numbers!) attached
           to a specific version of a file AND its filemaster entry."""
        version_labels = self.version_labels_by_uuid(version_uuid)
        filemaster_id = self.get_filemaster_id(version_uuid)

        file_master = self.session.query(models.FileMaster.url).filter(
            models.FileMaster.id == filemaster_id
        ).first()

        filemaster_labels = set()
        if file_master:
            filemaster_labels = self.filemaster_labels_by_url(file_master[0])
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
        labels = self.session.query(models.Label).filter(
            models.Label.shortName.in_(label_set)
        ).all()

        return {label.id for label in labels} if labels else set()

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
            # Only return UUIDs that exist in fileVersions AND are no longer
            # in the queue (i.e. the task was actually processed, not just a stub)
            queue_ids = select(models.Queue.id)
            query = self.session.query(models.LabelToVersion.versionUUID).join(
                models.FileVersion,
                models.FileVersion.id == models.LabelToVersion.versionUUID
            ).filter(
                models.LabelToVersion.labelID == label_id,
                ~models.LabelToVersion.versionUUID.in_(queue_ids)
            )
        else:
            # Simple query - return all UUIDs with this label
            query = self.session.query(models.LabelToVersion.versionUUID).filter(
                models.LabelToVersion.labelID == label_id
            )

        version_ids = query.all()
        return {uuid[0] for uuid in version_ids} if version_ids else set()

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
            self.session.query(models.LabelToVersion).filter(
                models.LabelToVersion.labelID == label_id,
                models.LabelToVersion.versionUUID == uuid
            ).delete()

        self.session.commit()
