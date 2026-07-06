"""
This manages labels for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import userprovided

from exoskeleton import database_connection
from exoskeleton import exo_url
from exoskeleton import models

logger = logging.getLogger(__name__)


class LabelManager:
    """Manage labels and their association.

       All public methods open a short-lived session per operation.
       Methods that are part of larger transactions (like adding a task
       to the queue) accept an optional session parameter: if provided,
       they operate on that session and leave the commit to the caller."""

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection) -> None:
        self.db_connection = db_connection

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
                         description: str | None = None,
                         session: Session | None = None) -> None:
        """If the label is not already used, define a new label and description.
           In case the label already exists, do not update the description."""
        shortname = userprovided.parameters.clean_trim(shortname) or ''
        if not shortname or not self.__shortname_ok(shortname):
            return
        if session is not None:
            self._define_new_label(session, shortname, description)
        else:
            with self.db_connection.session_scope() as new_session:
                self._define_new_label(new_session, shortname, description)

    @staticmethod
    def _define_new_label(session: Session,
                          shortname: str,
                          description: str | None) -> None:
        "Add a label within the given session (commit is up to the caller)."
        # Pre-check existence to avoid IntegrityError+rollback, which would
        # corrupt the outer transaction's flushed-but-uncommitted objects.
        existing = session.query(models.Label).filter(
            models.Label.shortName == shortname
        ).first()
        if existing:
            logger.debug('Label already existed.')
            return
        new_label = models.Label(shortName=shortname, description=description)
        session.add(new_label)
        session.flush()
        logger.debug('Added label to the database.')

    def define_or_update_label(self,
                               shortname: str,
                               description: str | None = None) -> None:
        """ Insert a new label into the database or update its description
            in case it already exists.
            Use define_new_label if an update has to be avoided. """
        shortname = userprovided.parameters.clean_trim(shortname) or ''
        if not shortname or not self.__shortname_ok(shortname):
            return

        with self.db_connection.session_scope() as session:
            existing_label = session.query(models.Label).filter(
                models.Label.shortName == shortname
            ).first()

            if existing_label:
                existing_label.description = description  # type: ignore[assignment]
            else:
                session.add(models.Label(
                    shortName=shortname,
                    description=description))

    # #########################################################################
    # ASSIGNING LABELS
    # #########################################################################

    def assign_labels_to_master(self,
                                url: exo_url.ExoUrl | str,
                                labels: set,
                                session: Session | None = None) -> None:
        """ Assigns one or multiple labels to the *fileMaster* entry.
            Removes duplicates and adds new labels to the label list
            if necessary."""
        if not labels:
            return None

        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)

        if session is not None:
            self._assign_labels_to_master(session, url, labels)
            return None

        try:
            with self.db_connection.session_scope() as new_session:
                self._assign_labels_to_master(new_session, url, labels)
        except IntegrityError:
            # Extremely unlikely as associations are pre-filtered: only a
            # concurrent writer could cause this.
            logger.debug('Some label associations already existed.')
        return None

    def _assign_labels_to_master(self,
                                 session: Session,
                                 url: exo_url.ExoUrl,
                                 labels: set) -> None:
        "Assign master labels within the given session."
        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            # Make sure all labels are in the database table.
            self._define_new_label(session, label, None)

        # Get all label-ids
        id_list = self._get_label_ids(session, label_set)

        # Check whether some labels are already associated with the fileMaster entry
        existing_associations = session.query(models.LabelToMaster.labelID).filter(
            models.LabelToMaster.urlHash == url.hash
        ).all()
        ids_associated = {assoc[0] for assoc in existing_associations}

        # ignore all labels already associated:
        for label_id in (id_list - ids_associated):
            session.add(models.LabelToMaster(
                labelID=label_id,
                urlHash=url.hash))
        session.flush()

    def assign_labels_to_uuid(self,
                              uuid_string: str,
                              labels: set,
                              session: Session | None = None) -> None:
        """Assigns one or multiple labels to a specific version of a file.
            Removes duplicates and adds new labels if necessary."""
        if not labels:
            return

        if session is not None:
            self._assign_labels_to_uuid(session, uuid_string, labels)
            return

        try:
            with self.db_connection.session_scope() as new_session:
                self._assign_labels_to_uuid(new_session, uuid_string, labels)
        except IntegrityError:
            logger.debug('Some label-to-version associations already existed.')

    def _assign_labels_to_uuid(self,
                               session: Session,
                               uuid_string: str,
                               labels: set) -> None:
        "Assign version labels within the given session."
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            self._define_new_label(session, label, None)

        id_list = self._get_label_ids(session, label_set)

        # Check if there are already labels assigned to the version
        existing_associations = session.query(models.LabelToVersion.labelID).filter(
            models.LabelToVersion.versionUUID == uuid_string
        ).all()
        ids_associated = {assoc[0] for assoc in existing_associations}

        for label_id in (id_list - ids_associated):
            session.add(models.LabelToVersion(
                labelID=label_id,
                versionUUID=uuid_string))
        session.flush()

    # #########################################################################
    # QUERY LABELS
    # #########################################################################

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with a specific
           version identified by its UUID."""
        with self.db_connection.session_scope() as session:
            file_version = session.query(models.FileVersion.fileMasterID).filter(
                models.FileVersion.id == version_uuid
            ).first()

            if not file_version or file_version[0] is None:
                raise ValueError("Invalid filemaster ID")
            return str(file_version[0])

    def filemaster_labels_by_url(self,
                                 url: exo_url.ExoUrl | str) -> set:
        """Get a list of label names (not id numbers!) attached to a specific
           filemaster entry using the URL associated."""
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)

        with self.db_connection.session_scope() as session:
            labels = session.query(models.Label.shortName).join(
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
        with self.db_connection.session_scope() as session:
            labels = session.query(models.Label.shortName).join(
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

        with self.db_connection.session_scope() as session:
            file_master = session.query(models.FileMaster.url).filter(
                models.FileMaster.id == filemaster_id
            ).first()

        filemaster_labels = set()
        if file_master:
            filemaster_labels = self.filemaster_labels_by_url(file_master[0])
        joined_set = version_labels | filemaster_labels
        return joined_set

    def get_label_ids(self,
                      label_set: set | str,
                      session: Session | None = None) -> set:
        """ Given a set of labels, this returns the corresponding ids
            in the labels table. """
        if not label_set:
            logger.error('No labels provided to get_label_ids().')
            return set()

        label_set = userprovided.parameters.convert_to_set(label_set)
        if session is not None:
            return self._get_label_ids(session, label_set)
        with self.db_connection.session_scope() as new_session:
            return self._get_label_ids(new_session, label_set)

    @staticmethod
    def _get_label_ids(session: Session,
                       label_set: set) -> set:
        "Look up label ids within the given session."
        labels = session.query(models.Label).filter(
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

        with self.db_connection.session_scope() as session:
            if processed_only:
                # Only return UUIDs that exist in fileVersions AND are no longer
                # in the queue (i.e. the task was actually processed, not just a stub)
                queue_ids = select(models.Queue.id)
                query = session.query(models.LabelToVersion.versionUUID).join(
                    models.FileVersion,
                    models.FileVersion.id == models.LabelToVersion.versionUUID
                ).filter(
                    models.LabelToVersion.labelID == label_id,
                    ~models.LabelToVersion.versionUUID.in_(queue_ids)
                )
            else:
                # Simple query - return all UUIDs with this label
                query = session.query(models.LabelToVersion.versionUUID).filter(
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

        with self.db_connection.session_scope() as session:
            # Get all label-ids
            id_list = self._get_label_ids(session, labels_to_remove)

            for label_id in id_list:
                session.query(models.LabelToVersion).filter(
                    models.LabelToVersion.labelID == label_id,
                    models.LabelToVersion.versionUUID == uuid
                ).delete()
