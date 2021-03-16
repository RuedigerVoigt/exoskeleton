#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
A Python framework to build a basic crawler / scraper with a MariaDB backend.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""

# python standard library:
from collections import Counter
# noinspection PyUnresolvedReferences
from collections import defaultdict  # noqa # pylint: disable=unused-import
import logging
from typing import Union, Optional

# Sister projects:
import compatibility
import userprovided

# import other modules of this framework
from exoskeleton import _version as version
from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import error_manager
from exoskeleton import file_manager
from exoskeleton import job_manager
from exoskeleton import notification_manager
from exoskeleton import queue_manager
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager


class Exoskeleton:
    """ Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint to complain a lot.
    # As the complexity is needed, disable some warnings:
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-public-methods
    # pylint: disable=too-many-branches

    def __init__(self,
                 database_settings: dict,
                 target_directory: str,
                 filename_prefix: str = '',
                 project_name: str = 'Bot',
                 bot_user_agent: str = 'Bot',
                 bot_behavior: Union[dict, None] = None,
                 mail_settings: Union[dict, None] = None,
                 mail_behavior: Union[dict, None] = None,
                 chrome_name: str = ''):
        "Sets defaults"

        compatibility.Check(
            package_name='exoskeleton',
            package_version=version.__version__,
            release_date=version.release_date,
            python_version_support={
                'min_version': '3.6',
                'incompatible_versions': [],
                'max_tested_version': '3.9'},
            nag_over_update={
                    'nag_days_after_release': 120,
                    'nag_in_hundred': 100},
            language_messages='en'
        )

        self.project: str = project_name.strip()
        self.user_agent: str = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # Init database Connection
        self.db = database_connection.DatabaseConnection(database_settings)
        self.cur = self.db.get_cursor()

        self.stats = statistics_manager.StatisticsManager(self.db)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Mail / Notification Setup
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        mail_settings = dict() if not mail_settings else mail_settings
        mail_behavior = dict() if not mail_behavior else mail_behavior

        self.milestone: Optional[int] = mail_behavior.get('milestone_num',
                                                          None)
        if self.milestone:
            if not isinstance(self.milestone, int):
                raise ValueError('milestone_num must be integer!')

        self.notify = notification_manager.NotificationManager(
            self.project, mail_settings, mail_behavior)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Bot Behavior
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        if bot_behavior:
            userprovided.parameters.validate_dict_keys(
                dict_to_check=bot_behavior,
                allowed_keys={'connection_timeout',
                              'queue_max_retries',
                              'queue_revisit',
                              'rate_limit_wait',
                              'stop_if_queue_empty',
                              'wait_min',
                              'wait_max'},
                necessary_keys=None,
                dict_name='bot_behavior')
        else:
            bot_behavior = dict()

        # Seconds until a connection times out:
        self.connection_timeout: int = userprovided.parameters.int_in_range(
            "self.connection_timeout",
            bot_behavior.get('connection_timeout', 60),
            1, 60, 50)

        # Init Classes

        self.time = time_manager.TimeManager(
            bot_behavior.get('wait_min', 5),
            bot_behavior.get('wait_max', 30))

        self.file = file_manager.FileManager(
            self.db,
            target_directory,
            filename_prefix)

        self.errorhandling = error_manager.CrawlingErrorManager(
            self.db,
            bot_behavior.get('queue_max_retries', 3),
            bot_behavior.get('rate_limit_wait', 1860)
            )

        self.controlled_browser = remote_control_chrome.RemoteControlChrome(
            chrome_name,
            self.errorhandling,
            self.stats)

        self.action = actions.ExoActions(
            self.db,
            self.stats,
            self.file,
            self.time,
            self.errorhandling,
            self.controlled_browser,
            self.user_agent,
            self.connection_timeout)

        self.queue = queue_manager.QueueManager(
            self.db,
            self.time,
            self.stats,
            self.action,
            self.notify,
            bot_behavior,
            self.milestone  # type: ignore[arg-type]
            )

        self.jobs = job_manager.JobManager(self.cur)

        # Create other objects
        self.cnt: Counter = Counter()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ACTIONS
    #
    # - Make some accessible from outside.
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def random_wait(self) -> None:
        """Wait for a random time within the limits set to init TimeManager."""
        self.time.random_wait()

    def process_queue(self) -> None:
        """Process the queue"""
        self.queue.process_queue()

    def return_page_code(self,
                         url: str) -> str:
        """Immediately return a page's code.
           Do *not* store it in the database."""
        return self.action.return_page_code(url)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # JOB MANAGEMENT
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def job_define_new(self,
                       job_name: str,
                       start_url: str) -> None:
        """ Create a new crawl job identified by it name and an url
        to start crawling. """
        self.jobs.define_new(job_name, start_url)

    def job_update_current_url(self,
                               job_name: str,
                               current_url: str) -> None:
        "Set the currentUrl for a specific job."
        self.jobs.update_current_url(job_name, current_url)

    def job_get_current_url(self,
                            job_name: str) -> str:
        """ Returns the current URl for this job. If none is stored, this
        returns the start URL. Raises exception if the job is already
        finished."""
        return self.jobs.get_current_url(job_name)

    def job_mark_as_finished(self,
                             job_name: str) -> None:
        "Mark a crawl job as finished."
        self.jobs.mark_as_finished(job_name)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # QUEUE MANAGEMENT
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_file_download(self,
                          url: str,
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False) -> Optional[str]:
        "Add a file download URL to the queue"
        uuid = self.queue.add_to_queue(url, 1, labels_master,
                                       labels_version, False,
                                       force_new_version)
        return uuid

    def add_save_page_code(self,
                           url: str,
                           labels_master: set = None,
                           labels_version: set = None,
                           prettify_html: bool = False,
                           force_new_version: bool = False) -> Optional[str]:
        """Add an URL to the queue to save its HTML code into the database."""
        uuid = self.queue.add_to_queue(url, 2, labels_master,
                                       labels_version, prettify_html,
                                       force_new_version)
        return uuid

    def add_page_to_pdf(self,
                        url: str,
                        labels_master: set = None,
                        labels_version: set = None,
                        force_new_version: bool = False) -> Optional[str]:
        """Add an URL to the queue to print it to PDF
        with headless Chrome. """
        if not self.controlled_browser.browser_present:
            logging.warning('Will add this task to the queue, but without ' +
                            'Chrome or Chromium this task cannot run!' +
                            'Provide the path to the executable when you ' +
                            'initialize exoskeleton.')
        uuid = self.queue.add_to_queue(url, 3, labels_master,
                                       labels_version, False,
                                       force_new_version)
        return uuid

    def add_save_page_text(self,
                           url: str,
                           labels_master: set = None,
                           labels_version: set = None,
                           force_new_version: bool = False) -> Optional[str]:
        """Add the task 'Extract the text (not the code) from a HTML page and
           store it into the database' to the queue.
           This can be useful for some language processing tasks, but compared
           to add_save_page_code this removes the possiblity to work on a
           specific part using a CSS selector."""
        uuid = self.queue.add_to_queue(url, 4, labels_master,
                                       labels_version, True,
                                       force_new_version)
        return uuid

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        """Get the id of the filemaster entry associated with this URL."""
        return self.queue.get_filemaster_id_by_url(url)

    def delete_from_queue(self,
                          queue_id: str) -> None:
        """Remove all label links from a queue item and then delete it
        from the queue."""
        self.queue.delete_from_queue(queue_id)

    def forget_all_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause any type of
        error, as if they are new tasks by removing that mark and
        any delay."""
        self.errorhandling.forget_all_errors()

    def forget_permanent_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *permanent*
        error, as if they are new tasks by removing that mark and
        any delay."""
        self.errorhandling.forget_error_group(True)

    def forget_temporary_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *temporary*
        error, as if they are new tasks by removing that mark and any delay."""
        self.errorhandling.forget_error_group(False)

    def forget_specific_error(self,
                              specific_error: int) -> None:
        """Treat all queued tasks, that are marked to cause a *specific*
        error, as if they are new tasks by removing that mark and
        any delay. The number of the error has to correspond to the
        errorType database table."""
        self.errorhandling.forget_specific_error(specific_error)

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None) -> None:
        """Add a specific fully qualified domain name (fqdn)
        - like www.example.com - to the blocklist."""
        self.queue.block_fqdn(fqdn, comment)

    def unblock_fqdn(self,
                     fqdn: str) -> None:
        """Remove a specific fqdn from the blocklist."""
        self.cur.execute('DELETE FROM blockList ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         (fqdn.strip(), ))

    def truncate_blocklist(self) -> None:
        """Remove *all* entries from the blocklist."""
        self.queue.truncate_blocklist()

    def prettify_html(self,
                      content: str) -> str:
        """Only use for HTML, not XML.
           Parse the HTML:
           * add a document structure if needed
           * Encode HTML-entities and the document as Unicode (UTF-8).
           * Empty elements are NOT removed as they might be used to find
             specific elements within the tree.
        """

        return self.action.prettify_html(content)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # LABELS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def assign_labels_to_master(self,
                                url: str,
                                labels: Union[set, None]) -> None:
        """ Assigns one or multiple labels to the *fileMaster* entry.
            Removes duplicates and adds new labels to the label list
            if necessary."""
        self.queue.assign_labels_to_master(url, labels)

    def assign_labels_to_uuid(self,
                              uuid: str,
                              labels: Union[set, None]) -> None:
        """ Assigns one or multiple labels either to a specific
            version of a file. Removes duplicates and adds new labels
            to the label list if necessary."""
        self.queue.assign_labels_to_uuid(uuid, labels)

    def get_label_ids(self,
                      label_set: Union[set, str]) -> set:
        """ Given a set of labels, this returns the corresponding ids
            in the labels table. """
        return self.queue.get_label_ids(label_set)

    def define_or_update_label(self,
                               shortname: str,
                               description: str = None) -> None:
        """ Insert a new label into the database or update its description
            in case it already exists. Use __define_new_label if an update
            has to be avoided. """
        if len(shortname) > 31:
            logging.error(
                "Labelname exceeds max length of 31 chars: cannot add it.")
            return

        self.cur.execute('INSERT INTO labels (shortName, description) ' +
                         'VALUES (%s, %s) ' +
                         'ON DUPLICATE KEY UPDATE description = %s;',
                         (shortname, description, description))

    def version_uuids_by_label(self,
                               single_label: str,
                               processed_only: bool = False) -> set:
        """Get a list of UUIDs (in this context file versions)
           which have *one* specific label attached to them.
           If processed_only is set to True only UUIDs of
           already downloaded items are returned.
           Otherwise it contains queue objects with that label."""
        returned_set = self.queue.get_label_ids(single_label)
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
        if not version_ids:
            return set()
        version_ids = {(uuid[0]) for uuid in version_ids}  # type: ignore[index, assignment]
        return version_ids  # type: ignore[return-value]

    def version_labels_by_uuid(self,
                               version_uuid: str) -> set:
        """Get a list of label names (not id numbers!) attached
           to a specific version of a file. Does not include
           labels attached to the filemaster entry."""
        self.cur.execute('SELECT DISTINCT shortName ' +
                         'FROM labels ' +
                         'WHERE ID IN (' +
                         '  SELECT labelID ' +
                         '  FROM labelToVersion ' +
                         '  WHERE versionUUID = %s' +
                         ');',
                         (version_uuid, ))
        labels = self.cur.fetchall()
        if not labels:
            return set()
        labels_set = {(label[0]) for label in labels}  # type: ignore[index]
        return labels_set

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with
           a specific version identified by its UUID."""
        self.cur.execute('SELECT fileMasterID ' +
                         'FROM exoskeleton.fileVersions ' +
                         'WHERE id = %s;',
                         (version_uuid, ))
        filemaster_id = self.cur.fetchone()
        if not filemaster_id:
            raise ValueError("Invalid filemaster ID")
        return filemaster_id[0]  # type: ignore[index]

    def filemaster_labels_by_id(self,
                                filemaster_id: str) -> set:
        """Get a list of label names (not id numbers!) attached
           to a specific filemaster entry."""
        self.cur.execute('SELECT DISTINCT shortName ' +
                         'FROM labels ' +
                         'WHERE ID IN (' +
                         '  SELECT labelID ' +
                         '  FROM labelToMaster ' +
                         '  WHERE labelID = %s' +
                         ');',
                         (filemaster_id, ))
        labels = self.cur.fetchall()
        if not labels:
            return set()
        labels_set = {(label[0]) for label in labels}  # type: ignore[index]
        return labels_set

    def filemaster_labels_by_url(self,
                                 url: str) -> set:
        """Primary use for automatic test: Get a list of label names
           (not id numbers!) attached to a specific filemaster entry
           via its URL instead of the id. The reason for this: The
           association with the URl predates the filemaster entry / the id."""
        self.cur.execute('SELECT DISTINCT shortName ' +
                         'FROM labels ' +
                         'WHERE ID IN (' +
                         '  SELECT labelID ' +
                         '  FROM labelToMaster ' +
                         '  WHERE urlHash = SHA2(%s,256)' +
                         ');',
                         (url, ))
        labels = self.cur.fetchall()
        if not labels:
            return set()
        labels_set = {(label[0]) for label in labels}  # type: ignore[index]
        return labels_set

    def all_labels_by_uuid(self,
                           version_uuid: str) -> set:
        """Get a set of ALL label names (not id numbers!) attached
           to a specific version of a file AND its filemaster entry."""
        version_labels = self.version_labels_by_uuid(version_uuid)
        filemaster_id = self.get_filemaster_id(version_uuid)
        filemaster_labels = self.filemaster_labels_by_id(filemaster_id)
        joined_set = version_labels | filemaster_labels
        return joined_set

    def remove_labels_from_uuid(self,
                                uuid: str,
                                labels_to_remove: set) -> None:
        "Detaches a label from a UUID / version."

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        labels_to_remove = userprovided.parameters.convert_to_set(
            labels_to_remove)

        # Get all label-ids
        id_list = self.queue.get_label_ids(labels_to_remove)

        for label_id in id_list:
            self.cur.execute("DELETE FROM labelToVersion " +
                             "WHERE labelID = %s and versionUUID = %s;",
                             (label_id, uuid))
