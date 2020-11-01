#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
A Python framework to build a basic crawler / scraper with a MariaDB backend.

Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""

# python standard library:
from collections import Counter
# noinspection PyUnresolvedReferences
from collections import defaultdict
import logging
import time
from typing import Union, Optional
from urllib.parse import urlparse

# 3rd party libraries:
import pymysql
# Sister projects:
import userprovided

# import other modules of this framework
from .DatabaseConnection import DatabaseConnection
from .ExoActions import ExoActions
from .FileManager import FileManager
from .JobManager import JobManager
from .StatisticsManager import StatisticsManager
from .TimeManager import TimeManager
from .NotificationManager import NotificationManager
from .QueueManager import QueueManager
from .RemoteControlChrome import RemoteControlChrome


class Exoskeleton:
    """ Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint3 to complain a lot.
    # As the complexity is needed, disable some warnings:
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
        """Sets defaults"""

        logging.info('You are using exoskeleton 1.1.0 (October 29, 2020)')

        self.project: str = project_name.strip()
        self.user_agent: str = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # Init database Connection
        self.db = DatabaseConnection(database_settings)
        self.cur = self.db.get_cursor()

        self.stats = StatisticsManager(self.cur)

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

        self.notify = NotificationManager(
            self.project, mail_settings, mail_behavior)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Bot Behavior
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        known_behavior_keys = {'connection_timeout',
                               'queue_max_retries',
                               'queue_revisit',
                               'rate_limit_wait',
                               'stop_if_queue_empty',
                               'wait_min',
                               'wait_max'}
        if bot_behavior:
            userprovided.parameters.validate_dict_keys(
                bot_behavior,
                known_behavior_keys,
                None,
                'bot_behavior')
        else:
            bot_behavior = dict()

        # Seconds until a connection times out:
        self.connection_timeout: int = userprovided.parameters.int_in_range(
            "self.connection.timeout",
            bot_behavior.get('connection_timeout', 60),
            1, 60, 50)

        self.stop_if_queue_empty: bool = bot_behavior.get(
            'stop_if_queue_empty', False)
        if type(self.stop_if_queue_empty) != bool:
            raise ValueError('The value for "stop_if_queue_empty" ' +
                             'must be a boolean (True / False).')
        # Time to wait after the queue is empty to check for new elements:
        self.queue_revisit: int = bot_behavior.get('queue_revisit', 20)
        self.queue_revisit = userprovided.parameters.int_in_range(
            "queue_revisit", self.queue_revisit, 10, 50, 50)

        # Init Classes

        self.tm = TimeManager(bot_behavior.get('wait_min', 5),
                              bot_behavior.get('wait_max', 30))

        self.qm = QueueManager(self.cur, self.tm, bot_behavior)

        self.fm = FileManager(self.cur,
                              self.qm,
                              target_directory,
                              filename_prefix)

        self.action = ExoActions(self.cur,
                                 self.stats,
                                 self.fm,
                                 self.tm,
                                 self.qm,
                                 self.user_agent,
                                 self.connection_timeout)

        self.controlled_browser = RemoteControlChrome(chrome_name, self.qm)

        self.jobs = JobManager(self.cur)

        # Create other objects
        self.cnt: Counter = Counter()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ACTIONS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # make random_wait() accessible from outside
    def random_wait(self):
        self.tm.random_wait()

    def process_queue(self):
        """Process the queue"""
        self.stats.log_queue_stats()

        while True:
            try:
                next_in_queue = self.qm.get_next_task()
            except pymysql.err.OperationalError as e:
                if e.args[0] == 2013:  # errno
                    # this error is unusual. Give the db some time:
                    logging.error('Lost connection to database server. ' +
                                  'Trying to restore it in 10 seconds ...')
                    time.sleep(10)
                    try:
                        self.cur = self.db.get_cursor()
                        next_in_queue = self.qm.get_next_task()
                        logging.info('Succesfully restored connection ' +
                                     'to database server!')
                    except Exception:
                        logging.error('Could not reestablish database ' +
                                      'server connection!', exc_info=True)
                        self.notify.send_msg('abort_lost_db')
                        raise ConnectionError('Lost database connection and ' +
                                              'could not restore it.')
                else:
                    logging.error('Unexpected Operational Error',
                                  exc_info=True)
                    raise

            if next_in_queue is None:
                # no actionable item in the queue
                if self.stop_if_queue_empty:
                    # Bot is configured to stop if queue is empty
                    # => check if that is only temporary or everything is done
                    self.cur.execute(
                        'SELECT num_items_with_temporary_errors();')
                    num_temp_errors = self.cur.fetchone()[0]
                    if num_temp_errors > 0:
                        # there are still tasks, but they have to wait
                        logging.debug("Tasks with temporary errors: " +
                                      "waiting %s seconds until next try.",
                                      self.queue_revisit)
                        time.sleep(self.queue_revisit)
                        continue
                    else:
                        # Nothing left
                        logging.info('Queue empty. Bot stops as configured.')

                        self.cur.execute(
                            'SELECT num_items_with_permanent_error();')
                        num_permanent_errors = self.cur.fetchone()[0]
                        if num_permanent_errors > 0:
                            logging.error("%s permanent errors!",
                                          num_permanent_errors)
                        self.notify.send_finish_msg(num_permanent_errors)
                    break
                else:
                    logging.debug("No actionable task: waiting %s seconds " +
                                  "until next check", self.queue_revisit)
                    time.sleep(self.queue_revisit)
                    continue
            else:
                # got a task from the queue
                queue_id = next_in_queue[0]
                action = next_in_queue[1]
                url = next_in_queue[2]
                url_hash = next_in_queue[3]
                prettify_html = True if next_in_queue[4] == 1 else False

                # The FQDN might have been added to the blocklist *after*
                # the task entered into the queue:
                if self.qm.check_blocklist(urlparse(url).hostname):
                    logging.error('Cannot process queue item as the FQDN ' +
                                  'has meanwhile been added to the blocklist!')
                    self.qm.delete_from_queue(queue_id)
                    logging.info('Removed item from queue: FQDN on blocklist.')
                else:
                    if action == 1:
                        # download file to disk
                        self.action.get_object(queue_id, 'file', url, url_hash)
                    elif action == 2:
                        # save page code into database
                        self.action.get_object(queue_id, 'content',
                                               url, url_hash,
                                               prettify_html)
                    elif action == 3:
                        # headless Chrome to create PDF
                        self.__page_to_pdf(url, url_hash, queue_id)
                    elif action == 4:
                        # save page text into database
                        self.action.get_object(queue_id, 'text',
                                               url, url_hash,
                                               prettify_html)
                    else:
                        logging.error('Unknown action id!')

                    if self.milestone and self.check_is_milestone():
                        stats = self.stats.queue_stats()
                        remaining_tasks = (stats['tasks_without_error'] +
                                           stats['tasks_with_temp_errors'])
                        self.notify.send_milestone_msg(
                            self.stats.get_processed_counter(),
                            remaining_tasks,
                            self.tm.estimate_remaining_time(
                                self.stats.get_processed_counter(),
                                remaining_tasks)
                                )

                    # wait some interval to avoid overloading the server
                    self.tm.random_wait()

    def __page_to_pdf(self,
                      url: str,
                      url_hash: str,
                      queue_id: str):
        """ Uses the Google Chrome or Chromium browser in headless mode
        to print the page to PDF and stores that.
        BEWARE: Some cookie-popups blank out the page and all what is stored
        is the dialogue."""

        filename = f"{self.fm.file_prefix}{queue_id}.pdf"
        path = self.fm.target_dir.joinpath(filename)

        self.controlled_browser.page_to_pdf(url, path, queue_id)

        hash_value = self.fm.get_file_hash(path)

        try:
            self.cur.callproc('insert_file_SP',
                              (url, url_hash, queue_id, 'application/pdf',
                               str(self.fm.target_dir), filename,
                               self.fm.get_file_size(path),
                               self.fm.hash_method, hash_value, 3))
        except pymysql.DatabaseError:
            self.cnt['transaction_fail'] += 1
            logging.error('Database Transaction failed: Could not add ' +
                          'already downloaded file %s to the database!',
                          path, exc_info=True)

    def return_page_code(self,
                         url: str):
        """Directly return a page's code. Do *not* store it in the database."""
        self.action.return_page_code(url)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # JOB MANAGEMENT
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def job_define_new(self,
                       job_name: str,
                       start_url: str):
        """ Create a new crawl job identified by it name and an url
        to start crawling. """
        self.jobs.define_new(job_name, start_url)

    def job_update_current_url(self,
                               job_name: str,
                               current_url: str):
        """ Set the currentUrl for a specific job. """
        self.jobs.update_current_url(job_name, current_url)

    def job_get_current_url(self,
                            job_name: str) -> str:
        """ Returns the current URl for this job. If none is stored, this
        returns the start URL. Raises exception if the job is already
        finished."""
        return self.jobs.get_current_url(job_name)

    def job_mark_as_finished(self,
                             job_name: str):
        """ Mark a crawl job as finished. """
        self.jobs.mark_as_finished(job_name)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # QUEUE MANAGEMENT
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_file_download(self,
                          url: str,
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False) -> Optional[str]:
        """Add a file download URL to the queue """
        uuid = self.qm.add_to_queue(url, 1, labels_master,
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
        uuid = self.qm.add_to_queue(url, 2, labels_master,
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
        uuid = self.qm.add_to_queue(url, 3, labels_master,
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
        uuid = self.qm.add_to_queue(url, 4, labels_master,
                                    labels_version, True,
                                    force_new_version)
        return uuid

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        """Get the id of the filemaster entry associated with this URL."""
        return self.qm.get_filemaster_id_by_url(url)

    def delete_from_queue(self,
                          queue_id: str):
        """Remove all label links from a queue item and then delete it
        from the queue."""
        self.qm.delete_from_queue(queue_id)

    def forget_all_errors(self):
        """Treat all queued tasks, that are marked to cause any type of
        error, as if they are new tasks by removing that mark and
        any delay."""
        self.qm.forget_all_errors()

    def forget_permanent_errors(self):
        """Treat all queued tasks, that are marked to cause a *permanent*
        error, as if they are new tasks by removing that mark and
        any delay."""
        self.qm.forget_error_group(True)

    def forget_temporary_errors(self):
        """Treat all queued tasks, that are marked to cause a *temporary*
        error, as if they are new tasks by removing that mark and any delay."""
        self.qm.forget_error_group(False)

    def forget_specific_error(self,
                              specific_error: int):
        """Treat all queued tasks, that are marked to cause a *specific*
        error, as if they are new tasks by removing that mark and
        any delay. The number of the error has to correspond to the
        errorType database table."""
        self.qm.forget_specific_error(specific_error)

    def check_is_milestone(self) -> bool:
        """ Check if a milestone is reached."""
        processed = self.stats.get_processed_counter()
        # Check >0 in case the bot starts failing with the first item.
        if (self.milestone and
                processed > 0 and
                (processed % self.milestone) == 0):
            logging.info("Milestone reached: %s processed", str(processed))
            return True
        return False

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None):
        """Add a specific fully qualified domain name (fqdn)
        - like www.example.com - to the blocklist."""
        self.qm.block_fqdn(fqdn, comment)

    def unblock_fqdn(self,
                     fqdn: str):
        """Remove a specific fqdn from the blocklist."""
        self.cur.execute('DELETE FROM blockList ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         fqdn.strip())

    def truncate_blocklist(self):
        """Remove *all* entries from the blocklist."""
        self.qm.truncate_blocklist()

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

    def define_or_update_label(self,
                               shortname: str,
                               description: str = None):
        """ Insert a new label into the database or update its
        description in case it already exists. Use __define_new_label
        if an update has to be avoided. """
        if len(shortname) > 31:
            logging.error("Labelname exceeds max length of 31 " +
                          "characters. Cannot add it.")
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
        returned_set = self.qm.get_label_ids(single_label)
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
                             label_id)
        else:
            self.cur.execute("SELECT versionUUID " +
                             "FROM labelToVersion " +
                             "WHERE labelID = %s;",
                             label_id)
        version_ids = self.cur.fetchall()
        version_ids = {(uuid[0]) for uuid in version_ids}
        return set() if version_ids is None else version_ids

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
                         version_uuid)
        labels = self.cur.fetchall()
        if not labels:
            return set()
        else:
            labels_set = {(label[0]) for label in labels}
            return labels_set

    def get_filemaster_id(self,
                          version_uuid: str) -> str:
        """Get the id of the filemaster entry associated with
        a specific version identified by its UUID."""
        self.cur.execute('SELECT fileMasterID ' +
                         'FROM exoskeleton.fileVersions ' +
                         'WHERE id = %s;',
                         version_uuid)
        filemaster_id = self.cur.fetchone()
        if not filemaster_id:
            raise ValueError("Invalid filemaster ID")
        else:
            return filemaster_id[0]

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
                         filemaster_id)
        labels = self.cur.fetchall()
        if labels:
            labels_set = {(label[0]) for label in labels}
            return labels_set
        else:
            return set()

    def filemaster_labels_by_url(self,
                                 url: str) -> set:
        """Primary use for automatic test: Get a list of label names
        (not id numbers!) attached to a specific filemaster entry
        via its URL instead of the id. The reason for this:
        The association with the URl predates the filemaster entry / the id."""
        self.cur.execute('SELECT DISTINCT shortName ' +
                         'FROM labels ' +
                         'WHERE ID IN (' +
                         '  SELECT labelID ' +
                         '  FROM labelToMaster ' +
                         '  WHERE urlHash = SHA2(%s,256)' +
                         ');',
                         url)
        labels = self.cur.fetchall()
        if labels:
            labels_set = {(label[0]) for label in labels}
            return labels_set
        else:
            return set()

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
                                labels_to_remove: set):
        """Detaches a label from a UUID / version."""

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        labels_to_remove = userprovided.parameters.convert_to_set(
            labels_to_remove)

        # Get all label-ids
        id_list = self.qm.get_label_ids(labels_to_remove)

        for label_id in id_list:
            self.cur.execute("DELETE FROM labelToVersion " +
                             "WHERE labelID = %s and versionUUID = %s;",
                             (label_id, uuid))
