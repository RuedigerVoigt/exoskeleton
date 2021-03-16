#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class QueueManager manages the action queue for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
from collections import defaultdict  # noqa # pylint: disable=unused-import
import logging
import time
from typing import Union, Optional
from urllib.parse import urlparse
import uuid


# external dependencies:
import pymysql
import userprovided

from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import notification_manager
from exoskeleton import statistics_manager
from exoskeleton import time_manager


class QueueManager:
    """Manage the queue and labels for the exoskeleton framework."""
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection,
            time_manager_object: time_manager.TimeManager,
            stats_manager_object: statistics_manager.StatisticsManager,
            actions_object: actions.ExoActions,
            notification_manager_object: notification_manager.NotificationManager,
            bot_behavior: dict,
            milestone: int) -> None:
        # Connection object AND cursor for the queue manager to get a new
        # cursor in case there is a problem.
        # Planned to be replaced with a connection pool. See issue #20
        self.db = db_connection
        self.cur: pymysql.cursors.Cursor = self.db.get_cursor()
        self.time = time_manager_object
        self.stats = stats_manager_object
        self.action = actions_object
        self.notify = notification_manager_object

        self.stop_if_queue_empty: bool = bot_behavior.get(
            'stop_if_queue_empty', False)
        userprovided.parameters.enforce_boolean(
            self.stop_if_queue_empty, 'stop_if_queue_empty')

        # Time to wait after the queue is empty to check for new elements:
        self.queue_revisit: int = bot_behavior.get('queue_revisit', 20)
        self.queue_revisit = userprovided.parameters.int_in_range(
            "queue_revisit", self.queue_revisit, 10, 50, 50)

        self.milestone = milestone

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # HANDLE THE BLOCKLIST
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def check_blocklist(self,
                        fqdn: str) -> bool:
        """Check if a specific FQDN is on the blocklist."""

        self.cur.execute('SELECT COUNT(*) FROM blockList ' +
                         'WHERE fqdnhash = SHA2(%s,256);',
                         (fqdn.strip(), ))
        response = self.cur.fetchone()
        count = int(response[0]) if response else 0  # type: ignore[index]
        if count > 0:
            return True
        return False

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None) -> None:
        """Add a specific fully qualified domain name (fqdn)
           - like www.example.com - to the blocklist."""
        if len(fqdn) > 255:
            raise ValueError('No valid FQDN can be longer than 255 ' +
                             'characters. Exoskeleton can only block ' +
                             'a FQDN but not URLs.')

        try:
            self.cur.execute('INSERT INTO blockList ' +
                             '(fqdn, fqdnHash, comment) ' +
                             'VALUES (%s, SHA2(%s,256), %s);',
                             (fqdn.strip(), fqdn.strip(), comment))
        except pymysql.err.IntegrityError:
            logging.info('FQDN already on blocklist.')

    def unblock_fqdn(self,
                     fqdn: str) -> None:
        """Remove a specific fqdn from the blocklist."""
        self.cur.execute('DELETE FROM blockList ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         (fqdn.strip(), ))

    def truncate_blocklist(self) -> None:
        """Remove *all* entries from the blocklist."""
        logging.info("Truncating the blocklist.")
        self.cur.execute('TRUNCATE TABLE blockList;')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ADDING TO AND REMOVING FROM THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_to_queue(self,
                     url: str,
                     action: int,
                     labels_master: set = None,
                     labels_version: set = None,
                     prettify_html: bool = False,
                     force_new_version: bool = False) -> Optional[str]:
        """ More general function to add items to queue. Called by
        add_file_download, add_save_page_code and add_page_to_pdf."""

        if action not in (1, 2, 3, 4):
            logging.error('Invalid value for action!')
            return None

        # Check if the FQDN of the URL is on the blocklist
        hostname = urlparse(url).hostname
        if hostname and self.check_blocklist(hostname):
            logging.error('Cannot add URL to queue: FQDN is on blocklist.')
            return None

        if prettify_html and action != 2:
            logging.error(
                'Option prettify_html not supported for this action.')
            prettify_html = False

        try:
            url = userprovided.url.normalize_url(url)
        except ValueError:
            logging.error('Could not add url.')
            return None

        # Check if the scheme is either http or https
        # (others are not supported by requests)
        if not userprovided.url.is_url(url, ('http', 'https')):
            logging.error(
                'Could not add URL %s : invalid or unsupported scheme', url)
            return None

        # Add labels for the master entry.
        # Ignore labels for the version at this point, as it might
        # not get processed.
        if labels_master:
            self.assign_labels_to_master(url, labels_master)

        if not force_new_version:
            # check if the URL has already been processed
            id_in_file_master = self.get_filemaster_id_by_url(url)

            if id_in_file_master:
                # The URL has been processed in _some_ way.
                # Check if was the _same_ as now requested.
                self.cur.execute('SELECT id FROM fileVersions ' +
                                 'WHERE fileMasterID = %s AND ' +
                                 'actionAppliedID = %s;',
                                 (id_in_file_master, action))
                version_id = self.cur.fetchone()
                if version_id:
                    logging.info(
                        'Skipping file already processed in the same way.')
                    return None

                # log and simply go on
                logging.debug('The file has already been processed, BUT not ' +
                              'in this way. Adding task to the queue.')
            else:
                # File has not been processed yet.
                # If the exact same task is *not* already in the queue,
                # add it.
                if self.__get_queue_uuids(url, action):
                    logging.info('Exact same task already in queue.')
                    return None

        # generate a random uuid for the file version
        uuid_value = uuid.uuid4().hex

        # get FQDN from URL
        fqdn = urlparse(url).hostname

        # add the new task to the queue
        self.cur.execute('INSERT INTO queue ' +
                         '(id, action, url, urlHash, fqdnHash, ' +
                         'prettifyHtml) VALUES ' +
                         '(%s, %s, %s, SHA2(%s,256), SHA2(%s,256), %s);',
                         (uuid_value, action, url, url, fqdn, prettify_html))

        # link labels to version item
        if labels_version:
            self.assign_labels_to_uuid(uuid_value, labels_version)

        return uuid_value

    def __get_queue_uuids(self,
                          url: str,
                          action: int) -> set:
        """Based on the URL and action ID this returns a set of UUIDs in the
        *queue* that match those. Normally this set has a single element,
        but as you can force exoskeleton to repeat tasks on the same
        URL it can be multiple. Returns an empty set if such combination
        is not in the queue."""
        self.cur.execute('SELECT id FROM queue ' +
                         'WHERE urlHash = SHA2(%s,256) AND ' +
                         'action = %s ' +
                         'ORDER BY addedToQueue ASC;',
                         (url, action))
        queue_uuids = self.cur.fetchall()
        uuid_set = set()
        if queue_uuids:
            uuid_set = {uuid[0] for uuid in queue_uuids}  # type: ignore[index]
        return uuid_set

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        """Get the id of the filemaster entry associated with this URL"""

        try:
            url = userprovided.url.normalize_url(url)
        except ValueError:
            return None

        self.cur.execute('SELECT id FROM fileMaster ' +
                         'WHERE urlHash = SHA2(%s,256);',
                         (url, ))
        id_in_file_master = self.cur.fetchone()
        if id_in_file_master:
            return id_in_file_master[0]  # type: ignore[index]
        return None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # HANDLE LABELS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __define_new_label(self,
                           shortname: str,
                           description: str = None) -> None:
        """ If the label is not already in use, define a new label
            and a description. """
        if len(shortname) > 31:
            logging.error(
                "Cannot add labelname exceeding max length of 31 characters.")
            return
        try:
            self.cur.execute('INSERT INTO labels (shortName, description) ' +
                             'VALUES (%s, %s);',
                             (shortname, description))

            logging.debug('Added label to the database.')
        except pymysql.err.IntegrityError:
            logging.debug('Label already existed.')

    def assign_labels_to_master(self,
                                url: str,
                                labels: Union[set, None]) -> None:
        """ Assigns one or multiple labels to the *fileMaster* entry.
            Removes duplicates and adds new labels to the label list
            if necessary."""

        try:
            url = userprovided.url.normalize_url(url)
        except ValueError:
            return None

        if not labels:
            return None

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            # Make sure all labels are in the database table.
            # -> If they already exist or malformed the command
            # will be ignored by the dbms.
            self.__define_new_label(label)

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
                              uuid: str,
                              labels: Union[set, None]) -> None:
        """ Assigns one or multiple labels either to a specific
            version of a file.
            Removes duplicates and adds new labels to the label list
            if necessary.."""

        if not labels:
            return

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        label_set = userprovided.parameters.convert_to_set(labels)

        for label in label_set:
            # Make sure all labels are in the database table.
            # -> If they already exist or malformed the command
            # will be ignored by the dbms.
            self.__define_new_label(label)

        # Get all label-ids
        id_list = self.get_label_ids(label_set)

        # Check if there are already labels assigned with the version
        self.cur.execute('SELECT labelID ' +
                         'FROM labelToVersion ' +
                         'WHERE versionUUID = %s;', (uuid, ))
        ids_found = self.cur.fetchall()
        ids_associated = set()
        if ids_found:
            ids_associated = set(ids_found)
        # ignore all labels already associated:
        remaining_ids = tuple(id_list - ids_associated)

        if len(remaining_ids) > 0:
            # Case: there are new labels
            # Convert into a format to INSERT with executemany
            insert_list = [(id, uuid) for id in remaining_ids]
            self.cur.executemany('INSERT IGNORE INTO labelToVersion ' +
                                 '(labelID, versionUUID) ' +
                                 'VALUES (%s, %s);', insert_list)

    def get_label_ids(self,
                      label_set: Union[set, str]) -> set:
        """ Given a set of labels, this returns the corresponding ids
            in the labels table. """
        if label_set:
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
            label_set = set()
            if label_id:
                label_set = {(id[0]) for id in label_id}  # type: ignore[index]

            return set() if label_id is None else label_set
        logging.error('No labels provided to get_label_ids().')
        return set()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESSING THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_next_task(self) -> Optional[str]:
        "Get the next suitable task"
        self.cur.execute('CALL next_queue_object_SP();')
        return self.cur.fetchone()  # type: ignore[return-value]

    def delete_from_queue(self,
                          queue_id: str) -> None:
        """Remove all label links from a queue item
           and then delete it from the queue."""
        # callproc expects a tuple. Do not remove the comma:
        self.cur.callproc('delete_from_queue_SP', (queue_id,))

    def process_queue(self) -> None:
        """Process the queue"""
        self.stats.log_queue_stats()

        while True:
            try:
                next_in_queue = self.get_next_task()
            except pymysql.err.OperationalError as e:
                if e.args[0] == 2013:  # errno
                    # this error is unusual. Give the db some time:
                    logging.error('Lost connection to database server. ' +
                                  'Trying to restore it in 10 seconds ...')
                    time.sleep(10)
                    try:
                        self.cur = self.db.get_cursor()
                        next_in_queue = self.get_next_task()
                        logging.info('Restored database connection!')
                    except Exception:
                        logging.error(
                            'Could not reestablish database server connection!',
                            exc_info=True)
                        self.notify.send_msg('abort_lost_db')
                        raise ConnectionError(
                            'Could not restore lost database connection.')
                else:
                    logging.error(
                        'Unexpected Operational Error', exc_info=True)
                    raise

            if next_in_queue is None:
                # no actionable item in the queue
                if self.stop_if_queue_empty:
                    # Bot is configured to stop if queue is empty
                    # => check if that is only temporary or everything is done
                    self.cur.execute(
                        'SELECT num_items_with_temporary_errors();')
                    num_temp_errors = self.cur.fetchone()[0]  # type: ignore[index]

                    if num_temp_errors > 0:
                        # there are still tasks, but they have to wait
                        logging.debug("Tasks with temporary errors: " +
                                      "waiting %s seconds until next try.",
                                      self.queue_revisit)
                        time.sleep(self.queue_revisit)
                        continue

                    # Nothing left (i.e. num_temp_errors == 0)
                    logging.info('Queue empty. Bot stops as configured.')

                    self.cur.execute(
                        'SELECT num_items_with_permanent_error();')
                    num_permanent_errors = self.cur.fetchone()[0]  # type: ignore[index]
                    if num_permanent_errors > 0:
                        logging.error("%s permanent errors!",
                                      num_permanent_errors)
                    self.notify.send_finish_msg(num_permanent_errors)
                    break

                logging.debug(
                    "No actionable task: waiting %s seconds until next check",
                    self.queue_revisit)
                time.sleep(self.queue_revisit)
                continue

            # Got a task from the queue!
            queue_id = next_in_queue[0]
            action = next_in_queue[1]
            url = next_in_queue[2]
            url_hash = next_in_queue[3]
            prettify_html = (next_in_queue[4] == 1)

            # The FQDN might have been added to the blocklist *after*
            # the task entered into the queue!
            # (We now that hostname is not None, as the URL was checked for
            # validity before beeing added: so ignore for mypy is OK)
            if self.check_blocklist(
                    urlparse(url).hostname):  # type: ignore[arg-type]
                logging.error('Cannot process queue item as the FQDN ' +
                              'has meanwhile been added to the blocklist!')
                self.delete_from_queue(queue_id)
                logging.info('Removed item from queue: FQDN on blocklist.')
            else:
                if action == 1:  # download file to disk
                    self.action.get_object(queue_id, 'file', url, url_hash)
                elif action == 2:  # save page code into database
                    self.action.get_object(
                        queue_id, 'content', url, url_hash, prettify_html)
                elif action == 3:  # headless Chrome to create PDF
                    self.action.page_to_pdf(url, url_hash, queue_id)
                elif action == 4:  # save page text into database
                    self.action.get_object(
                        queue_id, 'text', url, url_hash, prettify_html)
                else:
                    logging.error('Unknown action id!')

                if self.milestone and self.check_is_milestone():
                    stats = self.stats.queue_stats()
                    remaining_tasks = (stats['tasks_without_error'] +
                                       stats['tasks_with_temp_errors'])
                    self.notify.send_milestone_msg(
                        self.stats.get_processed_counter(),
                        remaining_tasks,
                        self.time.estimate_remaining_time(
                            self.stats.get_processed_counter(),
                            remaining_tasks)
                            )

                # wait some interval to avoid overloading the server
                self.time.random_wait()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # MILESTONES
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def check_is_milestone(self) -> bool:
        "Check if a milestone is reached."
        processed = self.stats.get_processed_counter()
        # Check >0 in case the bot starts failing with the first item.
        if (self.milestone and
                processed > 0 and
                (processed % self.milestone) == 0):
            logging.info("Milestone reached: %s processed", str(processed))
            return True
        return False
