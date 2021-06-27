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
from typing import Optional
from urllib.parse import urlparse
import uuid


# external dependencies:
import pymysql
import userprovided

from exoskeleton import actions
from exoskeleton import blocklist_manager
from exoskeleton import database_connection
from exoskeleton import label_manager
from exoskeleton import notification_manager
from exoskeleton import statistics_manager
from exoskeleton import time_manager


class QueueManager:
    "Manage the queue and labels for the exoskeleton framework."
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection,
            blocklist_manager_object: blocklist_manager.BlocklistManager,
            time_manager_object: time_manager.TimeManager,
            stats_manager_object: statistics_manager.StatisticsManager,
            actions_object: actions.ExoActions,
            notification_manager_object: notification_manager.NotificationManager,
            label_manager_object: label_manager.LabelManager,
            bot_behavior: dict,
            milestone: Optional[int] = None) -> None:
        # Connection object AND cursor for the queue manager to get a new
        # cursor in case there is a problem.
        # Planned to be replaced with a connection pool. See issue #20
        self.db_connection = db_connection
        self.cur: pymysql.cursors.Cursor = self.db_connection.get_cursor()
        self.blocklist = blocklist_manager_object
        self.time = time_manager_object
        self.stats = stats_manager_object
        self.action = actions_object
        self.notify = notification_manager_object
        self.labels = label_manager_object

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
        if hostname and self.blocklist.check_blocklist(hostname):
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
            self.labels.assign_labels_to_master(url, labels_master)

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
        self.cur.callproc('add_to_queue_SP',
                          (uuid_value, action, url, fqdn, prettify_html))

        # link labels to version item
        if labels_version:
            self.labels.assign_labels_to_uuid(uuid_value, labels_version)

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
        return {uuid[0] for uuid in queue_uuids} if queue_uuids else set()  # type: ignore[index]

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        "Get the id of the filemaster entry associated with this URL"

        try:
            url = userprovided.url.normalize_url(url)
        except ValueError:
            return None

        self.cur.execute('SELECT id FROM fileMaster ' +
                         'WHERE urlHash = SHA2(%s,256);',
                         (url, ))
        id_in_file_master = self.cur.fetchone()
        return id_in_file_master[0] if id_in_file_master else None  # type: ignore[index]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESSING THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_next_task(self) -> Optional[str]:
        "Get the next suitable task"
        self.cur.execute('CALL next_queue_object_SP();')
        return self.cur.fetchone()  # type: ignore[return-value]

    def delete_from_queue(self,
                          queue_id: str) -> None:
        "Remove all label links from item and delete it from the queue."
        self.cur.callproc('delete_from_queue_SP', (queue_id,))

    def process_queue(self) -> None:
        "Process the queue"
        self.stats.log_queue_stats()

        while True:
            try:
                next_in_queue = self.get_next_task()
            except pymysql.err.OperationalError as e:
                if e.args[0] == 2013:  # errno
                    # this error is unusual. Give the db some time:
                    logging.error('Lost database connection. ' +
                                  'Trying to restore it in 10 seconds ...')
                    time.sleep(10)
                    try:
                        self.cur = self.db_connection.get_cursor()
                        next_in_queue = self.get_next_task()
                        logging.info('Restored database connection!')
                    except Exception as exc:
                        msg = 'Could not reestablish database connection'
                        logging.exception(msg, exc_info=True)
                        self.notify.send_msg_abort_lost_db()
                        raise ConnectionError(msg) from exc
                else:
                    logging.error(
                        'Unexpected Operational Error', exc_info=True)
                    raise

            if next_in_queue is None:
                # no actionable item in the queue
                if self.stop_if_queue_empty:
                    # Bot is configured to stop if queue is empty
                    # => check if that is only temporary or everything is done

                    if self.stats.num_tasks_w_temporary_errors() > 0:
                        # there are still tasks, but they have to wait
                        logging.debug("Tasks with temporary errors: " +
                                      "waiting %s seconds until next try.",
                                      self.queue_revisit)
                        time.sleep(self.queue_revisit)
                        continue

                    # Nothing left (i.e. num_temp_errors == 0)
                    logging.info('Queue empty. Bot stops as configured.')

                    num_permanent_errors = self.stats.num_tasks_w_permanent_errors()
                    if num_permanent_errors > 0:
                        logging.error("%s permanent errors!",
                                      num_permanent_errors)
                    self.notify.send_msg_finish(num_permanent_errors)
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
            if self.blocklist.check_blocklist(
                    urlparse(url).hostname):  # type: ignore[arg-type]
                logging.error(
                    'Cannot process queue item: FQDN meanwhile on blocklist!')
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

                if self.check_is_milestone():
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
        if self.milestone is None or self.milestone == 0:
            return False
        processed = self.stats.get_processed_counter()
        # Check >0 in case the bot starts failing with the first item.
        if (processed > 0 and (processed % self.milestone) == 0):
            logging.info("Milestone reached: %s processed", str(processed))
            return True
        return False
