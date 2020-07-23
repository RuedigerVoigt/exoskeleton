#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database connection management for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
from collections import defaultdict
import logging
from typing import Union, Optional
from urllib.parse import urlparse
import uuid


# external dependencies:
import pymysql
import userprovided


class QueueManager:
    u"""Manage the queue and labels for the exoskeleton framework."""

    # If there is a temporary error, exoskeleton delays the
    # next try until the configured maximum of tries is
    # reached.
    # The time between tries is definied here to be able
    # to overwrite it in case of an automatic tests to
    # avoid multi-hour runtimes.
    # Steps: 1/4h, 1/2h, 1h, 3h, 6h
    DELAY_TRIES = (900, 1800, 3600, 10800, 21600)

    def __init__(self,
                 db_cursor,
                 time_manager_object,
                 bot_behavior: dict):
        self.cur = db_cursor
        self.tm = time_manager_object

        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = bot_behavior.get('queue_max_retries', 3)
        self.queue_max_retries = userprovided.parameters.int_in_range(
            "queue_max_retries", self.queue_max_retries, 0, 10, 3)

        # Seconds to wait before contacting the same FQDN again, after the bot
        # hit a rate limit. Defaults to 1860 seconds (i.e. 31 minutes):
        self.rate_limit_wait: int = bot_behavior.get('rate_limit_wait', 1860)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # HANDLE THE BLOCKLIST
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def check_blocklist(self,
                        fqdn: str) -> bool:
        u"""Check if a specific FQDN is on the blocklist."""

        self.cur.execute('SELECT COUNT(*) FROM blockList ' +
                         'WHERE fqdnhash = SHA2(%s,256);',
                         fqdn.strip())
        count = (self.cur.fetchone())[0]
        if count > 0:
            return True
        return False

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None):
        u"""Add a specific fully qualified domain name (fqdn)
            - like www.example.com - to the blocklist."""
        if len(fqdn) > 255:
            raise ValueError('No valid FQDN can be longer than 255 ' +
                             'characters. Exoskeleton can only block ' +
                             'a FQDN but not URLs.')
        else:
            try:
                self.cur.execute('INSERT INTO blockList ' +
                                 '(fqdn, fqdnHash, comment) ' +
                                 'VALUES (%s, SHA2(%s,256), %s);',
                                 (fqdn.strip(), fqdn.strip(), comment))
            except pymysql.err.IntegrityError:
                logging.info('FQDN already on blocklist.')

    def unblock_fqdn(self,
                     fqdn: str):
        u"""Remove a specific fqdn from the blocklist."""
        self.cur.execute('DELETE FROM blockList ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         fqdn.strip())

    def truncate_blocklist(self):
        u"""Remove *all* entries from the blocklist."""
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
        u""" More general function to add items to queue. Called by
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
            logging.error('Option prettify_html not supported for ' +
                          'this action. Will be ignored.')
            prettify_html = False

        # Excess whitespace might be common (copy and paste)
        # and would change the hash:
        url = url.strip()
        # check if it is an URL and if it is either http or https
        # (other schemas are not supported by requests)
        if not userprovided.url.is_url(url, ('http', 'https')):
            logging.error('Could not add URL %s : invalid or unsupported ' +
                          'scheme', url)
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
                    logging.info('File has already been processed ' +
                                 'in the same way. Skipping it.')
                    return None
                else:
                    # log and simply go on
                    logging.debug('The file has already been processed, ' +
                                  'BUT not in this way. ' +
                                  'Adding task to the queue.')
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
        u"""Based on the URL and action ID this returns a set of UUIDs in the
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
        uuid_set: set = {uuid[0] for uuid in queue_uuids}
        if uuid_set:
            return uuid_set
        else:
            return set()

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        u"""Get the id of the filemaster entry associated with this URL"""
        self.cur.execute('SELECT id FROM fileMaster ' +
                         'WHERE urlHash = SHA2(%s,256);',
                         url)
        id_in_file_master = self.cur.fetchone()
        if id_in_file_master:
            return id_in_file_master[0]
        else:
            return None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # HANDLE LABELS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __define_new_label(self,
                           shortname: str,
                           description: str = None):
        u""" If the label is not already in use, define a new label
        and a description. """
        if len(shortname) > 31:
            logging.error("Labelname exceeds max length of 31 " +
                          "characters. Cannot add it.")
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
                                labels: Union[set, None]):
        u""" Assigns one or multiple labels to the *fileMaster* entry.
        Removes duplicates and adds new labels to the label list
        if necessary.."""

        if not labels:
            return
        else:
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
                             'WHERE urlHash = SHA2(%s,256);', url)
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

    def assign_labels_to_uuid(self,
                              uuid: str,
                              labels: Union[set, None]):
        u""" Assigns one or multiple labels either to a specific
        version of a file.
        Removes duplicates and adds new labels to the label list
        if necessary.."""

        if not labels:
            return
        else:
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
                             'WHERE versionUUID = %s;', uuid)
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
        u""" Given a set of labels, this returns the corresponding ids
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
                label_set = {(id[0]) for id in label_id}

            return set() if label_id is None else label_set
        logging.error('No labels provided to get_label_ids().')
        return set()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESSING THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_next_task(self):
        u""" Get the next suitable task"""
        self.cur.execute('CALL next_queue_object_SP();')
        return self.cur.fetchone()

    def delete_from_queue(self,
                          queue_id: str):
        u"""Remove all label links from a queue item
            and then delete it from the queue."""
        # callproc expects a tuple. Do not remove the comma:
        self.cur.callproc('delete_from_queue_SP', (queue_id,))

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ERROR HANDLING AND CRAWL DELAYS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_crawl_delay(self,
                        queue_id: str,
                        error_type: Optional[int] = None):
        u"""In case of a timeout or a temporary error increment the counter for
            the number of tries by one. If the configured maximum of tries
            was reached, mark it as a permanent error. Otherwise add a delay,
            so exoskelton does not try the same task again. As multiple tasks
            may affect the same URL, the delay is added to all of them."""
        wait_time = 0

        # Increase the tries counter
        self.cur.execute('UPDATE queue ' +
                         'SET numTries = numTries + 1 ' +
                         'WHERE id =%s;', queue_id)
        # How many times this task was tried?
        self.cur.execute('SELECT numTries FROM queue WHERE id = %s;',
                         queue_id)
        num_tries = int((self.cur.fetchone())[0])

        # Does the number of tries exceed the configured maximum?
        if num_tries == self.queue_max_retries:
            # This is treated as a *permanent* failure!
            logging.error('Giving up: too many tries for queue item %s',
                          queue_id)
            self.mark_permanent_error(queue_id, 3)
        else:
            logging.info('Adding crawl delay to queue item %s', queue_id)
            # Using the class constant DELAY_TRIES because it can be easily
            # overwritten for automatic testing.
            if num_tries == 1:
                wait_time = self.DELAY_TRIES[0]  # 15 minutes
            elif num_tries == 2:
                wait_time = self.DELAY_TRIES[1]  # 30 minutes
            elif num_tries == 3:
                wait_time = self.DELAY_TRIES[2]  # 1 hour
            elif num_tries == 4:
                wait_time = self.DELAY_TRIES[3]  # 3 hours
            elif num_tries > 4:
                wait_time = self.DELAY_TRIES[4]  # 6 hours

            # Add the same delay to all tasks accesing the same URL MariaDB /
            # MySQL throws an error if the same table is specified
            # both as a target for 'UPDATE' and a source for data.
            # Therefore, two steps instead of a Sub-Select.
            self.cur.execute('SELECT urlHash FROM queue WHERE id = %s;',
                             queue_id)
            url_hash = (self.cur.fetchone())[0]
            self.cur.execute('UPDATE queue ' +
                             'SET delayUntil = ADDTIME(NOW(), %s) ' +
                             'WHERE urlHash = %s;',
                             (wait_time, url_hash))
            # Add the error type to the specific task that caused the delay
            if error_type:
                self.cur.execute('UPDATE queue ' +
                                 'SET causesError = %s ' +
                                 'WHERE id = %s;',
                                 (error_type, queue_id))

    def mark_permanent_error(self,
                             queue_id: str,
                             error: int):
        u""" Mark item in queue that causes a permanent error.
            Without this exoskeleton would try to execute the
            task over and over again."""

        self.cur.execute('UPDATE queue ' +
                         'SET causesError = %s, ' +
                         'delayUntil = NULL ' +
                         'WHERE id = %s;', (error, queue_id))
        logging.info('Marked queue-item that caused a permanent problem.')

    def forget_specific_error(self,
                              specific_error: int):
        u"""Treat all queued tasks, that are marked to cause a *specific*
            error, as if they are new tasks by removing that mark and
            any delay. The number of the error has to correspond to the
            errorType database table."""
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0,"
                         "delayUntil = NULL " +
                         "WHERE causesError = %s;",
                         specific_error)

    def forget_error_group(self,
                           permanent: bool):
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0, " +
                         "delayUntil = NULL " +
                         "WHERE causesError IN (" +
                         "    SELECT id from errorType WHERE permanent = %s);",
                         (1 if permanent else 0))

    def forget_all_errors(self):
        u"""Treat all queued tasks, that are marked to cause any type of
            error, as if they are new tasks by removing that mark and
            any delay."""
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0, " +
                         "delayUntil = NULL;")

    def add_rate_limit(self,
                       fqdn: str):
        u"""If a bot receives the statuscode 429 ('too many requests') it hit
            a rate limit. Adding the fully qualified domain name to the rate
            limit list, ensures that this FQDN is not contacted for a
            predefined time."""
        msg = (f"Bot hit a rate limit with {fqdn}. Will not try to " +
               f"contact this host for {self.rate_limit_wait} seconds.")
        logging.error(msg)
        # Always use SEC_TO_TIME() to avoid unexpected behavior if
        # just adding seconds as a plain number.
        self.cur.execute('INSERT INTO rateLimits ' +
                         '(fqdnHash, fqdn, noContactUntil) VALUES ' +
                         '(SHA2(%s,256), %s, ADDTIME(NOW(), SEC_TO_TIME(%s))) ' +
                         'ON DUPLICATE KEY UPDATE ' +
                         'noContactUntil = ADDTIME(NOW(), SEC_TO_TIME(%s));',
                         (fqdn, fqdn, self.rate_limit_wait,
                          self.rate_limit_wait))

    def forget_specific_rate_limit(self,
                                   fqdn: str):
        self.cur.execute('DELETE FROM rateLimits ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         fqdn)

    def forget_all_rate_limits(self):
        self.cur.execute('TRUNCATE TABLE rateLimits;')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # STATISTICS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __num_tasks_wo_errors(self) -> int:
        u"""Number of tasks left in the queue which are *not* marked as
            causing any kind of error. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_permanent_errors(self) -> int:
        u"""Number of tasks in the queue that are marked as causing a permanent
            error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IN " +
                         "    (SELECT id FROM errorType WHERE permanent = 1);")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_temporary_errors(self) -> int:
        u"""Number of tasks in the queue that are marked as causing a
            temporary error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IN " +
                         "    (SELECT id FROM errorType WHERE permanent = 0);")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_rate_limit(self) -> int:
        u"""Number of tasks in the queue that are marked as causing a permanent
            error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError NOT IN " +
                         "    (SELECT id FROM errorType " +
                         "     WHERE permanent = 1) " +
                         "AND fqdnhash IN " +
                         "    (SELECT fqdnhash FROM rateLimits " +
                         "     WHERE noContactUntil > NOW());")
        return int(self.cur.fetchone()[0])

    def queue_stats(self) -> dict:
        u"""Return a number of statistics about the queue as a dictionary."""
        stats = {
            'tasks_without_error': self.__num_tasks_wo_errors(),
            'tasks_with_temp_errors': self.__num_tasks_w_temporary_errors(),
            'tasks_with_permanent_errors': self.__num_tasks_w_permanent_errors(),
            'tasks_blocked_by_rate_limit': self.__num_tasks_w_rate_limit()
        }
        return stats

    def log_queue_stats(self):
        u"""Log the queue statistics. Especially useful when the bot starts
            ore resumes processing the queue."""
        stats = self.queue_stats()
        overall_workable = (stats['tasks_without_error'] +
                            stats['tasks_with_temp_errors'])
        message = (f"The queue contains {overall_workable} tasks waiting " +
                   f"to be executed. {stats['tasks_blocked_by_rate_limit']} " +
                   f"of those are stalled as the bot hit a rate limit. " +
                   f"{stats['tasks_with_permanent_errors']} cannot be " +
                   f"executed due to permanent errors.")
        logging.info(message)

    def update_host_statistics(self,
                               url: str,
                               successful_requests: int,
                               temporary_problems: int,
                               permanent_errors: int,
                               hit_rate_limit: int):
        u""" Updates the host based statistics. The URL gets shortened to
        the hostname. Increase the different counters."""

        fqdn = urlparse(url).hostname

        self.cur.execute('INSERT INTO statisticsHosts ' +
                         '(fqdnHash, fqdn, successfulRequests, ' +
                         'temporaryProblems, permamentErrors, hitRateLimit) ' +
                         'VALUES (SHA2(%s,256), %s, %s, %s, %s, %s) ' +
                         'ON DUPLICATE KEY UPDATE ' +
                         'successfulRequests = successfulRequests + %s, ' +
                         'temporaryProblems = temporaryProblems + %s, ' +
                         'permamentErrors = permamentErrors + %s, ' +
                         'hitRateLimit = hitRateLimit + %s;',
                         (fqdn, fqdn, successful_requests, temporary_problems,
                          permanent_errors, hit_rate_limit,
                          successful_requests, temporary_problems,
                          permanent_errors, hit_rate_limit))
