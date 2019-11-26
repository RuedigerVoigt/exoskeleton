#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# python standard libraries:
from collections import Counter
import hashlib
import logging
import os
import queue
import random
import re
import smtplib
import sys
import tempfile
import time


# 3rd party libraries:
import pymysql
import requests

import exoskeleton.checks as checks
import exoskeleton.utils as utils
import exoskeleton.communication as communication


class Exoskeleton:

    def __init__(self,
                 project_name: str = 'Bot',
                 bot_user_agent: str = 'BOT (http://www.example.com)',
                 min_wait: float = 5,
                 max_wait: float = 20,
                 timeout: int = 60,
                 database_type: str = 'MariaDB',
                 database_name: str = None,
                 database_user: str = None,
                 database_passphrase: str = None,
                 database_host: str = 'localhost',
                 database_port: int = None,
                 mail_server: str = 'localhost',
                 mail_admin: str = None,
                 mail_sender: str = None,
                 milestone_num: int = None,
                 mail_send_start: bool = True,
                 mail_send_finish: bool = True,
                 target_directory: str = None,
                 hash_algo: str = 'sha1',
                 queue_max_retry: int = 3,
                 queue_stop_on_empty: bool = False,
                 queue_wait_seconds_until_lookup: int = 60,
                 filename_prefix: str = ''):
        u"""Sets defaults"""

        logging.info('You are using exoskeleton in version 0.6.0 (beta)')

        self.PROJECT = project_name.strip()
        self.USER_AGENT = bot_user_agent.strip()

        self.DB_TYPE = database_type.strip().lower()
        if self.DB_TYPE not in ('mariadb'):
            logging.exception("At the moment exoskeleton " +
                              "only supports MariaDB. " +
                              "PostgreSQL support is planned.")
            raise ValueError
        self.DB_HOSTNAME = database_host.strip()

        self.DB_PORT = checks.validate_port(database_port, self.DB_TYPE)
        self.DB_NAME = database_name.strip()
        self.DB_USERNAME = database_user.strip()
        self.DB_PASSPHRASE = database_passphrase.strip()
        if self.DB_PASSPHRASE == '':
            logging.warning('No database passphrase provided.')

        self.WAIT_MIN = 5
        if type(min_wait) in (int, float):
            self.WAIT_MIN = min_wait
        self.WAIT_MAX = 20
        if type(max_wait) in (int, float):
            self.WAIT_MAX = max_wait

        self.cnt = Counter()
        self.MILESTONE = None
        if type(milestone_num) is int:
            self.MILESTONE = milestone_num
        elif milestone_num is not None:
            raise ValueError

        self.MAIL_FINISH_MESSAGE = mail_send_finish
        self.MAIL_ADMIN = checks.check_email_format(mail_admin)
        self.MAIL_SENDER = checks.check_email_format(mail_sender)

        self.MAIL_SEND = False
        if self.MAIL_ADMIN and self.MAIL_SENDER:
            # needing both to send mails
            self.MAIL_SEND = True
        elif self.MILESTONE:
            logging.error('Cannot send mail when milestone is reached. ' +
                          'Either sender or receiver for mails is missing.')
        elif self.MAIL_FINISH_MESSAGE:
            logging.error('Cannot send mail when bot is done. ' +
                          'Either sender or receiver for mails is missing.')

        self.TARGET_DIR = os.getcwd()

        if target_directory is None or target_directory == '':
            logging.warning('Target directory is not set. ' +
                            'Using the current working directory ' +
                            '%s to store files!',
                            self.TARGET_DIR)
        else:
            # Assuming that if a directory was set, it has
            # to be used. Therefore no fallback to the current
            # working directory.
            target_directory = target_directory.strip()
            if os.path.isdir(target_directory):
                self.TARGET_DIR = target_directory
                logging.debug("Set target directory to %s",
                              target_directory)
            else:
                raise OSError("Cannot find or access the user " +
                              "supplied target directory! " +
                              "Create this directory or " +
                              "check permissions.")

        self.HASH_METHOD = checks.check_hash_algo(hash_algo)

        self.QUEUE_MAX_RETRY = queue_max_retry
        self.QUEUE_STOP_IF_EMPTY = queue_stop_on_empty
        self.QUEUE_WAIT = queue_wait_seconds_until_lookup

        self.FILE_PREFIX = filename_prefix.strip()

        self.BOT_START = time.monotonic()
        self.PROCESS_TIME_START = time.process_time()
        logging.debug('started timer')

        # check if the timeout is valid and reasonable
        self.CONNECTION_TIMEOUT = 60  # default
        if timeout is None:
            pass
        elif type(timeout) in (float, int):
            if timeout <= 0:
                raise ValueError('Negative or zero value for timeout.')
            else:
                self.CONNECTION_TIMEOUT = timeout
                if timeout > 120:
                    logging.info('Very high value for timeout: ' +
                                 '%s seconds', timeout)
        else:
            raise ValueError('Invalid format for timeout.')

        self.local_download_queue = queue.Queue()

        self.MAX_PATH_LENGTH = 255

        if not (self.DB_TYPE and
                self.DB_HOSTNAME and
                self.DB_PORT and
                self.DB_NAME and
                self.DB_USERNAME):

            # give specific error messages:
            missing_params = []
            if not self.DB_TYPE:
                missing_params.append('database type')
            if not self.DB_HOSTNAME:
                missing_params.append('hostname')
            if not self.DB_PORT:
                missing_params.append('port')
            if not self.DB_NAME:
                missing_params.append('database name')
            if not self.DB_USERNAME:
                missing_params.append('username')
            # ... stop before connection try:
            raise ValueError('The following parameters were not supplied, ' +
                             'but are needed to connect to the database: ' +
                             '{}'.format(','.join(missing_params)))
            # TO DO: trusted connection case

        if self.DB_TYPE == 'mariadb':
            try:
                logging.debug('Trying to connect to database.')
                connection = pymysql.connect(host=self.DB_HOSTNAME,
                                             port=self.DB_PORT,
                                             database=self.DB_NAME,
                                             user=self.DB_USERNAME,
                                             password=self.DB_PASSPHRASE,
                                             autocommit=True)

                self.cur = connection.cursor()
                logging.info('Made database connection.')

                self.check_table_existence()

            except pymysql.InterfaceError:
                logging.exception('Exception related to the database ' +
                                  '*interface*.', exc_info=True)
                raise
            except pymysql.DatabaseError:
                logging.exception('Exception related to the database.',
                                  exc_info=True)
                raise
            except Exception:
                logging.exception('Unknown exception while ' +
                                  'trying to connect to the DBMS.',
                                  exc_info=True)
                raise
        elif self.DB_TYPE == 'postgresql':
            raise NotImplementedError('No PostgreSQL yet.')
        else:
            raise ValueError('Unknown database type.')



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ACTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_file(self,
                 queueId: int,
                 url: str,
                 timeout: int = None):
        u"""Download a file and save it at a specified path."""

        url = url.strip()
        name, ext = os.path.splitext(url)
        new_filename = self.FILE_PREFIX + str(queueId) + ext
        # TO Do: more generic pathhandling
        target_path = self.TARGET_DIR + '/' + new_filename

        try:
            logging.debug('starting download of queue id %s', queueId)

            r = requests.get(url,
                             headers={"User-agent": str(self.USER_AGENT)},
                             timeout=self.CONNECTION_TIMEOUT,
                             stream=True)
            if r.status_code == 200:
                with open(target_path, 'wb') as f:
                    for block in r.iter_content(1024):
                        f.write(block)
                    logging.debug('file written')
                    hash_value = None
                    if self.HASH_METHOD:
                        hash_value = utils.get_file_hash(target_path,
                                                         self.HASH_METHOD)

                    # Log the download and remove the item from Queue
                    self.cur.execute('INSERT INTO fileMaster (url) VALUES (%s);', url)

                    self.cur.execute('INSERT INTO fileVersions ' +
                                     '(fileID, storageTypeID, pathOrBucket, fileName, ' +
                                     'size, hashMethod, hashValue) ' +
                                     'VALUES (LAST_INSERT_ID() , 2, %s, %s, %s, %s, %s); ',
                                     (self.TARGET_DIR,
                                      new_filename,
                                      utils.get_file_size(target_path),
                                      self.HASH_METHOD,
                                      hash_value)
                                      )
                    # LAST_INSERT_ID() in MySQL / MariaDB is on connection basis!
                    # https://dev.mysql.com/doc/refman/8.0/en/getting-unique-id.html

                    self.cur.execute('DELETE FROM queue WHERE id = %s; ',
                                     queueId)

                logging.debug('download successful')
                self.cnt['processed'] += 1
            elif r.status_code in (402, 403, 404, 405, 410, 451):
                self.mark_error(queueId, r.status_code)
            elif r.status_code == 429:
                logging.error('The bot hit a rate limit! It queries too fast => increase min_wait.')
            else:
                logging.error('Unhandeled return code %s', r.status_code)


        except TimeoutError:
            logging.error('Reached timeout trying to download the file.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queueId)
        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            raise
        except requests.exceptions.MissingSchema:
            logging.error('Missing Schema Exception. Does your URL contain the ' +
                        'protocol i.e. http:// or https:// ?', exc_info=True)
            self.mark_error(queueId, 1)
        except Exception:
            logging.error('Unknown exception while trying ' +
                          'to download a file.', exc_info=True)
            raise
        return

    def store_page_content(self,
                           url: str,
                           queueId: int) -> str:
        u"""Retrieve a page and return it as text. """

        logging.debug('retriving %s', url)

        try:

            r = requests.get(url,
                             headers={"User-agent": str(self.USER_AGENT)},
                             timeout=self.CONNECTION_TIMEOUT
                             )

            if r.status_code == 200:
                detectedEncoding = str(r.encoding)
                logging.debug('detected encoding: ' + detectedEncoding)

                self.cur.execute('INSERT INTO fileMaster (url) VALUES (%s);', url)
                self.cur.execute('INSERT INTO fileVersions ' +
                                    '(fileID, storageTypeID) ' +
                                    'VALUES (LAST_INSERT_ID() , 1); ')
                self.cur.execute('INSERT INTO fileContent ' +
                                 '(versionID, pageContent) ' +
                                 'VALUES (LAST_INSERT_ID(), %s); ',
                                 r.text)
                self.cur.execute('DELETE FROM queue WHERE id = %s;', queueId)

                self.cnt['processed'] += 1

        except TimeoutError:
            logging.error('Reached timeout trying to download page content.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queueId)
        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            raise
        except Exception:
            logging.error('Unknown exception while trying ' +
                          'to download the page.', exc_info=True)
            raise

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DATABASE MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def check_table_existence(self) -> bool:
        u"""Check if all expected tables exist."""
        logging.debug('Checking if the database table structure is complete.')
        expected_tables = ['actions', 'queue', 'errorType', 'eventLog',
                           'fileMaster', 'storageTypes', 'fileVersions',
                           'fileContent']
        tables_count = 0
        if self.DB_TYPE == 'mariadb':
            self.cur.execute('SHOW TABLES;')
            tables_found = [item[0] for item in self.cur.fetchall()]
            for t in expected_tables:
                if t in tables_found:
                    tables_count += 1
                    logging.debug('Found table %s', t)
                else:
                    logging.error('Table %s not found.', t)
        if tables_count == 8:
            logging.info("Found all expected tables.")
            return True
        else:
            return False

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# QUEUE MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def random_wait(self):
        u"""Waits for a random time between actions
        (within the interval preset at initialization).
        This is done to avoid to accidentially overload
        the queried host. Some host actually enforce
        limits through IP blocking."""
        query_delay = random.randint(self.WAIT_MIN, self.WAIT_MAX)
        logging.debug("%s seconds delay until next action",
                      query_delay)
        time.sleep(query_delay)
        return

    def num_items_in_queue(self) -> int:
        u"""Number of items left in the queue. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        return self.cur.fetchone()[0]

    def absolute_run_time(self) -> int:
        u"""Return seconds since init. """
        return time.monotonic() - self.BOT_START

    def get_process_time(self) -> int:
        u"""Return execution time since init"""
        return time.process_time() - self.PROCESS_TIME_START

    def estimate_remaining_time(self) -> int:
        u"""estimate remaining seconds to finish crawl."""
        time_so_far = self.absolute_run_time()
        num_remaining = self.num_items_in_queue()

        if self.cnt['processed'] > 0:
            time_each = time_so_far / self.cnt['processed']
            return num_remaining * time_each
        else:
            logging.warning('Cannot estimate remaining time ' +
                            'as there are no data so far.')
            return -1

    def add_file_download(self,
                          url: str):
        u"""add a file download URL to the queue """
        self.cur.execute('INSERT INTO queue (action, url) VALUES (1, %s);',
                         url)

    def add_save_page_code(self,
                           url: str):
        u""" add an URL to the queue to save it's HTML code into the database."""
        self.cur.execute('INSERT INTO queue (action, url) VALUES (2, %s);',
                         url)

    def add_crawl_delay_to_item(self,
                                queueId: int,
                                delay_seconds: int = None):
        u"""In case of timeout or temporary error add a delay until
        the same URL is queried again. """
        logging.debug('Adding crawl delay to queue item %s', queueId)
        waittime = 30
        if delay_seconds:
            waittime = delay_seconds
        if self.DB_TYPE == 'mariadb':
            self.cur.execute('UPDATE queue ' +
                             'SET delayUntil = ADDTIME(NOW(), %s) ' +
                             'WHERE id = %s', (waittime, queueId))

    def mark_error(self,
                   queueId: int,
                   error: int):
        u""" Mark item in queue that causes permant error.

        Has to be marked as otherwise exoskelton will try to
        download it over and over again."""

        self.cur.execute('UPDATE queue ' +
                         'SET causesError = %s ' +
                         'WHERE id = %s;', (error, queueId))
        if error in (429, 500, 503):
            self.add_crawl_delay_to_item(queueId, 600)

    def process_queue(self):
        u"""Process the queue"""
        while True:
            # get the next suitable task
            self.cur.execute('SELECT ' +
                             '  id' +
                             '  ,action' +
                             '  ,url ' +
                             'FROM queue ' +
                             'WHERE causesError IS NULL AND ' +
                             '(delayUntil IS NULL OR delayUntil < NOW()) ' +
                             'ORDER BY addedToQueue ASC ' +
                             'LIMIT 1;')
            next_in_queue = self.cur.fetchone()
            if next_in_queue is None:
                # empty queue: either full stop or wait for new tasks
                if self.QUEUE_STOP_IF_EMPTY:
                    logging.info('Queue empty. Bot stops as configured to do.')
                    subject = self.PROJECT + ": queue empty / bot stopped"
                    content = ("The queue is empty. The bot " + self.PROJECT +
                               " stopped as configured.")
                    if self.MAIL_SEND:
                        communication.send_mail(self.MAIL_ADMIN,
                                                self.MAIL_SENDER,
                                                subject, content)
                    break
                else:
                    logging.debug("Queue empty. Waiting %s seconds until next check",
                                  self.QUEUE_WAIT)
                    time.sleep(self.QUEUE_WAIT)
                    continue
            else:
                # got a task from the queue
                queueId = next_in_queue[0]
                action = next_in_queue[1]
                url = next_in_queue[2]

                if action == 1:
                    # download file to disk
                    self.get_file(queueId, url)
                elif action == 2:
                    # save page code into database
                    self.store_page_content(url, queueId)
                else:
                    logging.error('Unknown action id!')

                if self.MILESTONE:
                    self.check_milestone()

                # wait some interval to avoid overloading the server
                self.random_wait()

    def check_milestone(self):
        processed = self.cnt['processed']
        if type(self.MILESTONE) is int:
            if processed % self.MILESTONE == 0:
                logging.info("Milestone reached: %s processed",
                             str(processed))

            if self.MAIL_SEND:
                subject = (self.PROJECT + ": Milestone reached: " +
                           str(self.cnt['processed']) + " processed")
                content = (str(self.cnt['processed']) + " processed.\n" +
                           str(self.num_items_in_queue()) + " items " +
                           "remaining in the queue.\n" +
                           "Estimated time to complete queue: " +
                           str(self.estimate_remaining_time()) + "seconds.\n")
                communication.send_mail(self.MAIL_ADMIN,
                                        self.MAIL_SENDER,
                                        subject, content)

                return True
            else:
                return False
        elif type(self.MILESTONE) is list:
            logging.error("Feature not yet implemented")
            return False
