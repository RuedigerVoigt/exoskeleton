#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~

"""

# python standard libraries:
from collections import Counter
import errno
import logging
import os
import queue
import random
import time
from typing import Union
from urllib.parse import urlparse


# 3rd party libraries:
import pymysql
import requests

import exoskeleton.checks as checks
import exoskeleton.communication as communication
import exoskeleton.utils as utils



class Exoskeleton:

    def __init__(self,
                 database_name: str,
                 database_user: str,
                 database_passphrase: str,
                 database_type: str = 'MariaDB',
                 database_host: str = 'localhost',
                 database_port: int = None,
                 project_name: str = 'Bot',
                 bot_user_agent: str = 'BOT (http://www.example.com)',
                 min_wait: float = 5,
                 max_wait: float = 20,
                 mail_server: str = 'localhost',
                 mail_admin: str = None,
                 mail_sender: str = None,
                 milestone_num: int = None,
                 target_directory: str = None,
                 queue_stop_on_empty: bool = False,
                 filename_prefix: str = ''):
        u"""Sets defaults"""

        logging.info('You are using exoskeleton in version 0.6.3 (beta)')

        self.PROJECT = project_name.strip()
        self.USER_AGENT = bot_user_agent.strip()

        self.DB_TYPE = database_type.strip().lower()
        if self.DB_TYPE != 'mariadb':
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

        self.CONNECTION_TIMEOUT = self.get_connection_timeout()

        self.HASH_METHOD = checks.check_hash_algo(self.get_setting('FILE_HASH_METHOD'))

        self.MAIL_START_MSG = True if self.get_setting('MAIL_START_MSG') == 'True' else False
        self.MAIL_FINISH_MSG = True if self.get_setting('MAIL_FINISH_MSG') == 'True' else False
        self.MILESTONE = None
        if type(milestone_num) is int:
            self.MILESTONE = milestone_num
        elif milestone_num is not None:
            raise ValueError
        self.MAIL_ADMIN = checks.check_email_format(mail_admin)
        self.MAIL_SENDER = checks.check_email_format(mail_sender)
        self.MAIL_SEND = False
        if self.MAIL_ADMIN and self.MAIL_SENDER:
            # needing both to send mails
            self.MAIL_SEND = True
        elif self.MILESTONE:
            logging.error('Cannot send mail when milestone is reached. ' +
                          'Either sender or receiver for mails is missing.')
        elif self.MAIL_FINISH_MSG:
            logging.error('Cannot send mail when bot is done. ' +
                          'Either sender or receiver for mails is missing.')

        self.QUEUE_MAX_RETRY = 3 # NOT YET IMPLEMENTET
        if self.get_numeric_setting('QUEUE_MAX_RETRY') is not None:
            self.QUEUE_MAX_RETRY = self.get_numeric_setting('QUEUE_MAX_RETRY')
        self.QUEUE_REVISIT = 60
        if self.get_numeric_setting('QUEUE_REVISIT') is not None:
            self.QUEUE_REVISIT = self.get_numeric_setting('QUEUE_REVISIT')

        self.WAIT_MIN = 5
        if type(min_wait) in (int, float):
            self.WAIT_MIN = min_wait
        self.WAIT_MAX = 20
        if type(max_wait) in (int, float):
            self.WAIT_MAX = max_wait

        self.cnt = Counter() # type: Counter

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


        self.QUEUE_STOP_IF_EMPTY = queue_stop_on_empty

        self.FILE_PREFIX = filename_prefix.strip()

        self.BOT_START = time.monotonic()
        self.PROCESS_TIME_START = time.process_time()
        logging.debug('started timer')

        self.local_download_queue = queue.Queue() # type: queue.Queue

        self.MAX_PATH_LENGTH = 255


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# SETTINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    def get_setting(self,
                    key: str) -> Union[str, None]:
        u""" Get setting from the database table by using the key. """
        self.cur.execute('SELECT settingValue ' +
                         'FROM settings ' +
                         'WHERE settingKey = %s;', key)
        try:
            setting = self.cur.fetchone()[0]
        except TypeError:
            logging.error('Setting not available')
            return None

        return setting

    def get_numeric_setting(self,
                            key: str) -> Union[int, float, None]:
        u""" Get setting, but return None if not numeric. """
        settingValue = self.get_setting(key)
        if type(settingValue) in (int, float):
            return settingValue
        else:
            return None


    def get_connection_timeout(self) -> Union[float, int]:
        u""" Connection timeout is set in the settings table. """

        timeout = self.get_numeric_setting('CONNECTION_TIMEOUT')

        if timeout is None:
            logging.error('Setting CONNECTION_TIMEOUT is missing. '+
                          'Fallback to 60 seconds.')
            return 60

        try:
            timeout = float(timeout)

            if timeout <= 0:
                logging.error('Negative or zero value for timeout. ' +
                              'Fallback to 60 seconds.')
                return 60
            else:
                if timeout > 120:
                    logging.info('Very high value for timeout: ' +
                                 '%s seconds', timeout)
            logging.debug('Connection timeout set to %s s.', timeout)
            return timeout
        except:
            logging.error('Invalid format for setting CONNECTION_TIMEOUT. ' +
                          'Fallback to 60 seconds.')
            return 60


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ACTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    def get_object(self,
                   queue_id: int,
                   action_type: str,
                   url: str,
                   url_hash: str):
        u""" Generic function to either download a file or store a page's content """
        if action_type not in ('file', 'content'):
            raise ValueError('Invalid action')

        url = url.strip()

        if action_type == 'file':
            name, ext = os.path.splitext(url)
            new_filename = self.FILE_PREFIX + str(queue_id) + ext

            # TO Do: more generic pathhandling
            target_path = self.TARGET_DIR + '/' + new_filename

            logging.debug('starting download of queue id %s', queue_id)

        elif action_type == 'content':
            logging.debug('retrieving content of queue id %s', queue_id)

        try:
            if action_type == 'file':
                r = requests.get(url,
                                 headers={"User-agent": str(self.USER_AGENT)},
                                 timeout=self.CONNECTION_TIMEOUT,
                                 stream=True)
            elif action_type == 'content':
                r = requests.get(url,
                                 headers={"User-agent": str(self.USER_AGENT)},
                                 timeout=self.CONNECTION_TIMEOUT,
                                 stream=False
                                )

            if r.status_code == 200:
                mime_type = ''
                if r.headers.get('content-type') is not None:
                    mime_type = (r.headers.get('content-type')).split(';')[0]

                if action_type == 'file':
                    with open(target_path, 'wb') as f:
                        for block in r.iter_content(1024):
                            f.write(block)
                        logging.debug('file written')
                        hash_value = None
                        if self.HASH_METHOD:
                            hash_value = utils.get_file_hash(target_path,
                                                             self.HASH_METHOD)
                elif action_type == 'content':

                    detected_encoding = str(r.encoding)
                    logging.debug('detected encoding: %s', detected_encoding)

                # both: Log the download and remove the item from Queue
                self.cur.execute('INSERT INTO fileMaster (url, urlHash) ' +
                                 'VALUES (%s, %s);',
                                 (url, url_hash))

                # LAST_INSERT_ID() in MySQL / MariaDB is on connection basis!
                # https://dev.mysql.com/doc/refman/8.0/en/getting-unique-id.html
                # However, it seems unreliable.
                #
                # As of December 2019 "INSERT ... RETURNING" is a feature in the
                # current ALPHA version of MariaDB.
                # Until that version is in use, an extra roundtrip is justified:
                self.cur.execute('SELECT id FROM fileMaster WHERE urlHash = %s;',
                                 url_hash)
                file_id = self.cur.fetchone()[0]

                if action_type == 'file':

                    self.cur.execute('INSERT INTO fileVersions ' +
                                     '(fileID, storageTypeID, pathOrBucket, fileName, ' +
                                     'mimeType, size, hashMethod, hashValue) ' +
                                     'VALUES (%s , 2, %s, %s, %s, %s, %s, %s); ',
                                     (file_id,
                                      self.TARGET_DIR,
                                      new_filename,
                                      mime_type,
                                      utils.get_file_size(target_path),
                                      self.HASH_METHOD,
                                      hash_value)
                                    )

                    logging.debug('download successful')

                elif action_type == 'content':

                    self.cur.execute('INSERT INTO fileVersions ' +
                                     '(fileID, storageTypeID, mimeType) ' +
                                     'VALUES (%s , 1, %s); ',
                                     (file_id, mime_type))

                    self.cur.execute('INSERT INTO fileContent ' +
                                     '(versionID, pageContent) ' +
                                     'VALUES (LAST_INSERT_ID(), %s); ',
                                     r.text)

                # for both actions again:
                self.cur.execute('DELETE FROM queue WHERE id = %s;',
                                 queue_id)

                self.cnt['processed'] += 1
                self.update_host_statistics(url, True)

            if r.status_code in (402, 403, 404, 405, 410, 451):
                self.mark_error(queue_id, r.status_code)
                self.update_host_statistics(url, False)
            elif r.status_code == 429:
                logging.error('The bot hit a rate limit! It queries too ' +
                              'fast => increase min_wait.')
                self.add_crawl_delay_to_item(queue_id)
                self.update_host_statistics(url, False)
            elif r.status_code not in (200, 402, 403, 404, 405, 410, 429, 451):
                logging.error('Unhandeled return code %s', r.status_code)
                self.update_host_statistics(url, False)

        except TimeoutError:
            logging.error('Reached timeout.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queue_id)
            self.update_host_statistics(url, False)

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.update_host_statistics(url, False)
            raise

        except requests.exceptions.MissingSchema:
            logging.error('Missing Schema Exception. Does your URL contain the ' +
                          'protocol i.e. http:// or https:// ? See queue_id = %s',
                          queue_id, exc_info=True)
            self.mark_error(queue_id, 1)

        except Exception:
            logging.error('Unknown exception while trying ' +
                          'to download a file.',
                          exc_info=True)
            self.update_host_statistics(url, False)
            raise


    def get_file(self,
                 queue_id: int,
                 url: str,
                 url_hash: str):
        u"""Download a file and save it in the specified folder."""
        self.get_object(queue_id, 'file', url, url_hash)


    def store_page_content(self,
                           url: str,
                           url_hash: str,
                           queue_id: int):
        u"""Retrieve a page and store it's content to the database. """
        self.get_object(queue_id, 'content', url, url_hash)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DATABASE MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def check_table_existence(self) -> bool:
        u"""Check if all expected tables exist."""
        logging.debug('Checking if the database table structure is complete.')
        expected_tables = ['actions', 'errorType', 'eventLog',
                           'fileContent', 'fileMaster', 'fileVersions',
                           'labels', 'labelToMaster', 'labelToVersion',
                           'queue', 'statisticsHosts', 'storageTypes']
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

        if tables_count != len(expected_tables):
            return False
        logging.info("Found all expected tables.")
        return True


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
        return int(self.cur.fetchone()[0])

    def absolute_run_time(self) -> float:
        u"""Return seconds since init. """
        return time.monotonic() - self.BOT_START

    def get_process_time(self) -> float:
        u"""Return execution time since init"""
        return time.process_time() - self.PROCESS_TIME_START

    def estimate_remaining_time(self) -> float:
        u"""estimate remaining seconds to finish crawl."""
        time_so_far = self.absolute_run_time()
        num_remaining = self.num_items_in_queue()

        if self.cnt['processed'] > 0:
            time_each = time_so_far / self.cnt['processed']
            return num_remaining * time_each

        logging.warning('Cannot estimate remaining time ' +
                        'as there are no data so far.')
        return -1

    def add_file_download(self,
                          url: str,
                          label: set = {}):
        u"""add a file download URL to the queue """
        self.add_to_queue(url, 1)

    def add_save_page_code(self,
                           url: str,
                           label: set = None):
        u""" add an URL to the queue to save it's HTML code into the database."""
        self.add_to_queue(url, 2)

    def add_to_queue(self,
                     url: str,
                     action: int,
                     label: set = None):
        u""" More general function to add items to queue. Called by
        add_file_download and add_save_page_code."""

        if action not in (1, 2):
            logging.error('Invalid value for action to take!')
            return

        # check if the file already has been processed
        self.cur.execute('SElECT id FROM fileMaster WHERE urlHash = SHA2(%s,256);', url)
        id_in_file_master = self.cur.fetchone()
        if id_in_file_master is not None:
            logging.info('The file has already been processed. Skipping it.')
            # TO DO: check if an Label has to be added
            return

        try:
            self.cur.execute('INSERT INTO queue (action, url, urlHash) ' +
                             'VALUES (%s, %s, SHA2(%s,256));',
                             (action, url, url))
        except pymysql.IntegrityError:
            # No further check here as an duplicate url / urlHash is
            # the only thing that can cause that error here.
            logging.info('URL already in queue. Not adding it again.')
            # TO DO: check if an Label has to be added


    def add_crawl_delay_to_item(self,
                                queue_id: int,
                                delay_seconds: int = None):
        u"""In case of timeout or temporary error add a delay until
        the same URL is queried again. """
        logging.debug('Adding crawl delay to queue item %s', queue_id)
        waittime = 30
        if delay_seconds:
            waittime = delay_seconds
        if self.DB_TYPE == 'mariadb':
            self.cur.execute('UPDATE queue ' +
                             'SET delayUntil = ADDTIME(NOW(), %s) ' +
                             'WHERE id = %s', (waittime, queue_id))

    def mark_error(self,
                   queue_id: int,
                   error: int):
        u""" Mark item in queue that causes permant error.

        Has to be marked as otherwise exoskelton will try to
        download it over and over again."""

        self.cur.execute('UPDATE queue ' +
                         'SET causesError = %s ' +
                         'WHERE id = %s;', (error, queue_id))
        if error in (429, 500, 503):
            self.add_crawl_delay_to_item(queue_id, 600)

    def process_queue(self):
        u"""Process the queue"""
        while True:
            # get the next suitable task
            self.cur.execute('SELECT ' +
                             '  id' +
                             '  ,action' +
                             '  ,url ' +
                             '  ,urlHash ' +
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
                                  self.QUEUE_REVISIT)
                    time.sleep(self.QUEUE_REVISIT)
                    continue
            else:
                # got a task from the queue
                queue_id = next_in_queue[0]
                action = next_in_queue[1]
                url = next_in_queue[2]
                url_hash = next_in_queue[3]

                if action == 1:
                    # download file to disk
                    self.get_file(queue_id, url, url_hash)
                elif action == 2:
                    # save page code into database
                    self.store_page_content(url, url_hash, queue_id)
                else:
                    logging.error('Unknown action id!')

                if self.MILESTONE:
                    self.check_milestone()

                # wait some interval to avoid overloading the server
                self.random_wait()

    def update_host_statistics(self,
                               url: str,
                               success: bool = True):
        u""" Updates the host based statistics"""

        fqdn = urlparse(url).hostname
        if success:
            successful, problems = 1, 0
        else:
            successful, problems = 0, 1

        self.cur.execute('INSERT INTO statisticsHosts ' +
                         '(fqdnHash, fqdn, successful, ' +
                         'problems) ' +
                         'VALUES (MD5(%s), %s, %s, %s) ' +
                         'ON DUPLICATE KEY UPDATE ' +
                         'successful = successful + %s, ' +
                         'problems = problems + %s;',
                         (fqdn, fqdn, successful, problems,
                          successful, problems))

    def check_milestone(self):
        u""" Check if milestone is reached and
        in case send mail if configured so."""
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
