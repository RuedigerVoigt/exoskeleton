#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
A Python framework to build a basic crawler / scraper with a MariaDB backend.
"""

# python standard library:
from collections import Counter
from collections import defaultdict
import logging
import pathlib
import queue
import random
import subprocess
import time
from typing import Union, List, Optional
from urllib.parse import urlparse
import uuid


# 3rd party libraries:
import pymysql
import urllib3  # type: ignore
import requests
# Sister projects:
import userprovided
import bote

# import other modules of this framework
import exoskeleton.utils as utils


class Exoskeleton:
    u""" Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint3 to complain a lot.
    # As the complexity is needed, disable some warnings:
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-public-methods
    # pylint: disable=too-many-branches

    MAX_PATH_LENGTH = 255

    def __init__(self,
                 database_settings: dict,
                 target_directory: str,
                 filename_prefix: str = '',
                 project_name: str = 'Bot',
                 bot_user_agent: str = 'Bot',
                 bot_behavior: Union[dict, None] = None,
                 mail_settings: Union[dict, None] = None,
                 mail_behavior: Union[dict, None] = None,
                 chrome_name: str = 'chromium-browser'):
        u"""Sets defaults"""

        logging.info('You are using exoskeleton 0.9.1 (beta / June 21, 2020)')

        self.project = project_name.strip()
        self.user_agent = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.db_host: Optional[str] = None
        self.db_port: Optional[int] = None
        self.db_name: str = None  # type: ignore
        self.db_username: str = None  # type: ignore
        self.db_passphrase: str = None  # type: ignore

        if database_settings is None:
            raise ValueError('You must supply database credentials for' +
                             'exoskeleton to work.')
        else:
            self.__check_database_settings(database_settings)

        # Establish the connection:
        self.connection = None
        self.establish_db_connection()
        # Add ignore for mypy as it cannot be None at this point, because
        # establish_db_connection would have failed before:
        self.cur = self.connection.cursor()  # type: ignore

        # Check the schema:
        self.__check_table_existence()
        self.__check_stored_procedures()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Mail / Notification Setup
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.send_mails: bool = False
        self.send_start_msg: bool = False
        self.send_finish_msg: bool = False
        self.milestone: Optional[int] = None
        self.mailer: Optional[bote.Mailer] = None

        if mail_settings is None:
            logging.info("Will not send any notification emails " +
                         "as there are no mail-settings.")
        else:
            self.mailer = bote.Mailer(mail_settings)
            # The constructur would have failed with exceptions,
            # if the settings were inplausible:
            self.send_mails = True
            logging.info('This bot will try to send notications per mail ' +
                         'in case it fails and cannot recover. ')

            if mail_settings and not mail_behavior:
                mail_behavior = dict()

            self.send_start_msg = mail_behavior.get('send_start_msg', True)  # type: ignore
            if not isinstance(self.send_start_msg, bool):
                raise ValueError('Value for send_start_msg must be boolean,' +
                                 'i.e True / False (without quotation marks).')
            if self.send_start_msg:
                self.mailer.send_mail(f"{self.project}: bot just started.",
                                      "This is a notification to check " +
                                      "the mail settings.")
                logging.info("Just send a notification email. If the " +
                             "receiving server uses greylisting, " +
                             "this may take some minutes.")

            self.send_finish_msg = mail_behavior.get('send_finish_msg', False)  # type: ignore
            if not isinstance(self.send_finish_msg, bool):
                raise ValueError('Value for send_finish_msg must be boolean,' +
                                 'i.e True / False (without quotation marks).')
            if self.send_finish_msg:
                logging.info('Will send notification email as soon as ' +
                             'the bot is done.')

            self.milestone = mail_behavior.get('milestone_num', None)  # type: ignore
            if self.milestone is not None:
                if not isinstance(self.milestone, int):
                    raise ValueError('milestone_num must be integer!')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Bot Behavior
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # Seconds until a connection times out:
        self.connection_timeout: int = 60

        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = 3
        # Time to wait after the queue is empty to check for new elements:
        self.queue_revisit: int = 60

        self.wait_min = None
        self.wait_max = None

        self.stop_if_queue_empty: bool = False

        if bot_behavior:
            self.__check_behavior_settings(bot_behavior)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # File Handling
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.target_dir = pathlib.Path.cwd()

        if target_directory is None or target_directory.strip() == '':
            logging.warning("Target directory is not set. " +
                            "Using the current working directory " +
                            "%s to store files!",
                            self.target_dir)
        else:
            # Assuming that if a directory was set, it has
            # to be used. Therefore no fallback to the current
            # working directory.
            self.target_dir = pathlib.Path(target_directory).resolve()
            if self.target_dir.is_dir():
                logging.debug("Set target directory to %s",
                              target_directory)
            else:
                raise OSError("Cannot find or access the user " +
                              "supplied target directory! " +
                              "Create this directory or check permissions.")

        self.file_prefix = filename_prefix.strip()
        # Limit the prefix length as on many systems the path must not be
        # longer than 255 characters and it needs space for folders and the
        # actual filename. 16 characters semms to be a reasonable limit.
        if len(self.file_prefix) > 16:
            raise ValueError('The file name prefix is limited to a ' +
                             'maximum of 16 characters.')

        self.hash_method = 'sha256'
        if not userprovided.hash.hash_available(self.hash_method):
            raise ValueError('The hash method SHA256 is not available on ' +
                             'your system.')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Init Timers
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.bot_start = time.monotonic()
        self.process_time_start = time.process_time()
        logging.debug('started timers')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Create Objects
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.cnt = Counter()  # type: Counter

        self.local_download_queue = queue.Queue()  # type: queue.Queue

        self.chrome_process = chrome_name.strip()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# SETUP
# Functions called from __init__ but outside of it for easier testing.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __check_database_settings(self,
                                  database_settings: dict):
        u"""Check the database settings for plausibility. """

        userprovided.parameters.validate_dict_keys(
            database_settings,
            {'host', 'port', 'database', 'username', 'passphrase'},
            {'database'},
            'database_settings')

        self.db_host = database_settings.get('host', None)
        if not self.db_host:
            logging.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port = database_settings.get('port', None)
        if not self.db_port:
            logging.info('No port number supplied. ' +
                         'Will try standard port instead.')
            self.db_port = 3306
        elif not userprovided.port.port_in_range(self.db_port):
            raise ValueError('Port outside valid range!')

        self.db_name = database_settings.get('database', None)
        if not self.db_name:
            raise ValueError('You must provide the name of the database.')

        self.db_username = database_settings.get('username', None)
        if not self.db_username:
            raise ValueError('You must provide a database user.')

        self.db_passphrase = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logging.warning('No database passphrase provided. ' +
                            'Will try to connect without.')

    def __check_behavior_settings(self,
                                  behavior_settings: dict):
        u"""Check the settings for bot behavior. """

        known_behavior_keys = {'connection_timeout',
                               'queue_max_retries',
                               'queue_revisit',
                               'stop_if_queue_empty',
                               'wait_min',
                               'wait_max'}
        if behavior_settings:
            userprovided.parameters.validate_dict_keys(
                behavior_settings,
                known_behavior_keys,
                None,
                'behavior_settings')
        else:
            behavior_settings = dict()

        self.connection_timeout = behavior_settings.get('connection_timeout',
                                                        60)
        if type(self.connection_timeout) != int:
            raise ValueError('The value for connection_timeout ' +
                             'must be numeric.')
        if self.connection_timeout <= 0:
            logging.error('Negative or zero value for timeout. ' +
                          'Fallback to 60 seconds.')
            self.connection_timeout = 60
        elif self.connection_timeout > 120:
            logging.info('Very high value for connection_timeout')

        self.wait_min = behavior_settings.get('wait_min', 5.0)
        if type(self.wait_min) not in (int, float):
            raise ValueError('The value for wait_min must be numeric.')
        self.wait_max = behavior_settings.get('wait_max', 30.0)
        if type(self.wait_max) not in (int, float):
            raise ValueError('The value for wait_max must be numeric.')

        # max retries NOT YET IMPLEMENTED:
        self.queue_max_retries = behavior_settings.get('queue_max_retries', 3)
        if type(self.queue_max_retries) != int:
            raise ValueError('The value for queue_max_retries ' +
                             'must be an integer.')

        self.queue_revisit = behavior_settings.get('queue_revisit', 60)
        try:
            self.queue_revisit = int(self.queue_revisit)
        except ValueError:
            raise ValueError('The value for queue_revisit must be numeric.')

        self.stop_if_queue_empty = behavior_settings.get(
            'stop_if_queue_empty',
            False)
        if type(self.stop_if_queue_empty) != bool:
            raise ValueError('The value for "stop_if_queue_empty" ' +
                             'must be a boolean (True / False).')

    def __check_table_existence(self) -> bool:
        u"""Check if all expected tables exist."""
        logging.debug('Checking if the database table structure is complete.')
        expected_tables = ['actions',
                           'errorType',
                           'fileContent',
                           'fileMaster',
                           'fileVersions',
                           'jobs',
                           'labels',
                           'labelToMaster',
                           'labelToVersion',
                           'queue',
                           'statisticsHosts',
                           'storageTypes']
        tables_count = 0

        self.cur.execute('SHOW TABLES;')
        tables = self.cur.fetchall()
        if not tables:
            logging.error('The database exists, but no tables found!')
            raise OSError('Database table structure missing. ' +
                          'Run generator script!')
        else:
            tables_found = [item[0] for item in tables]
            for table in expected_tables:
                if table in tables_found:
                    tables_count += 1
                    logging.debug('Found table %s', table)
                else:
                    logging.error('Table %s not found.', table)

        if tables_count != len(expected_tables):
            raise RuntimeError('Database Schema Incomplete: Missing Tables!')

        logging.info("Found all expected tables.")
        return True

    def __check_stored_procedures(self) -> bool:
        u"""Check if all expected stored procedures exist and if the user
        is allowed to execute them. """
        logging.debug('Checking if stored procedures exist.')
        expected_procedures = ['delete_all_versions_SP',
                               'delete_from_queue_SP',
                               'insert_content_SP',
                               'insert_file_SP',
                               'next_queue_object_SP']

        procedures_count = 0
        self.cur.execute('SELECT SPECIFIC_NAME ' +
                         'FROM INFORMATION_SCHEMA.ROUTINES ' +
                         'WHERE ROUTINE_SCHEMA = %s;',
                         self.db_name)
        procedures = self.cur.fetchall()
        procedures_found = [item[0] for item in procedures]
        for procedure in expected_procedures:
            if procedure in procedures_found:
                procedures_count += 1
                logging.debug('Found stored procedure %s', procedure)
            else:
                logging.error('Stored Procedure %s is missing (create it ' +
                              'with the database script) or the user lacks ' +
                              'permissions to use it.', procedure)

        if procedures_count != len(expected_procedures):
            raise RuntimeError('Database Schema Incomplete: ' +
                               'Missing Stored Procedures!')

        logging.info("Found all expected stored procedures.")
        return True

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ACTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __get_object(self,
                     queue_id: str,
                     action_type: str,
                     url: str,
                     url_hash: str,
                     prettify_html: bool = False):
        u""" Generic function to either download a file or
             store a page's content. """
        # pylint: disable=too-many-branches
        if not isinstance(queue_id, str):
            raise ValueError('The queue_id must be a string.')
        if action_type not in ('file', 'content'):
            raise ValueError('Invalid action')
        if url == '' or url is None:
            raise ValueError('Missing url')
        url = url.strip()
        if url_hash == '' or url_hash is None:
            raise ValueError('Missing url_hash')

        if action_type != 'content' and prettify_html:
            logging.error('Parameter prettify_html ignored ' +
                          'because of wrong action_type.')

        try:
            if action_type == 'file':
                logging.debug('starting download of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": str(self.user_agent)},
                                 timeout=self.connection_timeout,
                                 stream=True)
            elif action_type == 'content':
                logging.debug('retrieving content of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": str(self.user_agent)},
                                 timeout=self.connection_timeout,
                                 stream=False
                                 )

            if r.status_code == 200:
                mime_type = ''
                if r.headers.get('content-type') is not None:
                    mime_type = (r.headers.get('content-type')).split(';')[0]  # type: ignore

                if action_type == 'file':
                    extension = utils.determine_file_extension(url, mime_type)
                    new_filename = f"{self.file_prefix}{queue_id}{extension}"
                    target_path = self.target_dir.joinpath(new_filename)

                    with open(target_path, 'wb') as file_handle:
                        for block in r.iter_content(1024):
                            file_handle.write(block)
                        logging.debug('file written')
                        hash_value = None
                        if self.hash_method:
                            hash_value = utils.get_file_hash(target_path,
                                                             self.hash_method)

                    logging.debug('file written to disk')
                    try:
                        self.cur.callproc('insert_file_SP',
                                          (url, url_hash, queue_id, mime_type,
                                           str(self.target_dir), new_filename,
                                           utils.get_file_size(target_path),
                                           self.hash_method, hash_value))
                    except pymysql.DatabaseError:
                        self.cnt['transaction_fail'] += 1
                        logging.error('Transaction failed: Could not add ' +
                                      'already downloaded file %s to the ' +
                                      'database!', new_filename)

                elif action_type == 'content':

                    detected_encoding = str(r.encoding)
                    logging.debug('detected encoding: %s', detected_encoding)

                    page_content = r.text

                    if mime_type == 'text/html' and prettify_html:
                        page_content = utils.prettify_html(page_content)

                    try:
                        # Stored procedure saves the content,
                        # transfers the labels from the queue,
                        # and removes the queue item:
                        self.cur.callproc('insert_content_SP',
                                          (url, url_hash, queue_id,
                                           mime_type, page_content))
                    except pymysql.DatabaseError:
                        self.cnt['transaction_fail'] += 1
                        logging.error('Transaction failed: Could not add ' +
                                      'page code of queue item %s to ' +
                                      'the database!',
                                      queue_id, exc_info=True)

                self.cnt['processed'] += 1
                self.__update_host_statistics(url, True)

            if r.status_code in (402, 403, 404, 405, 410, 451):
                self.mark_error(queue_id, r.status_code)
                self.__update_host_statistics(url, False)
            elif r.status_code == 429:
                logging.error('The bot hit a rate limit! It queries too ' +
                              'fast => increase min_wait.')
                self.add_crawl_delay_to_item(queue_id)
                self.__update_host_statistics(url, False)
            elif r.status_code not in (200, 402, 403, 404, 405, 410, 429, 451):
                logging.error('Unhandeled return code %s', r.status_code)
                self.__update_host_statistics(url, False)

        except TimeoutError:
            logging.error('Reached timeout.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queue_id)
            self.__update_host_statistics(url, False)

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.__update_host_statistics(url, False)
            raise

        except urllib3.exceptions.NewConnectionError:
            logging.error('New Connection Error: might be a rate limit',
                          exc_info=True)
            self.__update_host_statistics(url, False)
            # Suppressing mypy errors as defaultdict and some checks
            # makes ensure self.wait_min and self.wait_max will be
            # float or int:
            if self.wait_min < 10.0:  # type: ignore
                self.wait_min = self.wait_min + 1.0  # type: ignore
                self.wait_max = self.wait_max + 1.0  # type: ignore
                logging.info('Increased min and max wait by 1 second each.')

        except requests.exceptions.MissingSchema:
            logging.error('Missing Schema Exception. Does your URL contain ' +
                          'the protocol i.e. http:// or https:// ? ' +
                          'See queue_id = %s', queue_id)
            self.mark_error(queue_id, 1)

        except Exception:
            logging.error('Unknown exception while trying ' +
                          'to download.', exc_info=True)
            self.__update_host_statistics(url, False)
            raise

    def __page_to_pdf(self,
                      url: str,
                      url_hash: str,
                      queue_id: str):
        u""" Uses the Google Chrome or Chromium browser in headless mode
        to print the page to PDF and stores that.
        Beware: some cookie-popups blank out the page and
        all what is stored is the dialogue."""
        # Using headless Chrome instead of Selenium for the following
        # reasons:
        # * The user does not need to install and update a version of
        #   chromedriver matching the browser.
        # * It is faster.
        # * It does not open a GUI.
        # * Selenium has no built in command for PDF export, but needs
        #   to operate the dialog. That is far more likely to break.

        if self.chrome_process is None:
            raise ValueError('You must provide the name of the Chrome ' +
                             'process to use this.')
        filename = f"{self.file_prefix}{queue_id}.pdf"
        path = self.target_dir.joinpath(filename)

        try:
            # Using the subprocess module as it is part of the
            # standard library and set up to replace os.system
            # and os.spawn!
            subprocess.run([self.chrome_process,
                            "--headless",
                            "--new-windows",
                            "--disable-gpu",
                            "--account-consistency",
                            # No additional quotation marks around the path:
                            # subprocess does the necessary escaping!
                            f"--print-to-pdf={path}",
                            url],
                           shell=False,
                           timeout=30,
                           check=True)

            hash_value = None
            if self.hash_method:
                hash_value = utils.get_file_hash(path,
                                                 self.hash_method)
            logging.debug('PDF of page saved to disk')
            try:
                self.cur.callproc('insert_file_SP',
                                  (url, url_hash, queue_id, 'application/pdf',
                                   str(self.target_dir), filename,
                                   utils.get_file_size(path),
                                   self.hash_method, hash_value))
            except pymysql.DatabaseError:
                self.cnt['transaction_fail'] += 1
                logging.error('Transaction failed: Could not add already ' +
                              'downloaded file %s to the database!',
                              filename, exc_info=True)
            except Exception:
                logging.error('Unknown exception', exc_info=True)
            self.cnt['processed'] += 1
            self.__update_host_statistics(url, True)
        except subprocess.TimeoutExpired:
            logging.error('Cannot create PDF due to timeout.')
            self.add_crawl_delay_to_item(queue_id)
            self.__update_host_statistics(url, False)
        except subprocess.CalledProcessError:
            logging.error('Cannot create PDF due to process error.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queue_id)
            self.__update_host_statistics(url, False)
        except Exception:
            logging.error('Exception.',
                          exc_info=True)
            self.add_crawl_delay_to_item(queue_id)
            self.__update_host_statistics(url, False)
            pass

    def return_page_code(self,
                         url: str):
        u"""Directly return a page's code and do *not* store it
        in the database. """
        if url == '' or url is None:
            raise ValueError('Missing url')
        url = url.strip()

        try:
            r = requests.get(url,
                             headers={"User-agent": str(self.user_agent)},
                             timeout=self.connection_timeout,
                             stream=False
                             )

            if r.status_code == 200:
                return r.text
            else:
                raise RuntimeError('Cannot return page code')

        except TimeoutError:
            logging.error('Reached timeout.', exc_info=True)
            self.__update_host_statistics(url, False)
            raise

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.__update_host_statistics(url, False)
            raise

        except Exception:
            logging.exception('Exception while trying to get page-code',
                              exc_info=True)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DATABASE MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def establish_db_connection(self):
        u"""Establish a connection to MariaDB """
        try:
            logging.debug('Trying to connect to database.')
            self.connection = pymysql.connect(host=self.db_host,
                                              port=self.db_port,
                                              database=self.db_name,
                                              user=self.db_username,
                                              password=self.db_passphrase,
                                              autocommit=True)

            logging.info('Established database connection.')

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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# JOB MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def job_define_new(self,
                       job_name: str,
                       start_url: str):
        u""" Create a new crawl job identified by it name and an url
        to start crawling. """
        # no check for None or '' here as it is a required argument

        job_name = job_name.strip()
        # the job_name may have consisted only of whitespace_
        if job_name == '':
            raise ValueError('Provide a valid job_name')

        if len(job_name) > 127:
            raise ValueError('Invalid job name: maximum 127 characters.')

        if start_url == '' or start_url is None:
            raise ValueError

        try:
            self.cur.execute('INSERT INTO jobs ' +
                             '(jobName, startUrl, startUrlHash) ' +
                             'VALUES (%s, %s, SHA2(%s,256));',
                             (job_name, start_url, start_url))
            logging.debug('Defined new job.')
        except pymysql.IntegrityError:
            # A job with this name already exists
            # Check if startURL is the same:
            self.cur.execute('SELECT startURL FROM jobs WHERE jobName = %s;',
                             job_name)
            existing_start_url = self.cur.fetchone()[0]
            if existing_start_url == start_url:
                logging.warning('A job with identical name and startURL ' +
                                'is already defined.')
            else:
                raise ValueError('A job with the identical name but ' +
                                 '*different* startURL is already defined!')

    def job_update_current_url(self,
                               job_name: str,
                               current_url: str):
        u""" Set the currentUrl for a specific job. """

        if job_name == '' or job_name is None:
            raise ValueError('Provide the job name.')
        if current_url == '' or current_url is None:
            raise ValueError('Current URL must not be empty.')

        affected_rows = self.cur.execute('UPDATE jobs ' +
                                         'SET currentURL = %s ' +
                                         'WHERE jobName = %s;',
                                         (current_url, job_name))
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')

    def job_get_current_url(self,
                            job_name: str) -> str:
        u""" Returns the current URl for this job. If none is stored, this
             returns the start URL. Raises exception if the job is already
             finished."""

        self.cur.execute('SELECT finished FROM jobs ' +
                         'WHERE jobName = %s;',
                         job_name)
        job_state = self.cur.fetchone()
        # If the job does not exist at all, then MariaDB returns None.
        # If the job exists, but the finished field has a value of NULL,
        # then MariaDB returns (None,)
        try:
            job_state = job_state[0]
        except TypeError:
            # Occurs if the the result was None, i.e. the job
            # does not exist.
            raise ValueError('Job is unknown!')

        if job_state is not None:
            # i.e. the finished field is not empty
            raise RuntimeError(f"Job already finished at {job_state}.")

        # The job exists and is not finished. So return the currentUrl,
        # or - in case that is not defined - the startUrl value.
        self.cur.execute('SELECT COALESCE(currentUrl, startUrl) ' +
                         'FROM jobs ' +
                         'WHERE jobName = %s;',
                         job_name)
        return self.cur.fetchone()[0]

    def job_mark_as_finished(self,
                             job_name: str):
        u""" Mark a crawl job as finished. """
        if job_name == '' or job_name is None:
            raise ValueError
        job_name = job_name.strip()
        affected_rows = self.cur.execute('UPDATE jobs SET ' +
                                         'finished = CURRENT_TIMESTAMP() ' +
                                         'WHERE jobName = %s;',
                                         job_name)
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')
        logging.debug('Marked job %s as finished.', job_name)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# QUEUE MANAGEMENT
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def random_wait(self):
        u"""Waits for a random time between actions
        (within the interval preset at initialization).
        This is done to avoid to accidentially overload
        the queried host. Some host actually enforce
        limits through IP blocking."""
        query_delay = random.randint(self.wait_min, self.wait_max)  # nosec
        logging.debug("%s seconds delay until next action", query_delay)
        time.sleep(query_delay)
        return

    def num_items_in_queue(self) -> int:
        u"""Number of items left in the queue. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        return int(self.cur.fetchone()[0])  # type: ignore

    def absolute_run_time(self) -> float:
        u"""Return seconds since init. """
        return time.monotonic() - self.bot_start

    def get_process_time(self) -> float:
        u"""Return execution time since init"""
        return time.process_time() - self.process_time_start

    def estimate_remaining_time(self) -> int:
        u"""Estimate remaining seconds to finish crawl."""
        time_so_far = self.absolute_run_time()
        num_remaining = self.num_items_in_queue()

        if self.cnt['processed'] > 0:
            time_each = time_so_far / self.cnt['processed']
            return round(num_remaining * time_each)

        logging.warning('Cannot estimate remaining time ' +
                        'as there are no data so far.')
        return -1

    def add_file_download(self,
                          url: str,
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False):
        u"""Add a file download URL to the queue """
        self.__add_to_queue(url, 1, labels_master,
                            labels_version, False,
                            force_new_version)

    def add_save_page_code(self,
                           url: str,
                           labels_master: set = None,
                           labels_version: set = None,
                           prettify_html: bool = False,
                           force_new_version: bool = False):
        u"""Add an URL to the queue to save it's HTML code
            into the database."""
        self.__add_to_queue(url, 2, labels_master,
                            labels_version, prettify_html,
                            force_new_version)

    def add_page_to_pdf(self,
                        url: str,
                        labels_master: set = None,
                        labels_version: set = None,
                        force_new_version: bool = False):
        u"""Add an URL to the queue to print it to PDF
            with headless Chrome. """
        self.__add_to_queue(url, 3, labels_master, labels_version,
                            False, force_new_version)

    def __add_to_queue(self,
                       url: str,
                       action: int,
                       labels_master: set = None,
                       labels_version: set = None,
                       prettify_html: bool = False,
                       force_new_version: bool = False):
        u""" More general function to add items to queue. Called by
        add_file_download, add_save_page_code and add_page_to_pdf."""

        if action not in (1, 2, 3):
            logging.error('Invalid value for action!')
            return

        prettify = 0  # numeric because will be added to int database field
        if prettify_html and action != 2:
            logging.error('Option prettify_html not ' +
                          'supported for this action.')
        elif prettify_html:
            prettify = 1

        # Excess whitespace might be common (copy and paste)
        # and would change the hash:
        url = url.strip()
        # check if it is an URL and if it is either http or https
        # (other schemas are not supported by requests)
        if not userprovided.url.is_url(url, ('http', 'https')):
            logging.error('Could not add URL %s : invalid or unsupported',
                          url)
            return

        # Add labels for the master entry.
        # Ignore labels for the version at this point, as it might
        # not get processed.
        if labels_master:
            self.__assign_labels_to_master(url, labels_master)

        if not force_new_version:
            # check if the URL has already been processed
            self.cur.execute('SELECT id FROM fileMaster ' +
                             'WHERE urlHash = SHA2(%s,256);',
                             url)
            id_in_file_master = self.cur.fetchone()

            if id_in_file_master:
                print('ID ist in file master')
                # The URL has been processed in _some_ way.
                # Check if was the _same_ as now requested.
                self.cur.execute('SELECT id FROM fileVersions ' +
                                 'WHERE fileMasterID = %s AND ' +
                                 'storageTypeID = %s;',
                                 (id_in_file_master[0], action))
                version_id = self.cur.fetchone()
                if version_id:
                    logging.info('The file has already been processed ' +
                                 'in the same way. Skipping it.')
                    return
                else:
                    # log and simply go on
                    logging.debug('The file has already been processed, ' +
                                  'BUT not in this way. Therefore ' +
                                  'adding task to the queue.')
            else:
                # File has not been processed yet.
                # If the exact same task is *not* already in the queue,
                # add it.
                self.cur.execute('SELECT id FROM queue ' +
                                 'WHERE urlHash = SHA2(%s,256) AND ' +
                                 'action = %s;',
                                 (url, action))
                in_queue = self.cur.fetchone()
                if in_queue:
                    logging.info('Exact same task already in queue.')
                    return

        # generate a random uuid for the file version
        uuid_value = uuid.uuid4().hex

        # add the new task to the queue
        self.cur.execute('INSERT INTO queue ' +
                         '(id, action, url, urlHash, prettifyHtml) ' +
                         'VALUES (%s, %s, %s, SHA2(%s,256), %s);',
                         (uuid_value, action, url, url, prettify))

        # link labels to version item
        if labels_version:
            self.__assign_labels_to_version(uuid_value, labels_version)

    def delete_from_queue(self,
                          queue_id: int):
        u"""Remove all label links from a queue item
            and then delete it from the queue."""
        # callproc expects a tuple. Do not remove the comma:
        self.cur.callproc('delete_from_queue_SP', (queue_id, ))

    def add_crawl_delay_to_item(self,
                                queue_id: str,
                                delay_seconds: Optional[int] = None):
        u"""In case of timeout or temporary error add a delay until
        the same URL is queried again. """
        logging.debug('Adding crawl delay to queue item %s', queue_id)
        waittime = 900  # 15 minutes default
        if delay_seconds:
            waittime = delay_seconds

        self.cur.execute('UPDATE queue ' +
                         'SET delayUntil = ADDTIME(NOW(), %s) ' +
                         'WHERE id = %s', (waittime, queue_id))

    def mark_error(self,
                   queue_id: str,
                   error: int):
        u""" Mark item in queue that causes permant error.

        Has to be marked as otherwise exoskelton will try to
        download it over and over again."""

        self.cur.execute('UPDATE queue ' +
                         'SET causesError = %s ' +
                         'WHERE id = %s;', (error, queue_id))
        logging.debug('Marked queue-item that caused a problem.')
        if error in (429, 500, 503):
            self.add_crawl_delay_to_item(queue_id, 600)

    def __get_next_task(self):
        u""" get the next suitable task"""
        self.cur.execute('CALL next_queue_object_SP();')
        return self.cur.fetchone()

    def process_queue(self):
        u"""Process the queue"""
        while True:
            try:
                next_in_queue = self.__get_next_task()
            except pymysql.err.OperationalError as e:
                if e.args[0] == 2013:  # errno
                    logging.error('Lost connection to database server. ' +
                                  'Trying to restore it...')
                    # this error is unusual. Give the db some time:
                    time.sleep(10)
                    try:
                        self.establish_db_connection()
                        self.cur = self.connection.cursor()
                        next_in_queue = self.__get_next_task()
                        logging.info('Succesfully restored connection ' +
                                     'to database server!')
                    except Exception:
                        logging.error('Could not reestablish database ' +
                                      'server connection!', exc_info=True)
                        if self.send_mails:
                            subject = f"{self.project}: bot ABORTED"
                            content = ("The bot lost the database " +
                                       "connection and could not restore it.")
                            self.mailer.send_mail(subject, content)
                        raise ConnectionError('Lost DB connection and ' +
                                              'could not restore it.')
                else:
                    logging.error('Unexpected Operational Error',
                                  exc_info=True)
                    raise

            if next_in_queue is None:
                # no actionable item in the queue
                if self.stop_if_queue_empty:
                    # Bot is configured to stop if queue is empty
                    # => check if that is omnly temporary or everything is done
                    self.cur.execute('SELECT num_items_with_temporary_errors();')
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

                        self.cur.execute('SELECT num_items_with_permanent_error();')
                        num_permanent_errors = self.cur.fetchone()[0]
                        if num_permanent_errors > 0:
                            logging.error("%s permanent errors!",
                                          num_permanent_errors)

                    if self.send_mails:
                        subject = f"{self.project}: queue empty / bot stopped"
                        content = (f"The queue is empty. The bot " +
                                   f"{self.project} stopped as configured. " +
                                   f"{num_permanent_errors} errors.")
                        self.mailer.send_mail(subject, content)
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
                prettify_html = 1 if next_in_queue[4] == 1 else 0

                if action == 1:
                    # download file to disk
                    self.__get_object(queue_id, 'file', url, url_hash)
                elif action == 2:
                    # save page code into database
                    self.__get_object(queue_id, 'content',
                                      url, url_hash,
                                      prettify_html)
                elif action == 3:
                    # headless Chrome to create PDF
                    self.__page_to_pdf(url, url_hash, queue_id)
                else:
                    logging.error('Unknown action id!')

                if self.milestone:
                    self.check_milestone()

                # wait some interval to avoid overloading the server
                self.random_wait()

    def __update_host_statistics(self,
                                 url: str,
                                 success: bool = True):
        u""" Updates the host based statistics. The URL
        gets shortened to the hostname. If success is True
        the success counter is incremented / if not the
        problem counter."""

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
        u""" Check if milestone is reached. If that is the case,
        send a mail (if configured to do so)."""
        processed = self.cnt['processed']
        # Have to check >0 in case the bot starts
        # failing with the first item.
        if processed > 0 and (processed % self.milestone) == 0:
            logging.info("Milestone reached: %s processed",
                         str(processed))
            if self.send_mails:
                subject = (f"{self.project} Milestone reached: " +
                           f"{self.cnt['processed']} processed")
                content = (f"{self.cnt['processed']} processed.\n" +
                           f"{self.num_items_in_queue()} items " +
                           f"remaining in the queue.\n" +
                           f"Estimated time to complete queue: " +
                           f"{self.estimate_remaining_time()} seconds.\n")
                self.mailer.send_mail(subject, content)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# LABELS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    def define_or_update_label(self,
                               shortname: str,
                               description: str = None):
        u""" Insert a new label into the database or update it's
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

    def get_label_ids(self,
                      label_set: Union[set, str]):
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

            return None if label_id is None else label_id
        logging.error('No labels provided to get_label_ids().')
        return None

    def version_uuids_by_label(self,
                               single_label: str) -> Optional[List[str]]:
        u"""Get a list of UUIDs (in this context file versions)
            which have *one* specific label attached to them."""
        label_id = self.get_label_ids(single_label)
        if label_id:
            label_id = label_id[0]
        else:
            logging.error('Unknown label. Check for typo.')
        self.cur.execute("SELECT versionUUID " +
                         "FROM labelToVersion " +
                         "WHERE labelID = %s;",
                         label_id)
        version_ids = self.cur.fetchall()
        version_ids = [(uuid[0]) for uuid in version_ids]
        return None if version_ids is None else version_ids

    def version_labels_by_uuid(self,
                               version_uuid: str) -> list:
        u"""Get a list of label names (not id numbers!) attached
            to a specific version of a file. Does not include
            labels attached to the filemaster entry."""
        self.cur.execute('SELECT shortName ' +
                         'FROM labels ' +
                         'WHERE ID IN (' +
                         '  SELECT labelID ' +
                         '  FROM labelToVersion ' +
                         '  WHERE versionUUID = %s' +
                         ');',
                         version_uuid)
        labels = self.cur.fetchall()
        labels = [(label[0]) for label in labels]
        return labels

    def __assign_labels_to_version(self,
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

            # Check if there are already labels assigned
            self.cur.execute('SELECT labelID FROM labelToVersion ' +
                             'WHERE versionUUID = %s;', uuid)
            ids_associated = self.cur.fetchall()

            # ignore all labels already associated:
            id_list = tuple(set(id_list) - set(ids_associated))

            if id_list:
                # Case: there are new labels
                # Convert into a format to INSERT with executemany
                insert_list = [(id[0], uuid) for id in id_list]
                self.cur.executemany('INSERT IGNORE INTO labelToVersion ' +
                                     '(labelID, versionUUID) ' +
                                     'VALUES (%s, %s);', insert_list)

    def __assign_labels_to_master(self,
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
            self.cur.execute('SELECT labelID FROM labelToMaster ' +
                             'WHERE urlHash = SHA2(%s,256);', url)
            ids_associated = self.cur.fetchall()

            # ignore all labels already associated
            id_list = tuple(set(id_list) - set(ids_associated))

            if id_list:
                # Case: there are new labels
                # Convert into a format to INSERT with executemany
                insert_list = [(id[0], url) for id in id_list]
                # Add those associatons
                self.cur.executemany('INSERT IGNORE INTO labelToMaster ' +
                                     '(labelID, urlHash) ' +
                                     'VALUES (%s, SHA2(%s,256));', insert_list)
