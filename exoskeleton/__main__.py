#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
A Python framework to build a basic crawler / scraper
with a MariaDB backend.
"""

# python standard library:
from collections import Counter
# noinspection PyUnresolvedReferences
from collections import defaultdict
import logging
import pathlib
import subprocess
import time
from typing import Union, Optional
from urllib.parse import urlparse
import uuid

# 3rd party libraries:
import pymysql
from requests.models import Response
import urllib3  # type: ignore
import requests
# Sister projects:
import userprovided
import bote

# import other modules of this framework
import exoskeleton.database_check as db_check
import exoskeleton.utils as utils
from .TimeManager import TimeManager


class Exoskeleton:
    u""" Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint3 to complain a lot.
    # As the complexity is needed, disable some warnings:
    # pylint: disable=too-many-statements
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

        logging.info('You are using exoskeleton 0.9.3 (beta / July 10, 2020)')

        self.project: str = project_name.strip()
        self.user_agent: str = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        if database_settings is None:
            raise ValueError('You must supply database credentials for' +
                             'exoskeleton to work.')

        userprovided.parameters.validate_dict_keys(
            database_settings,
            {'host', 'port', 'database', 'username', 'passphrase'},
            {'database'},
            'database_settings')

        self.db_host: str = database_settings.get('host', None)
        if not self.db_host:
            logging.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port: int = database_settings.get('port', None)
        if not self.db_port:
            logging.info('No port number supplied. ' +
                         'Will try standard port 3306 instead.')
            self.db_port = 3306
        elif not userprovided.port.port_in_range(self.db_port):
            raise ValueError('Port outside valid range!')

        self.db_name: str = database_settings.get('database', None)
        if not self.db_name:
            raise ValueError('You must provide the name of the database.')

        self.db_username: str = database_settings.get('username', None)
        if not self.db_username:
            raise ValueError('You must provide a database user.')

        self.db_passphrase: str = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logging.warning('No database passphrase provided. ' +
                            'Will try to connect without.')

        # Establish the connection:
        self.connection = None
        self.establish_db_connection()
        # Add ignore for mypy as it cannot be None at this point, because
        # establish_db_connection would have failed before:
        self.cur = self.connection.cursor()  # type: ignore

        # Check the schema:
        db_check.check_table_existence(self.cur)
        db_check.check_stored_procedures(self.cur, self.db_name)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Mail / Notification Setup
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.send_mails: bool = False
        self.send_start_msg: bool = False
        self.send_finish_msg: bool = False
        self.milestone: Optional[int] = None
        self.mailer: Optional[bote.Mailer] = None

        # make sure mail-behavior exists for defaultdict to work
        mail_behavior = dict() if not mail_behavior else mail_behavior

        if mail_settings is None:
            logging.info("Will not send any notification emails " +
                         "as there are no mail-settings.")
        else:
            self.mailer = bote.Mailer(mail_settings)
            # The constructor would have failed with exceptions,
            # if the settings were implausible:
            self.send_mails = True
            logging.info('This bot will try to send notifications via mail ' +
                         'in case it fails and cannot recover. ')

            self.send_start_msg = mail_behavior.get('send_start_msg', True)
            if not isinstance(self.send_start_msg, bool):
                raise ValueError('Value for send_start_msg must be boolean,' +
                                 'i.e True / False (without quotation marks).')
            if self.send_start_msg:
                self.mailer.send_mail(f"{self.project}: bot just started.",
                                      "This is a notification to check " +
                                      "the mail settings.")
                logging.info("Just sent a notification email. If the " +
                             "receiving server uses greylisting, " +
                             "this may take some minutes.")

            self.send_finish_msg = mail_behavior.get('send_finish_msg', False)
            if not isinstance(self.send_finish_msg, bool):
                raise ValueError('Value for send_finish_msg must be boolean,' +
                                 'i.e True / False (without quotation marks).')
            if self.send_finish_msg:
                logging.info('Will send notification email as soon as ' +
                             'the bot is done.')

            self.milestone = mail_behavior.get('milestone_num', None)
            if self.milestone is not None:
                if not isinstance(self.milestone, int):
                    raise ValueError('milestone_num must be integer!')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Bot Behavior
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        known_behavior_keys = {'connection_timeout',
                               'queue_max_retries',
                               'queue_revisit',
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

        self.wait_min: int = bot_behavior.get('wait_min', 5)
        self.wait_max: int = bot_behavior.get('wait_max', 30)

        # max retries NOT YET IMPLEMENTED:
        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = bot_behavior.get('queue_max_retries', 3)
        self.queue_max_retries = userprovided.parameters.int_in_range(
            "queue_max_retries", self.queue_max_retries, 0, 10, 3)

        # Time to wait after the queue is empty to check for new elements:
        self.queue_revisit: int = bot_behavior.get('queue_revisit', 50)
        self.queue_revisit = userprovided.parameters.int_in_range(
            "queue_revisit", self.queue_revisit, 10, 50, 50)

        self.stop_if_queue_empty: bool = bot_behavior.get(
            'stop_if_queue_empty', False)
        if type(self.stop_if_queue_empty) != bool:
            raise ValueError('The value for "stop_if_queue_empty" ' +
                             'must be a boolean (True / False).')

        # Init time management
        self.tm = TimeManager(self.wait_min, self.wait_max)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: File Handling
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
        # actual filename. 16 characters seems to be a reasonable limit.
        if len(self.file_prefix) > 16:
            raise ValueError('The file name prefix is limited to a ' +
                             'maximum of 16 characters.')

        self.hash_method = 'sha256'
        if not userprovided.hash.hash_available(self.hash_method):
            raise ValueError('The hash method SHA256 is not available on ' +
                             'your system.')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Create Objects
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        self.cnt: Counter = Counter()

        self.chrome_process = chrome_name.strip()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ACTIONS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # make random_wait() accessible from outside
    def random_wait(self):
        self.tm.random_wait()

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
            logging.error('Wrong action_type: prettify_html ignored.')

        r: Response = Response()
        try:
            if action_type == 'file':
                logging.debug('starting download of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": self.user_agent},
                                 timeout=self.connection_timeout,
                                 stream=True)
            elif action_type == 'content':
                logging.debug('retrieving content of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": self.user_agent},
                                 timeout=self.connection_timeout,
                                 stream=False
                                 )

            if r.status_code == 200:
                mime_type = ''
                if r.headers.get('content-type') is not None:
                    mime_type = (r.headers.get('content-type')).split(';')[0]  # type: ignore

                if action_type == 'file':
                    extension = userprovided.url.determine_file_extension(
                        url, mime_type)
                    new_filename = f"{self.file_prefix}{queue_id}{extension}"
                    target_path = self.target_dir.joinpath(new_filename)

                    with open(target_path, 'wb') as file_handle:
                        for block in r.iter_content(1024):
                            file_handle.write(block)
                        logging.debug('file written')
                        hash_value = None
                        if self.hash_method:
                            hash_value = userprovided.hash.calculate_file_hash(
                                target_path,
                                self.hash_method)

                    logging.debug('file written to disk')
                    try:
                        self.cur.callproc('insert_file_SP',
                                          (url, url_hash, queue_id, mime_type,
                                           str(self.target_dir), new_filename,
                                           utils.get_file_size(target_path),
                                           self.hash_method, hash_value, 1))
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
                                           mime_type, page_content, 2))
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
                logging.error('Unhandled return code %s', r.status_code)
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
            self.tm.increase_wait()

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
                hash_value = userprovided.hash.calculate_file_hash(
                    path, self.hash_method)
            logging.debug('PDF of page saved to disk')
            try:
                self.cur.callproc('insert_file_SP',
                                  (url, url_hash, queue_id, 'application/pdf',
                                   str(self.target_dir), filename,
                                   utils.get_file_size(path),
                                   self.hash_method, hash_value, 3))
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

    def num_items_in_queue(self) -> int:
        u"""Number of items left in the queue. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        return int(self.cur.fetchone()[0])

    def add_file_download(self,
                          url: str,
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False) -> Optional[str]:
        u"""Add a file download URL to the queue """
        uuid = self.__add_to_queue(url, 1, labels_master,
                                   labels_version, False,
                                   force_new_version)
        return uuid

    def add_save_page_code(self,
                           url: str,
                           labels_master: set = None,
                           labels_version: set = None,
                           prettify_html: bool = False,
                           force_new_version: bool = False) -> Optional[str]:
        u"""Add an URL to the queue to save it's HTML code
            into the database."""
        uuid = self.__add_to_queue(url, 2, labels_master,
                                   labels_version, prettify_html,
                                   force_new_version)
        return uuid

    def add_page_to_pdf(self,
                        url: str,
                        labels_master: set = None,
                        labels_version: set = None,
                        force_new_version: bool = False) -> Optional[str]:
        u"""Add an URL to the queue to print it to PDF
            with headless Chrome. """
        uuid = self.__add_to_queue(url, 3, labels_master,
                                   labels_version, False,
                                   force_new_version)
        return uuid

    def __add_to_queue(self,
                       url: str,
                       action: int,
                       labels_master: set = None,
                       labels_version: set = None,
                       prettify_html: bool = False,
                       force_new_version: bool = False) -> Optional[str]:
        u""" More general function to add items to queue. Called by
        add_file_download, add_save_page_code and add_page_to_pdf."""

        if action not in (1, 2, 3):
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
                if self.get_queue_id(url, action):
                    logging.info('Exact same task already in queue.')
                    return None

        # generate a random uuid for the file version
        uuid_value = uuid.uuid4().hex

        # add the new task to the queue
        self.cur.execute('INSERT INTO queue ' +
                         '(id, action, url, urlHash, prettifyHtml) ' +
                         'VALUES (%s, %s, %s, SHA2(%s,256), %s);',
                         (uuid_value, action, url, url, prettify_html))

        # link labels to version item
        if labels_version:
            self.assign_labels_to_uuid(uuid_value, labels_version)

        return uuid_value

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        self.cur.execute('SELECT id FROM fileMaster ' +
                         'WHERE urlHash = SHA2(%s,256);',
                         url)
        id_in_file_master = self.cur.fetchone()
        if id_in_file_master:
            return id_in_file_master[0]
        else:
            return None

    def get_queue_id(self,
                     url: str,
                     action: int) -> Optional[str]:
        u"""Get the id in the queue based on the URL and action ID.
        Returns None if such combination is not in the queue."""
        self.cur.execute('SELECT id FROM queue ' +
                         'WHERE urlHash = SHA2(%s,256) AND ' +
                         'action = %s;',
                         (url, action))
        uuid = self.cur.fetchone()
        if uuid:
            return uuid[0]
        else:
            return None

    def delete_from_queue(self,
                          queue_id: int):
        u"""Remove all label links from a queue item
            and then delete it from the queue."""
        # callproc expects a tuple. Do not remove the comma:
        self.cur.callproc('delete_from_queue_SP', (queue_id,))

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
        u""" Get the next suitable task"""
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
                prettify_html = True if next_in_queue[4] == 1 else False

                # The FQDN might have been added to the blocklist *after*
                # the task entered into the queue:
                if self.check_blocklist(urlparse(url).hostname):
                    logging.error('Cannot process queue item as the ' +
                                  'the FQDN has meanwhile been added to ' +
                                  'the blocklist!')
                    self.delete_from_queue(queue_id)
                    logging.info('Removed item fron queue: FQDN on blocklist.')
                else:
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
                    self.tm.random_wait()

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
                estimate = self.tm.estimate_remaining_time(
                    self.cnt['processed'],
                    self.num_items_in_queue()
                )
                subject = (f"{self.project} Milestone reached: " +
                           f"{self.cnt['processed']} processed")
                content = (f"{self.cnt['processed']} processed.\n" +
                           f"{self.num_items_in_queue()} items " +
                           f"remaining in the queue.\n" +
                           f"Estimated time to complete queue: " +
                           f"{estimate} seconds.\n")
                self.mailer.send_mail(subject, content)

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
        self.cur.execute('TRUNCATE TABLE blockList;')
        pass

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

    def version_uuids_by_label(self,
                               single_label: str,
                               processed_only: bool = False) -> set:
        u"""Get a list of UUIDs (in this context file versions)
            which have *one* specific label attached to them.
            If processed_only is set to True only UUIDs of
            already downloaded items are returned.
            Otherwise it contains queue objects with that label."""
        returned_set = self.get_label_ids(single_label)
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
        u"""Get a list of label names (not id numbers!) attached
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
        u"""Get the id of the filemaster entry associated with
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
        u"""Get a list of label names (not id numbers!) attached
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
        u"""Primary use for automatic test: Get a list of label names
            (not id numbers!) attached to a specific filemaster entry
            via its URL instead of the id. The reason for this is that
            the association with the URl predates the filemaster entry /
            the id."""
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
        u"""Get a set of ALL label names (not id numbers!) attached
            to a specific version of a file AND its filemaster entry."""
        version_labels = self.version_labels_by_uuid(version_uuid)
        filemaster_id = self.get_filemaster_id(version_uuid)
        filemaster_labels = self.filemaster_labels_by_id(filemaster_id)
        joined_set = version_labels | filemaster_labels
        return joined_set

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

    def remove_labels_from_uuid(self,
                                uuid: str,
                                labels_to_remove: set):
        u"""Detaches a label from a UUID / version."""

        # Using a set to avoid duplicates. However, accept either
        # a single string or a list type.
        labels_to_remove = userprovided.parameters.convert_to_set(
            labels_to_remove)

        # Get all label-ids
        id_list = self.get_label_ids(labels_to_remove)

        for label_id in id_list:
            self.cur.execute("DELETE FROM labelToVersion " +
                             "WHERE labelID = %s and versionUUID = %s;",
                             (label_id, uuid))

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
