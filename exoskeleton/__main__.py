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
import shutil
import subprocess
import time
from typing import Union, Optional
from urllib.parse import urlparse

# 3rd party libraries:
from bs4 import BeautifulSoup  # type: ignore
import pymysql
import urllib3  # type: ignore
import requests
# Sister projects:
import userprovided

# import other modules of this framework
from .DatabaseConnection import DatabaseConnection
from .TimeManager import TimeManager
from .NotificationManager import NotificationManager
from .QueueManager import QueueManager


class Exoskeleton:
    u""" Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint3 to complain a lot.
    # As the complexity is needed, disable some warnings:
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-public-methods
    # pylint: disable=too-many-branches

    MAX_PATH_LENGTH = 255

    # HTTP response codes have to be handeled differently depending on whether
    # they signal a permanent or temporary error. The following lists include
    # some non-standard codes. See:
    # https://en.wikipedia.org/wiki/List_of_HTTP_status_codes
    HTTP_PERMANENT_ERRORS = (400, 401, 402, 403, 404, 405, 406,
                             407, 410, 451, 501)
    # 429 (Rate Limit) is handeled separately:
    HTTP_TEMP_ERRORS = (408, 500, 502, 503, 504, 509, 529, 598)

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

        logging.info('You are using exoskeleton 1.0.0 (July 23, 2020)')

        self.project: str = project_name.strip()
        self.user_agent: str = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # Init database Connection
        self.db = DatabaseConnection(database_settings)
        self.cur = self.db.get_cursor()

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

        # Init time management
        self.tm = TimeManager(bot_behavior.get('wait_min', 5),
                              bot_behavior.get('wait_max', 30))

        # Init queue management
        self.qm = QueueManager(self.cur, self.tm, bot_behavior)

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
        if not userprovided.parameters.string_in_range(
                self.file_prefix, 0, 16):
            raise ValueError('The file name prefix is limited to a ' +
                             'maximum of 16 characters.')

        self.hash_method = 'sha256'
        if not userprovided.hash.hash_available(self.hash_method):
            raise ValueError('The hash method SHA256 is not available on ' +
                             'your system.')

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Browser
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # See if the executable name provided by the setup is in the path:
        self.browser_present = True if shutil.which(chrome_name) else False
        # if it is not avaible, some functionality is not avaialble:
        if not self.browser_present:
            logging.warning("No browser available with this executable name." +
                            "Saving a HTML page as PDF is not possible " +
                            "without that.")
        else:
            # found an executable
            # check whether the user provided an unsupported browser:
            unsupported_browsers = {'firefox', 'safari', 'edge'}
            supported_browsers = {'google-chrome', 'chrome', 'chromium',
                                  'chromium-browser'}
            if chrome_name.lower() in supported_browsers:
                logging.info('Broser supported and available.')
            elif chrome_name.lower() in unsupported_browsers:
                raise ValueError('Only Chrome and Chromium are supported!')
            else:
                logging.warning("Browser executable seems to be neither " +
                                "Google Chrome nor Chromium")

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

    def process_queue(self):
        u"""Process the queue"""
        self.qm.log_queue_stats()

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
                    logging.error('Cannot process queue item as the ' +
                                  'the FQDN has meanwhile been added to ' +
                                  'the blocklist!')
                    self.qm.delete_from_queue(queue_id)
                    logging.info('Removed item from queue: FQDN on blocklist.')
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
                    elif action == 4:
                        # save page text into database
                        self.__get_object(queue_id, 'text',
                                          url, url_hash,
                                          prettify_html)
                    else:
                        logging.error('Unknown action id!')

                    if self.milestone:
                        self.check_milestone()

                    # wait some interval to avoid overloading the server
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
        if action_type not in ('file', 'content', 'text'):
            raise ValueError('Invalid action')
        if url == '' or url is None:
            raise ValueError('Missing url')
        url = url.strip()
        if url_hash == '' or url_hash is None:
            raise ValueError('Missing url_hash')

        if action_type not in ('content', 'text') and prettify_html:
            logging.error('Wrong action_type: prettify_html ignored.')

        r = requests.Response()
        try:
            if action_type == 'file':
                logging.debug('starting download of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": self.user_agent},
                                 timeout=self.connection_timeout,
                                 stream=True)
            elif action_type in('content', 'text'):
                logging.debug('retrieving content of queue id %s', queue_id)
                r = requests.get(url,
                                 headers={"User-agent": self.user_agent},
                                 timeout=self.connection_timeout,
                                 stream=False
                                 )

            if r.status_code == 200:
                mime_type = ''
                content_type = r.headers.get('content-type')
                if content_type:
                    mime_type = (content_type).split(';')[0]

                if action_type == 'file':
                    extension = userprovided.url.determine_file_extension(
                        url, mime_type)
                    new_filename = f"{self.file_prefix}{queue_id}{extension}"
                    target_path = self.target_dir.joinpath(new_filename)

                    with open(target_path, 'wb') as file_handle:
                        for block in r.iter_content(1024):
                            file_handle.write(block)
                        logging.debug('file written')
                        hash_value = userprovided.hash.calculate_file_hash(
                            target_path, self.hash_method)

                    logging.debug('file written to disk')
                    try:
                        self.cur.callproc('insert_file_SP',
                                          (url, url_hash, queue_id, mime_type,
                                           str(self.target_dir), new_filename,
                                           self.get_file_size(target_path),
                                           self.hash_method, hash_value, 1))
                    except pymysql.DatabaseError:
                        self.cnt['transaction_fail'] += 1
                        logging.error('Transaction failed: Could not add ' +
                                      'already downloaded file %s to the ' +
                                      'database!', new_filename)

                elif action_type in ('content', 'text'):

                    detected_encoding = str(r.encoding)
                    logging.debug('detected encoding: %s', detected_encoding)

                    page_content = r.text

                    if mime_type == 'text/html' and prettify_html:
                        page_content = self.prettify_html(page_content)

                    if action_type == 'text':
                        soup = BeautifulSoup(page_content, 'lxml')
                        page_content = soup.get_text()

                    try:
                        # Stored procedure saves the content, transfers the
                        # labels from the queue, and removes the queue item:
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
                self.qm.update_host_statistics(url, 1, 0, 0, 0)

            elif r.status_code in self.HTTP_PERMANENT_ERRORS:
                self.qm.mark_permanent_error(queue_id, r.status_code)
                self.qm.update_host_statistics(url, 0, 0, 1, 0)
            elif r.status_code == 429:
                # The server tells explicity that the bot hit a rate limit!
                logging.error('The bot hit a rate limit! It queries too ' +
                              'fast => increase min_wait.')
                fqdn = urlparse(url).hostname
                if fqdn:
                    self.qm.add_rate_limit(fqdn)
                self.qm.update_host_statistics(url, 0, 0, 0, 1)
            elif r.status_code in self.HTTP_TEMP_ERRORS:
                logging.info('Temporary error. Adding delay to queue item.')
                self.qm.add_crawl_delay(queue_id, r.status_code)
            else:
                logging.error('Unhandled return code %s', r.status_code)
                self.qm.update_host_statistics(url, 0, 0, 1, 0)

        except TimeoutError:
            logging.error('Reached timeout.',
                          exc_info=True)
            self.qm.add_crawl_delay(queue_id, 4)
            self.qm.update_host_statistics(url, 0, 1, 0, 0)

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.qm.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except urllib3.exceptions.NewConnectionError:
            logging.error('New Connection Error: might be a rate limit',
                          exc_info=True)
            self.qm.update_host_statistics(url, 0, 0, 0, 1)
            self.tm.increase_wait()

        except requests.exceptions.MissingSchema:
            logging.error('Missing Schema Exception. Does your URL contain ' +
                          'the protocol i.e. http:// or https:// ? ' +
                          'See queue_id = %s', queue_id)
            self.qm.mark_permanent_error(queue_id, 1)

        except Exception:
            logging.error('Unknown exception while trying ' +
                          'to download.', exc_info=True)
            self.qm.update_host_statistics(url, 0, 0, 1, 0)
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

        if not self.browser_present:
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
                                   self.get_file_size(path),
                                   self.hash_method, hash_value, 3))
            except pymysql.DatabaseError:
                self.cnt['transaction_fail'] += 1
                logging.error('Transaction failed: Could not add already ' +
                              'downloaded file %s to the database!',
                              filename, exc_info=True)
            except Exception:
                logging.error('Unknown exception', exc_info=True)
            self.cnt['processed'] += 1
            self.qm.update_host_statistics(url, 1, 0, 0, 0)
        except subprocess.TimeoutExpired:
            logging.error('Cannot create PDF due to timeout.')
            self.qm.add_crawl_delay(queue_id, 4)
            self.qm.update_host_statistics(url, 0, 1, 0, 0)
        except subprocess.CalledProcessError:
            logging.error('Cannot create PDF due to process error.',
                          exc_info=True)
            self.qm.add_crawl_delay(queue_id, 5)
            self.qm.update_host_statistics(url, 0, 0, 1, 0)
        except Exception:
            logging.error('Exception.',
                          exc_info=True)
            self.qm.add_crawl_delay(queue_id, 0)
            self.qm.update_host_statistics(url, 0, 1, 0, 0)
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
            self.qm.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.qm.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except Exception:
            logging.exception('Exception while trying to get page-code',
                              exc_info=True)

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

    def add_file_download(self,
                          url: str,
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False) -> Optional[str]:
        u"""Add a file download URL to the queue """
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
        u"""Add an URL to the queue to save its HTML code
            into the database."""
        uuid = self.qm.add_to_queue(url, 2, labels_master,
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
        if not self.browser_present:
            logging.warning('Will add this task to the queue, but without ' +
                            'Chrome or Chromium this task cannot run!')
        uuid = self.qm.add_to_queue(url, 3, labels_master,
                                    labels_version, False,
                                    force_new_version)
        return uuid

    def add_save_page_text(self,
                           url: str,
                           labels_master: set = None,
                           labels_version: set = None,
                           force_new_version: bool = False) -> Optional[str]:
        u"""Extract the text (not the code) from a HTML page and store it
            into the database. This can be useful for some language processing tasks,
            but compared to add_save_page_code this removes the possiblity to work
            on a specific part using a CSS selector."""
        uuid = self.qm.add_to_queue(url, 4, labels_master,
                                    labels_version, True,
                                    force_new_version)
        return uuid

    def get_filemaster_id_by_url(self,
                                 url: str) -> Optional[str]:
        u"""Get the id of the filemaster entry associated with this URL."""
        return self.qm.get_filemaster_id_by_url(url)

    def delete_from_queue(self,
                          queue_id: str):
        u"""Remove all label links from a queue item and then delete it
            from the queue."""
        self.qm.delete_from_queue(queue_id)

    def forget_all_errors(self):
        u"""Treat all queued tasks, that are marked to cause any type of
            error, as if they are new tasks by removing that mark and
            any delay."""
        self.qm.forget_all_errors()

    def forget_permanent_errors(self):
        u"""Treat all queued tasks, that are marked to cause a *permanent*
            error, as if they are new tasks by removing that mark and
            any delay."""
        self.qm.forget_error_group(True)

    def forget_temporary_errors(self):
        u"""Treat all queued tasks, that are marked to cause a *temporary*
            error, as if they are new tasks by removing that mark and
            any delay."""
        self.qm.forget_error_group(False)

    def forget_specific_error(self,
                              specific_error: int):
        u"""Treat all queued tasks, that are marked to cause a *specific*
            error, as if they are new tasks by removing that mark and
            any delay. The number of the error has to correspond to the
            errorType database table."""
        self.qm.forget_specific_error(specific_error)

    def check_milestone(self):
        u""" Check if milestone is reached. If that is the case,
        send a notification (if configured to do so)."""
        processed = self.cnt['processed']
        # Have to check >0 in case the bot starts
        # failing with the first item.
        if processed > 0 and (processed % self.milestone) == 0:
            logging.info("Milestone reached: %s processed", str(processed))
            stats = self.qm.queue_stats()
            remaining_tasks = (stats['tasks_without_error'] +
                               stats['tasks_with_temp_errors'])
            self.notify.send_milestone_msg(
                self.cnt['processed'],
                remaining_tasks,
                self.tm.estimate_remaining_time(self.cnt['processed'],
                                                remaining_tasks)
                                            )

    def block_fqdn(self,
                   fqdn: str,
                   comment: Optional[str] = None):
        u"""Add a specific fully qualified domain name (fqdn)
            - like www.example.com - to the blocklist."""
        self.qm.block_fqdn(fqdn, comment)

    def unblock_fqdn(self,
                     fqdn: str):
        u"""Remove a specific fqdn from the blocklist."""
        self.cur.execute('DELETE FROM blockList ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         fqdn.strip())

    def truncate_blocklist(self):
        u"""Remove *all* entries from the blocklist."""
        self.qm.truncate_blocklist()

    def get_file_size(self,
                      file_path: pathlib.Path) -> int:
        u"""File size in bytes."""
        try:
            return file_path.stat().st_size
        except Exception:
            logging.error('Cannot get file size', exc_info=True)
            raise

    def prettify_html(self,
                      content: str) -> str:
        u"""Parse the HTML => add a document structure if needed
            => Encode HTML-entities and the document as Unicode (UTF-8).
            Only use for HTML, not XML."""

        # Empty elements are not removed as they might
        # be used to find specific elements within the tree.

        content = BeautifulSoup(content, 'lxml').prettify()

        return content

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # LABELS
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def define_or_update_label(self,
                               shortname: str,
                               description: str = None):
        u""" Insert a new label into the database or update its
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
        u"""Get a list of UUIDs (in this context file versions)
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

    def remove_labels_from_uuid(self,
                                uuid: str,
                                labels_to_remove: set):
        u"""Detaches a label from a UUID / version."""

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
