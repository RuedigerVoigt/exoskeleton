#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class class ExoActions manages actions like downloading
a file or page content.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import Final, Literal

import pymysql
import requests
import urllib3  # type: ignore
import userprovided

from exoskeleton import database_connection
from exoskeleton import error_manager
from exoskeleton import exo_url
from exoskeleton import file_manager
from exoskeleton import helpers
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager


class GetObjectBaseClass:
    "Base class to get objects."
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    # HTTP response codes have to be handeled differently depending on whether
    # they signal a permanent or temporary error. The following lists include
    # some non-standard codes. See:
    # https://en.wikipedia.org/wiki/List_of_HTTP_status_codes
    HTTP_PERMANENT_ERRORS: Final = (400, 401, 402, 403, 404, 405, 406,
                                    407, 410, 451, 501)
    # 429 (Rate Limit) is handeled separately:
    HTTP_TEMP_ERRORS: Final = (408, 500, 502, 503, 504, 509, 529, 598)

    def __init__(
            self,
            objects: dict,
            queue_id: str,
            url: exo_url.ExoUrl,
            prettify_html: bool = False):
        if not isinstance(queue_id, str):
            raise ValueError('The queue_id must be a string.')
        if not url:
            raise ValueError('Missing parameter url')
        self.db_connection = objects['db_connection']
        self.cur: pymysql.cursors.Cursor = self.db_connection.get_cursor()
        self.stats = objects['stats_manager_object']
        self.file = objects['file_manager_object']
        self.time = objects['time_manager_object']
        self.errorhandling = objects['crawling_error_manager_object']
        self.user_agent = objects['user_agent']
        self.connection_timeout = objects['connection_timeout']
        self.queue_id = queue_id
        self.url = url
        self.prettify_html = prettify_html

        self.mime_type: str = ''

        self.get_the_object()

    def get_the_object(self) -> None:
        "Get the object and handle exceptions that might occur"
        try:
            response = self.handle_action()
            status_code = response.status_code

            if status_code == 200:
                content_type = response.headers.get('content-type')
                if content_type:
                    self.mime_type = (content_type).split(';')[0]
                self.store_result(response)
            elif status_code in self.HTTP_PERMANENT_ERRORS:
                self.errorhandling.mark_permanent_error(self.queue_id, status_code)
                self.stats.log_permanent_error(self.url)
            elif status_code == 429:
                # The server tells explicity that the bot hit a rate limit!
                logging.error('The bot hit a rate limit => increase min_wait.')
                self.errorhandling.add_rate_limit(self.url.hostname)
                self.stats.log_rate_limit_hit(self.url)
            elif status_code in self.HTTP_TEMP_ERRORS:
                logging.info('Temporary error. Adding delay to queue item.')
                self.errorhandling.add_crawl_delay(self.queue_id, status_code)
            else:
                logging.error('Unhandled return code %s', status_code)
                self.stats.log_permanent_error(self.url)
        except TimeoutError:
            logging.error('Reached timeout.', exc_info=True)
            self.errorhandling.add_crawl_delay(self.queue_id, 4)
            self.stats.log_temporary_problem(self.url)

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.stats.log_temporary_problem(self.url)
            raise

        except urllib3.exceptions.NewConnectionError:
            logging.error('New Connection Error: might be a rate limit',
                          exc_info=True)
            self.stats.log_rate_limit_hit(self.url)
            self.time.increase_wait()

        except Exception:
            logging.error('Unknown exception while trying to download.',
                          exc_info=True)
            self.stats.log_permanent_error(self.url)
            raise
        self.stats.increment_processed_counter()
        self.stats.log_successful_request(self.url)

    def handle_action(self) -> requests.Response:
        "Do the actual request"
        raise NotImplementedError('Thou shalt use a derived class')

    def store_result(self,
                     response: requests.Response,
                     strip_code: bool = False) -> None:
        "Write the result to the database"
        raise NotImplementedError('Thou shalt use a derived class')


class GetFile(GetObjectBaseClass):
    "Download a file"
    def handle_action(self) -> requests.Response:
        logging.debug('starting download of queue id %s', self.queue_id)
        response = requests.get(
            str(self.url),
            headers={"User-agent": self.user_agent},
            timeout=self.connection_timeout,
            stream=True)
        return response

    def store_result(self,
                     response: requests.Response,
                     strip_code: bool = False) -> None:
        extension = userprovided.url.determine_file_extension(
            str(self.url), self.mime_type)
        new_filename = f"{self.file.file_prefix}{self.queue_id}{extension}"
        file_path = self.file.write_response_to_file(
            response, new_filename)
        hash_value = self.file.get_file_hash(file_path)

        try:
            self.cur.callproc('insert_file_SP',
                              (self.url, self.url.hash,
                               self.queue_id,
                               self.mime_type,
                               str(self.file.target_dir),
                               new_filename,
                               self.file.get_file_size(file_path),
                               self.file.HASH_METHOD,
                               hash_value, 1))
        except pymysql.DatabaseError:
            logging.error(
                'Did not add already downloaded file %s to the database!',
                new_filename)


class GetContent(GetObjectBaseClass):
    "Get a page's content including the HTML code"
    def handle_action(self) -> requests.Response:
        logging.debug('retrieving content of queue id %s', self.queue_id)
        response = requests.get(
            str(self.url),
            headers={"User-agent": self.user_agent},
            timeout=self.connection_timeout,
            stream=False)
        return response

    def store_result(self,
                     response: requests.Response,
                     strip_code: bool = False) -> None:
        detected_encoding = str(response.encoding)
        logging.debug('detected encoding: %s', detected_encoding)
        page_content = response.text
        if self.mime_type == 'text/html' and self.prettify_html:
            page_content = helpers.prettify_html(page_content)
        if strip_code:
            page_content = helpers.strip_code(page_content)

        try:
            # Stored procedure saves the content, transfers the
            # labels from the queue, and removes the queue item:
            self.cur.callproc('insert_content_SP',
                              (self.url, self.url.hash, self.queue_id,
                               self.mime_type, page_content, 2))
        except pymysql.DatabaseError:
            logging.error(
                'Transaction failed: Can not save page code of queue item %s!',
                self.queue_id, exc_info=True)


class GetText(GetContent):
    "Get the text on a page without the code."
    def store_result(self,
                     response: requests.Response,
                     strip_code: bool = True) -> None:
        return super().store_result(response, strip_code)


class GetPDF():
    """Use the Google Chrome or Chromium browser in headless mode to print the
       page to PDF and store it.
       BEWARE: Some cookie-popups blank out the page and all what is stored,
       is the dialogue."""
    # pylint: disable=too-many-instance-attributes

    # too different to use the same base class as the others
    def __init__(self,
                 objects: dict,
                 queue_id: str,
                 url: exo_url.ExoUrl) -> None:
        self.db_connection = objects['db_connection']
        self.cur: pymysql.cursors.Cursor = self.db_connection.get_cursor()
        self.file = objects['file_manager_object']
        self.controlled_browser = objects['controlled_browser']
        self.url = url
        self.queue_id = queue_id
        self.filename = f"{self.file.file_prefix}{self.queue_id}.pdf"
        self.path = self.file.target_dir.joinpath(self.filename)

        self.handle_action()
        self.store_result()

    def handle_action(self) -> None:
        "Get the PDF using a headless Chrome/Chromium"
        self.controlled_browser.page_to_pdf(self.url, self.path, self.queue_id)

    def store_result(self) -> None:
        "Store the PDF info in the database"
        try:
            self.cur.callproc(
                'insert_file_SP',
                (self.url, self.url.hash, self.queue_id, 'application/pdf',
                 str(self.file.target_dir), self.filename,
                 self.file.get_file_size(self.path),
                 self.file.HASH_METHOD, self.file.get_file_hash(self.path), 3))
        except pymysql.DatabaseError:
            logging.error(
                'Transaction failed: Could not add file %s to the database!',
                self.path, exc_info=True)


class ExoActions:
    """Manage actions (i.e. interactions with servers)
       except remote control of the chromium browser. """
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection,
            stats_manager_object: statistics_manager.StatisticsManager,
            file_manager_object: file_manager.FileManager,
            time_manager_object: time_manager.TimeManager,
            crawling_error_manager_object: error_manager.CrawlingErrorManager,
            remote_control_chrome_object: remote_control_chrome.RemoteControlChrome,
            user_agent: str,
            connection_timeout: int) -> None:
        "Init class"
        self.db_connection = db_connection
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        self.stats = stats_manager_object
        self.user_agent = user_agent
        self.connection_timeout = connection_timeout
        self.objects = {
            'db_connection': self.db_connection,
            'stats_manager_object': self.stats,
            'file_manager_object': file_manager_object,
            'time_manager_object': time_manager_object,
            'crawling_error_manager_object': crawling_error_manager_object,
            'user_agent': self.user_agent,
            'connection_timeout': self.connection_timeout,
            'controlled_browser': remote_control_chrome_object
        }

    def get_object(self,
                   queue_id: str,
                   action_type: Literal['file', 'content', 'text', 'page_to_pdf'],
                   url: exo_url.ExoUrl,
                   prettify_html: bool = False) -> None:
        "Generic function to either download a file or store a page's content."

        if action_type == 'file':
            GetFile(self.objects, queue_id, url, False)
        elif action_type == 'content':
            GetContent(self.objects, queue_id, url, prettify_html)
        elif action_type == 'text':
            GetText(self.objects, queue_id, url, prettify_html)
        elif action_type == 'page_to_pdf':
            GetPDF(self.objects, queue_id, url)
        else:
            raise ValueError('Invalid action_type!')

    def return_page_code(self,
                         url: exo_url.ExoUrl) -> str:
        "Directly return a page's code. Do *not* store it in the database."
        if not url:
            raise ValueError('Missing URL')

        try:
            response = requests.get(
                str(url),
                headers={"User-agent": str(self.user_agent)},
                timeout=self.connection_timeout,
                stream=False)

            if response.status_code == 404:
                raise RuntimeError('Not found: check URL')
            if response.status_code != 200:
                raise RuntimeError('Cannot return page code')
            return response.text

        except (TimeoutError, ConnectionError):
            logging.exception(
                'Exception while getting page-code', exc_info=True)
            self.stats.log_temporary_problem(url)
            raise
        except Exception:
            logging.exception(
                'Exception while getting page-code', exc_info=True)
            raise
