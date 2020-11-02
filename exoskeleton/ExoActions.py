#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
from collections import Counter
import logging

from bs4 import BeautifulSoup  # type: ignore
import pymysql
import requests
import urllib3  # type: ignore
from urllib.parse import urlparse
import userprovided


class ExoActions:
    """Manage actions (i.e. interactions with servers)
       except remote control of the chromium browser. """

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
                 db_cursor,
                 stats_manager_object,
                 file_manager_object,
                 time_manager_object,
                 crawling_error_manager_object,
                 remote_control_chrome_object,
                 user_agent: str,
                 connection_timeout: int):
        """ """
        self.cur = db_cursor
        self.stats = stats_manager_object
        self.fm = file_manager_object
        self.tm = time_manager_object
        self.errorhandling = crawling_error_manager_object
        self.controlled_browser = remote_control_chrome_object
        self.user_agent = user_agent
        self.connection_timeout = connection_timeout
        self.cnt: Counter = Counter()

    def get_object(self,
                   queue_id: str,
                   action_type: str,
                   url: str,
                   url_hash: str,
                   prettify_html: bool = False):
        """ Generic function to either download a file or
            store a page's content."""
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
                    new_filename = f"{self.fm.file_prefix}{queue_id}{extension}"
                    file_path = self.fm.write_response_to_file(r, new_filename)
                    hash_value = self.fm.get_file_hash(file_path)

                    try:
                        self.cur.callproc('insert_file_SP',
                                          (url, url_hash, queue_id, mime_type,
                                           str(self.fm.target_dir),
                                           new_filename,
                                           self.fm.get_file_size(file_path),
                                           self.fm.hash_method, hash_value, 1))
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

                self.stats.increment_processed_counter()
                self.stats.update_host_statistics(url, 1, 0, 0, 0)

            elif r.status_code in self.HTTP_PERMANENT_ERRORS:
                self.errorhandling.mark_permanent_error(
                    queue_id, r.status_code)
                self.stats.update_host_statistics(url, 0, 0, 1, 0)
            elif r.status_code == 429:
                # The server tells explicity that the bot hit a rate limit!
                logging.error('The bot hit a rate limit! It queries too ' +
                              'fast => increase min_wait.')
                fqdn = urlparse(url).hostname
                if fqdn:
                    self.errorhandling.add_rate_limit(fqdn)
                self.stats.update_host_statistics(url, 0, 0, 0, 1)
            elif r.status_code in self.HTTP_TEMP_ERRORS:
                logging.info('Temporary error. Adding delay to queue item.')
                self.errorhandling.add_crawl_delay(queue_id, r.status_code)
            else:
                logging.error('Unhandled return code %s', r.status_code)
                self.stats.update_host_statistics(url, 0, 0, 1, 0)

        except TimeoutError:
            logging.error('Reached timeout.', exc_info=True)
            self.errorhandling.add_crawl_delay(queue_id, 4)
            self.stats.update_host_statistics(url, 0, 1, 0, 0)

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.stats.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except urllib3.exceptions.NewConnectionError:
            logging.error('New Connection Error: might be a rate limit',
                          exc_info=True)
            self.stats.update_host_statistics(url, 0, 0, 0, 1)
            self.tm.increase_wait()

        except requests.exceptions.MissingSchema:
            logging.error('Missing Schema Exception. Does your URL contain ' +
                          'the protocol i.e. http:// or https:// ? ' +
                          'See queue_id = %s', queue_id)
            self.errorhandling.mark_permanent_error(queue_id, 1)

        except Exception:
            logging.error('Unknown exception while trying to download.',
                          exc_info=True)
            self.stats.update_host_statistics(url, 0, 0, 1, 0)
            raise

    def return_page_code(self,
                         url: str):
        """Directly return a page's code. Do *not* store it in the database."""
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
            self.stats.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.stats.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except Exception:
            logging.exception('Exception while trying to get page-code',
                              exc_info=True)

    def page_to_pdf(self,
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


    def prettify_html(self,
                      content: str) -> str:
        """Only use for HTML, not XML.
           Parse the HTML:
           * add a document structure if needed
           * Encode HTML-entities and the document as Unicode (UTF-8).
           * Empty elements are NOT removed as they might be used to find
             specific elements within the tree.
        """

        content = BeautifulSoup(content, 'lxml').prettify()

        return content
