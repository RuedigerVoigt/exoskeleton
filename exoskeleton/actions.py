"""
The class class ExoActions manages actions like downloading
a file or page content.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import Final, Literal

import requests
import urllib3
import userprovided
from sqlalchemy.exc import DatabaseError, SQLAlchemyError

from exoskeleton import database_connection
from exoskeleton import error_manager
from exoskeleton import exo_url
from exoskeleton import file_manager
from exoskeleton import helpers
from exoskeleton import models
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager

logger = logging.getLogger(__name__)


def insert_file_to_db(
        db_connection: database_connection.DatabaseConnection,
        url: str,
        url_hash: str,
        queue_id: str,
        mime_type: str,
        path_or_bucket: str,
        file_name: str,
        size: int,
        hash_method: str,
        hash_value: str,
        action_applied_id: int) -> None:
    """Update FileVersion with file metadata after download.
    FileVersion stub was created when added to queue, now update with actual data.

    This function handles the transaction and error handling that was
    previously in the stored procedure."""
    session = db_connection.get_session()

    try:
        # UPDATE fileVersions with actual file data
        # (FileVersion stub was created in queue_manager.add_to_queue)
        session.query(models.FileVersion).filter(
            models.FileVersion.id == queue_id
        ).update({
            models.FileVersion.mimeType: mime_type,
            models.FileVersion.pathOrBucket: path_or_bucket,
            models.FileVersion.fileName: file_name,
            models.FileVersion.size: size,
            models.FileVersion.hashMethod: hash_method,
            models.FileVersion.hashValue: hash_value
        }, synchronize_session=False)

        # DELETE from queue
        session.query(models.Queue).filter(
            models.Queue.id == queue_id
        ).delete(synchronize_session=False)

        session.commit()

    except SQLAlchemyError:
        session.rollback()
        # Update queue to mark error (causesError = 2)
        try:
            session.query(models.Queue).filter(
                models.Queue.id == queue_id
            ).update({models.Queue.causesError: 2}, synchronize_session=False)
            session.commit()
        except SQLAlchemyError:
            session.rollback()
        raise


def insert_content_to_db(db_connection: database_connection.DatabaseConnection,
                         url: str,
                         url_hash: str,
                         queue_id: str,
                         mime_type: str,
                         page_content: str,
                         action_applied_id: int) -> None:
    """Update FileVersion and insert page content into database using ORM.
    FileVersion stub was created when added to queue, now update with actual data.

    This function handles the transaction and error handling that was
    previously in the stored procedure."""
    session = db_connection.get_session()

    try:
        # UPDATE fileVersions with mime type
        # (FileVersion stub was created in queue_manager.add_to_queue)
        session.query(models.FileVersion).filter(
            models.FileVersion.id == queue_id
        ).update({
            models.FileVersion.mimeType: mime_type
        }, synchronize_session=False)

        # INSERT into fileContent
        new_content = models.FileContent(
            versionID=queue_id,
            pageContent=page_content
        )
        session.add(new_content)

        # DELETE from queue
        session.query(models.Queue).filter(
            models.Queue.id == queue_id
        ).delete(synchronize_session=False)

        session.commit()

    except SQLAlchemyError:
        session.rollback()
        # Update queue to mark error (causesError = 2)
        try:
            session.query(models.Queue).filter(
                models.Queue.id == queue_id
            ).update({models.Queue.causesError: 2}, synchronize_session=False)
            session.commit()
        except SQLAlchemyError:
            session.rollback()
        raise


def delete_all_versions(
        db_connection: database_connection.DatabaseConnection,
        file_master_id: int) -> None:
    """Delete all versions of a file including labels and the fileMaster entry.
    Converted from delete_all_versions_SP stored procedure.

    This function handles cascading deletes across multiple tables in the correct order
    to satisfy foreign key constraints.

    Args:
        db_connection: Database connection object
        file_master_id: The ID of the fileMaster entry to delete
    """
    session = db_connection.get_session()

    try:
        # Get the urlHash before we start deleting
        file_master = session.query(models.FileMaster).filter(
            models.FileMaster.id == file_master_id
        ).first()

        if not file_master:
            # Nothing to delete
            return

        url_hash = file_master.urlHash

        # Step 1: Get all version UUIDs for this file master
        version_ids = session.query(models.FileVersion.id).filter(
            models.FileVersion.fileMasterID == file_master_id
        ).all()
        version_uuid_list = [v[0] for v in version_ids]

        # Step 2: Remove all labels attached to versions
        if version_uuid_list:
            session.query(models.LabelToVersion).filter(
                models.LabelToVersion.versionUUID.in_(version_uuid_list)
            ).delete(synchronize_session=False)

        # Step 3: Remove all file versions
        session.query(models.FileVersion).filter(
            models.FileVersion.fileMasterID == file_master_id
        ).delete(synchronize_session=False)

        # Step 4: Remove all labels attached to the fileMaster
        session.query(models.LabelToMaster).filter(
            models.LabelToMaster.urlHash == url_hash
        ).delete(synchronize_session=False)

        # Step 5: Remove the fileMaster entry
        session.query(models.FileMaster).filter(
            models.FileMaster.id == file_master_id
        ).delete(synchronize_session=False)

        session.commit()

    except SQLAlchemyError:
        session.rollback()
        raise


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
                self.stats.increment_processed_counter()
                self.stats.log_successful_request(self.url)
            elif status_code in self.HTTP_PERMANENT_ERRORS:
                self.errorhandling.mark_permanent_error(self.queue_id, status_code)
                self.stats.log_permanent_error(self.url)
            elif status_code == 429:
                # The server tells explicity that the bot hit a rate limit!
                logger.error('The bot hit a rate limit => increase min_wait.')
                self.errorhandling.add_rate_limit(self.url.hostname)
                self.stats.log_rate_limit_hit(self.url)
            elif status_code in self.HTTP_TEMP_ERRORS:
                logger.info('Temporary error. Adding delay to queue item.')
                self.errorhandling.add_crawl_delay(self.queue_id, status_code)
            else:
                logger.error('Unhandled return code %s', status_code)
                self.stats.log_permanent_error(self.url)
        except requests.exceptions.Timeout:
            logger.error('Reached timeout.', exc_info=True)
            self.errorhandling.add_crawl_delay(self.queue_id, 4)
            self.stats.log_temporary_problem(self.url)

        except requests.exceptions.ConnectionError:
            logger.error('Connection Error', exc_info=True)
            self.stats.log_temporary_problem(self.url)
            raise

        except urllib3.exceptions.NewConnectionError:
            logger.error('New Connection Error: might be a rate limit',
                         exc_info=True)
            self.stats.log_rate_limit_hit(self.url)
            self.time.increase_wait()

        except Exception:
            logger.error('Unknown exception while trying to download.',
                         exc_info=True)
            self.stats.log_permanent_error(self.url)
            raise

    def handle_action(self) -> requests.Response:
        "Do the actual request"
        raise NotImplementedError('Thou shalt use a derived class')

    def store_result(self,
                     response: requests.Response,
                     strip_code: bool = False) -> None:
        "Write the result to the database"
        raise NotImplementedError('Thou shalt use a derived class')

    def _insert_file_to_db(
            self,
            url: str,
            url_hash: str,
            queue_id: str,
            mime_type: str,
            path_or_bucket: str,
            file_name: str,
            size: int,
            hash_method: str,
            hash_value: str,
            action_applied_id: int) -> None:
        """Update FileVersion with file metadata after download.
        FileVersion stub was created when added to queue, now update with actual data.

        This method handles the transaction and error handling that was
        previously in the stored procedure."""
        session = self.db_connection.get_session()

        try:
            # UPDATE fileVersions with actual file data
            # (FileVersion stub was created in queue_manager.add_to_queue)
            session.query(models.FileVersion).filter(
                models.FileVersion.id == queue_id
            ).update({
                models.FileVersion.mimeType: mime_type,
                models.FileVersion.pathOrBucket: path_or_bucket,
                models.FileVersion.fileName: file_name,
                models.FileVersion.size: size,
                models.FileVersion.hashMethod: hash_method,
                models.FileVersion.hashValue: hash_value
            }, synchronize_session=False)

            # DELETE from queue
            session.query(models.Queue).filter(
                models.Queue.id == queue_id
            ).delete(synchronize_session=False)

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            # Update queue to mark error (causesError = 2)
            try:
                session.query(models.Queue).filter(
                    models.Queue.id == queue_id
                ).update({models.Queue.causesError: 2}, synchronize_session=False)
                session.commit()
            except SQLAlchemyError:
                session.rollback()
            raise


class GetFile(GetObjectBaseClass):
    "Download a file"
    def handle_action(self) -> requests.Response:
        logger.debug('starting download of queue id %s', self.queue_id)
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
            insert_file_to_db(
                self.db_connection,
                str(self.url),
                self.url.hash,
                self.queue_id,
                self.mime_type,
                str(self.file.target_dir),
                new_filename,
                self.file.get_file_size(file_path),
                self.file.HASH_METHOD,
                hash_value,
                1)
        except DatabaseError:
            logger.error(
                'Did not add already downloaded file %s to the database!',
                new_filename)


class GetContent(GetObjectBaseClass):
    "Get a page's content including the HTML code"
    def handle_action(self) -> requests.Response:
        logger.debug('retrieving content of queue id %s', self.queue_id)
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
        logger.debug('detected encoding: %s', detected_encoding)
        page_content = response.text
        if self.mime_type == 'text/html' and self.prettify_html:
            page_content = helpers.prettify_html(page_content)
        if strip_code:
            page_content = helpers.strip_code(page_content)

        try:
            insert_content_to_db(
                self.db_connection,
                str(self.url),
                self.url.hash,
                self.queue_id,
                self.mime_type,
                page_content,
                2)
        except DatabaseError:
            logger.error(
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
            insert_file_to_db(
                self.db_connection,
                str(self.url),
                self.url.hash,
                self.queue_id,
                'application/pdf',
                str(self.file.target_dir),
                self.filename,
                self.file.get_file_size(self.path),
                self.file.HASH_METHOD,
                self.file.get_file_hash(self.path),
                3)
        except DatabaseError:
            logger.error(
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
            return str(response.text)

        except (TimeoutError, ConnectionError):
            logger.exception(
                'Exception while getting page-code', exc_info=True)
            self.stats.log_temporary_problem(url)
            raise
        except Exception:
            logger.exception(
                'Exception while getting page-code', exc_info=True)
            raise
