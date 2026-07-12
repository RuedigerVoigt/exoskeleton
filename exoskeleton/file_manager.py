"""
File handling for the exoskeleton framework
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
import logging
import pathlib


# external dependencies:
import requests
import userprovided

from exoskeleton import database_connection
from exoskeleton import err

logger = logging.getLogger(__name__)


class FileManager:
    "File handling for the exoskeleton framework"

    HASH_METHOD = 'sha256'

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection,
                 target_directory: str,
                 filename_prefix: str,
                 max_file_size: int | None = None
                 ) -> None:
        self.db_connection = db_connection
        self.target_dir = self.__check_target_directory(target_directory)
        logger.info("Saving files in this directory: %s", self.target_dir)
        self.file_prefix = self.__clean_prefix(filename_prefix)
        # None (the default) means downloads are not size-limited.
        self.max_file_size = max_file_size
        if self.max_file_size:
            logger.info("Downloads are capped at %s bytes.",
                        self.max_file_size)

    @staticmethod
    def __check_target_directory(target_directory: str) -> pathlib.Path:
        """Check if a target directory is set to write files to.
           If not fallback to the current working directory.
           If a directory is set, but not accessible, fail early."""

        if not target_directory or target_directory.strip() == '':
            logger.warning("Target directory is not set. Using the "
                           "current working directory as a fallback!")
            return pathlib.Path.cwd()

        # Assuming that if a directory was set, it has to be used.
        # Therefore no fallback to the current working directory.
        try:
            # make the path absolute and fail if it not exists (strict mode)
            target_dir = pathlib.Path(target_directory).resolve(strict=True)
        except FileNotFoundError as not_found:
            msg = "Cannot find the target directory! Create it."
            logger.exception(msg)
            raise FileNotFoundError(msg) from not_found
        # Is it a directory or a file
        if not target_dir.is_dir():
            msg = (f"Parameter 'target_directory' ({target_dir}) " +
                   "not a directory.")
            logger.exception(msg)
            raise AttributeError(msg)

        return target_dir

    @staticmethod
    def __clean_prefix(file_prefix: str) -> str:
        """Remove whitespace around the filename prefix and
           limit it to 16 characters."""
        if not file_prefix:
            logger.warning('You defined no filename prefix.')
            return ''

        file_prefix = file_prefix.strip()
        # Limit the prefix length as on many systems the path must not be
        # longer than 255 characters and it needs space for folders and the
        # actual filename. 16 characters seems to be a reasonable limit.
        if not userprovided.parameters.string_in_range(file_prefix, 0, 16):
            raise ValueError('File name prefix must be 16 characters or less.')
        if len(file_prefix) == 0:
            logger.warning('You defined no filename prefix.')

        return file_prefix

    def write_response_to_file(self,
                               response: requests.Response,
                               file_name: str) -> pathlib.Path:
        """Write the server's response into a file.

        Streams the response to disk in blocks. If max_file_size is set and
        the accumulated size exceeds it, writing stops, the partial file is
        removed, and err.FileSizeLimitError is raised."""
        target_path = self.target_dir.joinpath(file_name)
        bytes_written = 0
        size_exceeded = False
        with open(target_path, 'wb') as file_handle:
            for block in response.iter_content(1024):
                file_handle.write(block)
                bytes_written += len(block)
                if self.max_file_size and bytes_written > self.max_file_size:
                    size_exceeded = True
                    break

        if size_exceeded:
            target_path.unlink(missing_ok=True)
            raise err.FileSizeLimitError(
                f'Download exceeded the maximum file size of '
                f'{self.max_file_size} bytes; partial file removed.')

        logger.debug('file written to disk')
        return target_path

    def get_file_hash(self,
                      file_path: pathlib.Path) -> str:
        "Calculate the hash of a file (method fixed to SHA256)."
        hash_value = userprovided.hashing.calculate_file_hash(
            file_path, self.HASH_METHOD)
        return hash_value

    @staticmethod
    def get_file_size(file_path: pathlib.Path) -> int:
        "File size in bytes."
        try:
            return file_path.stat().st_size
        except Exception:
            logger.error('Cannot get size of %s', file_path, exc_info=True)
            raise
