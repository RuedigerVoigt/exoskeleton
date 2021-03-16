#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File handling for the exoskeleton framework
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""
# standard library:
import logging
import pathlib


# external dependencies:
import pymysql
import requests
import userprovided

from exoskeleton import database_connection


class FileManager:
    """File handling for the exoskeleton framework"""

    HASH_METHOD = 'sha256'

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection,
                 target_directory: str,
                 filename_prefix: str
                 ) -> None:
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        self.target_dir = self.__check_target_directory(target_directory)
        logging.info("Saving files in this directory: %s", self.target_dir)
        self.file_prefix = self.__clean_prefix(filename_prefix)

        if not userprovided.hash.hash_available(self.HASH_METHOD):
            raise ValueError(f"Hash method {self.HASH_METHOD} not available!")

    @staticmethod
    def __check_target_directory(target_directory: str) -> pathlib.Path:
        """Check if a target directory is set to write files to.
           If not fallback to the current working directory.
           If a directory is set, but not accessible, fail early."""

        if not target_directory or target_directory.strip() == '':
            logging.error("Target directory is not set. Using the " +
                          "current working directory as a fallback!")
            return pathlib.Path.cwd()

        # Assuming that if a directory was set, it has to be used.
        # Therefore no fallback to the current working directory.
        try:
            # make the path absolute and fail if it not exists (strict mode)
            target_dir = pathlib.Path(target_directory).resolve(strict=True)
        except FileNotFoundError as not_found:
            msg = "Cannot find the target directory! Create it."
            logging.exception(msg)
            raise FileNotFoundError(msg) from not_found
        # Is it a directory or a file
        if not target_dir.is_dir():
            msg = (f"Parameter 'target_directory' ({target_dir}) " +
                   "not a directory.")
            logging.exception(msg)
            raise AttributeError(msg)

        return target_dir

    @staticmethod
    def __clean_prefix(file_prefix: str) -> str:
        """Remove whitespace around the filename prefix and
           limit it to 16 characters."""
        if not file_prefix:
            logging.warning('You defined no filename prefix.')
            return ''

        file_prefix = file_prefix.strip()
        # Limit the prefix length as on many systems the path must not be
        # longer than 255 characters and it needs space for folders and the
        # actual filename. 16 characters seems to be a reasonable limit.
        if not userprovided.parameters.string_in_range(file_prefix, 0, 16):
            raise ValueError('File name prefix must be 16 characters or less.')
        if len(file_prefix) == 0:
            logging.warning('You defined no filename prefix.')

        return file_prefix

    def write_response_to_file(self,
                               response: requests.Response,
                               file_name: str) -> pathlib.Path:
        "Write the server's response into a file."
        target_path = self.target_dir.joinpath(file_name)
        with open(target_path, 'wb') as file_handle:
            for block in response.iter_content(1024):
                file_handle.write(block)
            logging.debug('file written to disk')

        return target_path

    def get_file_hash(self,
                      file_path: pathlib.Path) -> str:
        "Calculate the hash of a file (method currently fixed to SHA256)."
        hash_value = userprovided.hash.calculate_file_hash(
            file_path, self.HASH_METHOD)
        return hash_value

    @staticmethod
    def get_file_size(file_path: pathlib.Path) -> int:
        """File size in bytes."""
        try:
            return file_path.stat().st_size
        except Exception:
            logging.error('Cannot get file size of %s',
                          file_path, exc_info=True)
            raise
