#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File handling for the exoskeleton framework
~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
import logging
import pathlib


# external dependencies:
import requests
import userprovided


class FileManager:
    """File handling for the exoskeleton framework"""

    def __init__(self,
                 db_cursor,
                 target_directory: str,
                 filename_prefix: str
                 ):
        self.cur = db_cursor
        self.target_dir = self.__check_target_directory(target_directory)
        self.file_prefix = self.__clean_prefix(filename_prefix)

        self.hash_method = 'sha256'
        if not userprovided.hash.hash_available(self.hash_method):
            raise ValueError('The hash method SHA256 is not available on ' +
                             'your system.')

    def __check_target_directory(self,
                                 target_directory) -> pathlib.Path:
        """Check if a target directory is set to write files to.
           If not fallback to the current working directory.
           If a directory is set, but not accesible, fail early."""

        target_dir = pathlib.Path.cwd()

        if target_directory is None or target_directory.strip() == '':
            logging.warning("Target directory is not set. " +
                            "Using the current working directory " +
                            "%s to store files!",
                            target_dir)
        else:
            # Assuming that if a directory was set, it has to be used.
            # Therefore no fallback to the current working directory.
            target_dir = pathlib.Path(target_directory).resolve()
            if target_dir.is_dir():
                logging.debug("Set target directory to %s", target_directory)
            else:
                raise OSError("Cannot find or access the user " +
                              "supplied target directory! " +
                              "Create this directory or check permissions.")

        return target_dir

    def __clean_prefix(self,
                       file_prefix: str) -> str:
        """Remove whitespace around the filenam prefix and check if it is
           not longer than 16 characters."""
        file_prefix = file_prefix.strip()
        # Limit the prefix length as on many systems the path must not be
        # longer than 255 characters and it needs space for folders and the
        # actual filename. 16 characters seems to be a reasonable limit.
        if not userprovided.parameters.string_in_range(file_prefix, 0, 16):
            raise ValueError('The file name prefix is limited to a ' +
                             'maximum of 16 characters.')
        if len(file_prefix) == 0:
            logging.warning('You defined no filename prefix.')

        return file_prefix

    def write_response_to_file(self,
                               r: requests.Response,
                               file_name: str) -> pathlib.Path:
        """Write the servers response (request.Response()) into a file."""
        target_path = self.target_dir.joinpath(file_name)
        with open(target_path, 'wb') as file_handle:
            for block in r.iter_content(1024):
                file_handle.write(block)
            logging.debug('file written to disk')

        return target_path

    def get_file_hash(self,
                      file_path: pathlib.Path) -> str:
        """Calculate the hash of a file using the set method
           (currently fixed to SHA256)."""
        hash_value = userprovided.hash.calculate_file_hash(
            file_path, self.hash_method)
        return hash_value

    def get_file_size(self,
                      file_path: pathlib.Path) -> int:
        """File size in bytes."""
        try:
            return file_path.stat().st_size
        except Exception:
            logging.error('Cannot get file size', exc_info=True)
            raise
