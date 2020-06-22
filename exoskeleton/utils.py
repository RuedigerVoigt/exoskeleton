#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Utility functions to interact with files, ... """

# python standard library:
import hashlib
import logging
import mimetypes
import pathlib
from typing import Optional

# 3rd party libraries:
from bs4 import BeautifulSoup  # type: ignore


def get_file_size(file_path: pathlib.Path) -> int:
    u"""file size in bytes."""
    try:
        # TO DO: needs a path like object
        return file_path.stat().st_size
    except Exception:
        logging.error('Cannot get file size', exc_info=True)
        raise


def get_file_hash(file_path: pathlib.Path,
                  method: str) -> str:
    u"""hash value for a file"""

    hash_algo = method

    if hash_algo == 'sha224':
        h = hashlib.sha224()
    elif hash_algo == 'sha256':
        h = hashlib.sha256()
    elif hash_algo == 'sha512':
        h = hashlib.sha512()
    else:
        raise ValueError('Hash method not supported by exoskeleton.')

    try:
        with open(file_path, 'rb') as file:
            content = file.read()
        h.update(content)
        return h.hexdigest()
    except FileNotFoundError:
        logging.error('File not found or path not readable. ' +
                      'Cannot calculate hash.', exc_info=True)
        raise
    except Exception:
        logging.error('Exception while trying to get file hash',
                      exc_info=True)
        raise


def determine_file_extension(url: str,
                             provided_mime_type: Optional[str] = None) -> str:
    u"""Guess the correct filename extension from an URL and / or
    the mime-type returned by the server.
    Sometimes a valid URL does not contain a file extension
    (like https://www.example.com/), or it is ambiguous.
    So the mime type acts as a fallback. In case the correct
    extension cannot be determined at all it is set to 'unknown'."""
    if provided_mime_type == '':
        provided_mime_type = None
    type_by_url = mimetypes.guess_type(url)[0]
    extension = None
    if type_by_url is not None and type_by_url == provided_mime_type:
        # Best case: URL and server header suggest the same filetype.
        extension = mimetypes.guess_extension(provided_mime_type)
    elif type_by_url is None and provided_mime_type is not None:
        # The URL does not contain an usable extension, but
        # the server provides one.
        extension = mimetypes.guess_extension(provided_mime_type)
    elif type_by_url is not None and provided_mime_type is None:
        # Misconfigured server but the type can be guessed.
        extension = mimetypes.guess_extension(type_by_url)
    else:
        # Worst case: neither the URL nor the server does hint to the
        # correct extension
        logging.error("The mime type (%s) suggested by the URL (%s)" +
                      "does not match the mime type supplied" +
                      "by the server (%s).",
                      (type_by_url, url, provided_mime_type))
        extension = None

    if extension is not None:
        if extension == '.bat' and provided_mime_type == 'text/plain':
            # text/plain is mapped to .bat in python 3.6.
            # Python 3.8 correctly guesses .txt as extension.
            return '.txt'

        if extension == '.htm':
            return '.html'

        return extension
    else:
        return '.unknown'


def prettify_html(content: str) -> str:
    u"""Parse the HTML => add a document structure if needed
        => Encode HTML-entities and the document as Unicode (UTF-8).
        Only use for HTML, not XML."""

    # Empty elements are not removed as they might
    # be used to find specific elements within the tree.

    content = BeautifulSoup(content, 'lxml').prettify()

    # TO DO: Add functionality to add a base URL
    # See issue #13.

    return content
