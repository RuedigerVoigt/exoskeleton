#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Utility functions to interact with files, ... """

import hashlib
import logging
import os


def get_file_size(file_path: str) -> int:
    u"""file size in bytes."""
    try:
        # TO DO: needs a path like object
        return os.path.getsize(file_path)
    except:
        logging.error('Cannot get file size', exc_info=True)
        raise


def get_file_hash(file_path: str,
                  method: str) -> str:
    u"""hash value for a file"""

    hash_algo = method

    if hash_algo == 'md5':
        h = hashlib.md5()
    elif hash_algo == 'sha1':
        h = hashlib.sha1()
    elif hash_algo == 'sha224':
        h = hashlib.sha256()
    elif hash_algo == 'sha256':
        h = hashlib.sha256()
    elif hash_algo == 'sha512':
        h = hashlib.sha512()
    else:
        raise ValueError('Hash method not supported by exoskeleton')

    try:
        with open(file_path, 'rb') as file:
            content = file.read()
        h.update(content)
        return h.hexdigest()
    except FileNotFoundError:
        logging.error('File not found or path not readable. ' +
                      'Cannot calculate hash.', exc_info=True)
        raise
    except:
        logging.error('Unknown exception while trying ' +
                      'to get file hash', exc_info=True)
        raise


def convert_to_set(convert_this: list) -> set:
    u""" Convert a string, a tuple, or a list into a set
    (i.e. no duplicates, unordered)"""

    if isinstance(convert_this, set):
        # functions using this expect a set, so everything
        # else just captures bad input by users
        new_set = convert_this
    elif isinstance(convert_this, str):
        new_set = {convert_this}
    elif isinstance(convert_this, list):
        new_set = set(convert_this)
    elif isinstance(convert_this, tuple):
        new_set = set(convert_this)
    else:
        raise TypeError('The function calling this expects a set.')

    return new_set
