#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging
import os
import re


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
        with open(file_path, 'rb') as f:
            content = f.read()
        h.update(content)
        return(h.hexdigest())
    except FileNotFoundError:
        logging.error('File not found or path not readable. ' +
                      'Cannot calculate hash.', exc_info=True)
        raise
    except:
        logging.error('Unknown exception while trying ' +
                      'to get file hash', exc_info=True)
        raise
