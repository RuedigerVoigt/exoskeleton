#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" A range of function to check correctness
     of settings and parameters """

import hashlib
import logging
from typing import Union


def validate_port(port_number: Union[int, None]) -> int:
    u"""Checks if the port is within range.
    Returns standard port if none is set."""

    if port_number is None:
        logging.info('No port number supplied. ' +
                     'Will try standard port instead.')
        return 3306
    elif isinstance(port_number, int):
        if port_number >= 0 and port_number < 65536:
            logging.debug('Port within range')
            return port_number
        else:
            raise ValueError('Port not within valid ' +
                             'range from 0 to 65536')
    else:
        raise ValueError('Port has to be an integer.')


def check_hash_algo(hash_method: Union[str, None]) -> bool:
    u"""Checks if the supplied hashing algorithm is available
        and supported. Will raise an exception if not."""

    if hash_method == '' or hash_method is None:
        # switch off calculating hashes
        logging.warning('No hash method set. Will *not* ' +
                        'calculate file hashes!')
        return False

    # Is the chosen method available and supported?
    hash_method = hash_method.strip()
    if hash_method in hashlib.algorithms_available:
        if hash_method in ('md5', 'sha1'):
            raise ValueError('The supplied hash method %s is deprecated ' +
                             ' and NOT supported by exoskelton!')
        elif hash_method in ('sha224', 'sha256', 'sha512'):
            logging.info('File hash method set to %s', hash_method)
            return True
        else:
            raise ValueError('The supplied hash method is available on ' +
                             'the system, but NOT supported by exoskelton!')
    else:
        raise ValueError('Chosen hash method NOT available on this system!')
