#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging
import re


def check_email_format(mailaddress: str,
                       purpose: str = None) -> str:
    u"""very basic check if the email address has a valid format"""

    if mailaddress is None or mailaddress == '':
        logging.warning('No mail address supplied. ' +
                        'Unable to send emails.')
        return None
    else:
        mailaddress = mailaddress.strip()
        # MATCH KANN AUCH WHITESPACE ENTHALTEN. ZU SIMPEL!
        if not re.match(r"[^@]+@[^@]+\.[^@]+", mailaddress):
            logging.error('The supplied mailaddress has an unknown ' +
                          'format. Might not be able to send emails.')
            return None
        else:
            logging.debug('%s has a valid format', mailaddress)
            return mailaddress


def validate_port(port_number: int,
                  db: str):
    u"""Checks if the port is within range.
    Returns standard port if none is set."""

    if port_number is None:
        logging.info('No port number supplied. ' +
                     'Will try standard port instead.')
        if db == 'mariadb':
            return 3306
        elif db == 'postgresql':
            return 5432
        else:
            # should not be reached
            raise ValueError('Unknown DBMS')
    elif isinstance(port_number, int):
        if port_number >= 0 and port_number < 65536:
            logging.debug('Port within range')
            return port_number
        else:
            raise ValueError('Port not within valid ' +
                             'range from 0 to 65536')
    else:
        raise ValueError('Port has to be an integer.')


def check_hash_algo(hash_method: str):
    u"""Checks if the supplied hash algo is available and supported."""

    # Has a method been set?
    if hash_method == '' or hash_method is None:
        logging.warning('No hash method set. Will not ' +
                        'calculate file hashes.')
        return None

    # Is the chosen method available and supported?
    hash_method = hash_method.strip()
    if hash_method in hashlib.algorithms_available:
        logging.debug('Chosen hashing method is available on the system.')
        if hash_method in ('md5', 'sha1'):
            logging.info('Hash method set to %s', hash_method)
            logging.info('%s is fast, but a weak hashing algorithm. ' +
                         'Consider using another method if security ' +
                         'is important.', hash_method)
            return hash_method
        elif hash_method in ('sha224', 'sha256', 'sha512'):
            logging.info('Hash method set to %s', hash_method)
            return hash_method
        else:
            raise ValueError('The supplied hash method is available on ' +
                             'the system, but NOT (yet) supported ' +
                             'by exoskelton!')
    else:
        raise ValueError('The chosen hash method is NOT available ' +
                         'on this system!')
