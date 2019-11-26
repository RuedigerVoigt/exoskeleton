#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging
import re


def check_email_format(mailaddress: str,
                       purpose: str = None) -> str:
    u"""Very basic check if the email address has a valid format
    and returns it as is except if it is obviously false."""

    if mailaddress is None or mailaddress == '':
        logging.warning('No mail address supplied. ' +
                        'Unable to send emails.')
        return None
    else:
        mailaddress = mailaddress.strip()
        if not re.match(r"^[^\s@]+@[^\s\W@]+\.[a-zA-Z]+", mailaddress):
            logging.error('The supplied mailaddress %s has an unknown ' +
                          'format.',
                          mailaddress)
            raise ValueError
        else:
            logging.debug('%s seems to have a valid format', mailaddress)
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

def validate_aws_s3_bucket_name(bucket_name: str) -> bool:
    u"""returns True if bucket name is well-formed for AWS S3 buckets

    Applying the rules set here:
    https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    """

    # Lengthy code which could be written as a single regular expression.
    # However written in this way to provide useful error messages.
    if len(bucket_name) < 3:
        logging.error('Any AWS bucket name has to be at least 3 ' +
                      'characters long.')
        return False
    elif len(bucket_name) > 63:
        logging.error('The provided bucket name for AWS exceeds the ' +
                      'maximum length of 63 characters.')
        return False
    elif not re.match(r"^[a-z0-9\-\.]*$", bucket_name):
        logging.error('The AWS bucket name contains invalid characters.')
        return False
    elif re.match(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", bucket_name):
        # Check if the bucket name resembles an IPv4 address.
        # No need to check IPv6 as the colon is not an allowed character.
        logging.error('An AWS must not resemble an IP address.')
        return False
    elif re.match(r"([a-z0-9][a-z0-9\-]*[a-z0-9]\.)*[a-z0-9][a-z0-9\-]*[a-z0-9]", bucket_name):
        # must start with a lowercase letter or number
        # Bucket names must be a series of one or more labels.
        # Adjacent labels are separated by a single period (.).
        # Each label must start and end with a lowercase letter or a number.
        # => Adopted the answer provided by Zak (zero or more labels followed by
        # a dot) found here:
        # https://stackoverflow.com/questions/50480924
        return True
    else:
        logging.error('Invalid AWS bucket name.')
        return False
