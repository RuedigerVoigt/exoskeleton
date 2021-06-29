#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class ExoUrl keeps and normalizes URLs which can be processed
by exoskeleton.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""

from hashlib import sha256
import logging
from urllib.parse import urlparse

import userprovided


class ExoUrl:
    "Hold a normalized URL that can be processed by exoskeleton."
    def __init__(self,
                 url_string: str) -> None:
        if not url_string:
            raise ValueError('Missing URL.')

        try:
            url_string = userprovided.url.normalize_url(url_string)
        except ValueError:
            logging.exception('Could not normalize URL string.')
            raise

        # Check if the scheme is either http or https (others are not supported)
        if not userprovided.url.is_url(url_string, ('http', 'https')):
            logging.exception(
                'Can not add URL %s : invalid or unsupported scheme',
                url_string)
            raise ValueError('Cannot add URL: invalid or unsupported scheme')

        self.url = url_string
        self.hash: str = self.generate_sha256_hash(self.url)
        self.hostname = urlparse(self.url).hostname

    def __str__(self) -> str:
        return str(self.url)

    def __repr__(self) -> str:
        return str(self.url)

    def __eq__(self, other):  # type: ignore[no-untyped-def]
        return self.url == other

    @staticmethod
    def generate_sha256_hash(url_string: str) -> str:
        """Generate the SHA256 hash of an URL the same way the database does.
           The encoding of the database is a version of UTF-8."""
        return sha256(url_string.encode('utf-8')).hexdigest()
