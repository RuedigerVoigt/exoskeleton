#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" A range of function to check correctness
     of settings and parameters """

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
