#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

if sys.version_info.major != 3 or sys.version_info.minor < 6:
    raise RuntimeError('You need at least Python 3.6 to run exoskeleton. ' +
                       'You are running: ' + str(sys.version_info.major) +
                       '.' + str(sys.version_info.minor))

from exoskeleton.__main__ import Exoskeleton

NAME = "exoskeleton"
__version__ = "1.2.1"
__author__ = "Rüdiger Voigt"
