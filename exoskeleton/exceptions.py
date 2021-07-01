#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework: Custom Exceptions

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""


class ExoskeletonException(Exception):
    "An exception occured"
    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        Exception.__init__(self, *args, **kwargs)


class InvalidDatabaseSchemaError(ExoskeletonException):
    """Raised if the database schema does not match the version of exoskeleton,
       or required elements (tables, procedures, ...) are missing."""


class HostOnBlocklistError(ExoskeletonException):
    """Raised in two cases:
         * You want to add a task, but the host is on the blocklist.
         * you want to execute a task, but the task has meanwhile been blocked.
    """
