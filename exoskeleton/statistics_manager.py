#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Manage the host statistics for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""
# standard library:
from collections import Counter
import logging
from typing import Optional
from urllib.parse import urlparse

import pymysql

from exoskeleton import database_connection


class StatisticsManager:
    """Manage the statistics like counting requests and errors,"""

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        self.cnt: Counter = Counter()

    def __num_tasks_wo_errors(self) -> Optional[int]:
        """Number of tasks left in the queue which are *not* marked as
        causing any kind of error. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        response = self.cur.fetchone()
        return int(response[0]) if response else None  # type: ignore[index]

    def num_tasks_w_permanent_errors(self) -> int:
        "Number of tasks in the queue marked as causing a permanent error."
        self.cur.execute('SELECT num_items_with_permanent_error();')
        num_permanent_errors = self.cur.fetchone()
        return int(num_permanent_errors[0]) if num_permanent_errors else 0  # type: ignore[index]

    def num_tasks_w_temporary_errors(self) -> int:
        "Number of tasks in the queue marked as causing a *temporary* error."
        self.cur.execute('SELECT num_items_with_temporary_errors();')
        num_temp_errors = self.cur.fetchone()
        return int(num_temp_errors[0]) if num_temp_errors else 0  # type: ignore[index]

    def num_tasks_w_rate_limit(self) -> int:
        """Number of tasks in the queue that do not yield a permanent error,
           but are currently affected by a rate limit."""
        self.cur.execute("SELECT num_tasks_with_active_rate_limit();")
        num_rate_limited = self.cur.fetchone()
        return int(num_rate_limited[0]) if num_rate_limited else 0  # type: ignore[index]

    def queue_stats(self) -> dict:
        """Return a number of statistics about the queue as a dictionary."""
        stats = {
            'tasks_without_error': self.__num_tasks_wo_errors(),
            'tasks_with_temp_errors': self.num_tasks_w_temporary_errors(),
            'tasks_with_permanent_errors': self.num_tasks_w_permanent_errors(),
            'tasks_blocked_by_rate_limit': self.num_tasks_w_rate_limit()
        }
        return stats

    def log_queue_stats(self) -> None:
        """Log the queue statistics using logging - that means to the screen
           or into a file depending on your setup. Especially useful when
           a bot starts or resumes processing the queue."""
        stats = self.queue_stats()
        overall_workable = (stats['tasks_without_error'] +
                            stats['tasks_with_temp_errors'])
        message = (f"The queue contains {overall_workable} tasks waiting " +
                   f"to be executed. {stats['tasks_blocked_by_rate_limit']} " +
                   "of those are stalled as the bot hit a rate limit. " +
                   f"{stats['tasks_with_permanent_errors']} cannot be " +
                   "executed due to permanent errors.")
        logging.info(message)

    def __update_host_statistics(self,
                                 url: str,
                                 successful_requests: int,
                                 temporary_problems: int,
                                 permanent_errors: int,
                                 hit_rate_limit: int) -> None:
        """ Updates the host based statistics. The URL gets shortened to
            the hostname. Increase the different counters."""

        fqdn = urlparse(url).hostname

        self.cur.execute('INSERT INTO statisticsHosts ' +
                         '(fqdnHash, fqdn, successfulRequests, ' +
                         'temporaryProblems, permamentErrors, hitRateLimit) ' +
                         'VALUES (SHA2(%s,256), %s, %s, %s, %s, %s) ' +
                         'ON DUPLICATE KEY UPDATE ' +
                         'successfulRequests = successfulRequests + %s, ' +
                         'temporaryProblems = temporaryProblems + %s, ' +
                         'permamentErrors = permamentErrors + %s, ' +
                         'hitRateLimit = hitRateLimit + %s;',
                         (fqdn, fqdn, successful_requests, temporary_problems,
                          permanent_errors, hit_rate_limit,
                          successful_requests, temporary_problems,
                          permanent_errors, hit_rate_limit))

    def log_successful_request(self,
                               url: str) -> None:
        """ Update the host based statistics: Log a succesful request
            for the host of the provided URL."""
        self.__update_host_statistics(url, 1, 0, 0, 0)

    def log_temporary_problem(self,
                              url: str) -> None:
        """ Update the host based statistics: Log a temporary error
            for the host of the provided URL."""
        self.__update_host_statistics(url, 0, 1, 0, 0)

    def log_permanent_error(self,
                            url: str) -> None:
        """ Update the host based statistics: Log a permanent error
            for the host of the provided URL."""
        self.__update_host_statistics(url, 0, 0, 1, 0)

    def log_rate_limit_hit(self,
                           url: str) -> None:
        """ Update the host based statistics: Log that the crawler hit the
            rate limit for the host of the provided URL."""
        self.__update_host_statistics(url, 0, 0, 0, 1)

    def increment_processed_counter(self) -> None:
        """Count the number of actions processed.
           This function is wrapping a Counter object
           to make it accesible from different objects."""
        self.cnt['processed'] += 1

    def get_processed_counter(self) -> int:
        """The number of processed tasks."""
        return self.cnt['processed']
