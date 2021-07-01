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
from typing import Literal, Optional

import pymysql

from exoskeleton import database_connection
from exoskeleton import exo_url


class StatisticsManager:
    """Manage the statistics like counting requests and errors,"""

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        self.cnt: Counter = Counter()

    def num_tasks_wo_errors(self) -> Optional[int]:
        """Number of tasks in the queue, which are *not* marked as causing
           any kind of error."""
        self.cur.execute("SELECT num_tasks_in_queue_without_error();")
        without_error = self.cur.fetchone()
        return int(without_error[0]) if without_error else None  # type: ignore[index]

    def num_tasks_w_permanent_errors(self) -> int:
        "Number of tasks in the queue marked as causing a *permanent* error."
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
            'tasks_without_error': self.num_tasks_wo_errors(),
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

    def __update_host_statistics(
                self,
                url: exo_url.ExoUrl,
                successful_requests_increment: Literal[0, 1] = 0,
                temporary_problems_increment: Literal[0, 1] = 0,
                permanent_errors_increment: Literal[0, 1] = 0,
                hit_rate_limit_increment: Literal[0, 1] = 0
                ) -> None:
        """ Updates the host based statistics. The URL gets shortened to
            the hostname. Increase the different counters."""
        # pylint: disable=too-many-arguments
        self.cur.callproc('update_host_stats_SP',
                          (url.hostname,
                           successful_requests_increment,
                           temporary_problems_increment,
                           permanent_errors_increment,
                           hit_rate_limit_increment))

    def log_successful_request(self,
                               url: exo_url.ExoUrl) -> None:
        """ Update the host based statistics: Log a succesful request
            for the host of the provided URL."""
        self.__update_host_statistics(url, successful_requests_increment=1)

    def log_temporary_problem(self,
                              url: exo_url.ExoUrl) -> None:
        """ Update the host based statistics: Log a temporary error
            for the host of the provided URL."""
        self.__update_host_statistics(url, temporary_problems_increment=1)

    def log_permanent_error(self,
                            url: exo_url.ExoUrl) -> None:
        """ Update the host based statistics: Log a permanent error
            for the host of the provided URL."""
        self.__update_host_statistics(url, permanent_errors_increment=1)

    def log_rate_limit_hit(self,
                           url: exo_url.ExoUrl) -> None:
        """ Update the host based statistics: Log that the crawler hit the
            rate limit for the host of the provided URL."""
        self.__update_host_statistics(url, hit_rate_limit_increment=1)

    def increment_processed_counter(self) -> None:
        """Count the number of actions processed.
        This wraps a Counter to make it accesible from outside the class."""
        self.cnt['processed'] += 1

    def get_processed_counter(self) -> int:
        "The number of processed tasks."
        return self.cnt['processed']
