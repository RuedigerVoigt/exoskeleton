#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
from collections import Counter
import logging
from urllib.parse import urlparse


class StatisticsManager:
    """Manage the statistics."""

    def __init__(self,
                 db_cursor):
        self.cur = db_cursor
        self.cnt: Counter = Counter()

    def __num_tasks_wo_errors(self) -> int:
        """Number of tasks left in the queue which are *not* marked as
        causing any kind of error. """
        # How many are left in the queue?
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IS NULL;")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_permanent_errors(self) -> int:
        """Number of tasks in the queue that are marked as causing a permanent
        error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IN " +
                         "    (SELECT id FROM errorType WHERE permanent = 1);")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_temporary_errors(self) -> int:
        """Number of tasks in the queue that are marked as causing a
        temporary error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError IN " +
                         "    (SELECT id FROM errorType WHERE permanent = 0);")
        return int(self.cur.fetchone()[0])

    def __num_tasks_w_rate_limit(self) -> int:
        """Number of tasks in the queue that are marked as causing a permanent
        error."""
        self.cur.execute("SELECT COUNT(*) FROM queue " +
                         "WHERE causesError NOT IN " +
                         "    (SELECT id FROM errorType " +
                         "     WHERE permanent = 1) " +
                         "AND fqdnhash IN " +
                         "    (SELECT fqdnhash FROM rateLimits " +
                         "     WHERE noContactUntil > NOW());")
        return int(self.cur.fetchone()[0])

    def queue_stats(self) -> dict:
        """Return a number of statistics about the queue as a dictionary."""
        stats = {
            'tasks_without_error': self.__num_tasks_wo_errors(),
            'tasks_with_temp_errors': self.__num_tasks_w_temporary_errors(),
            'tasks_with_permanent_errors': self.__num_tasks_w_permanent_errors(),
            'tasks_blocked_by_rate_limit': self.__num_tasks_w_rate_limit()
        }
        return stats

    def log_queue_stats(self):
        """Log the queue statistics using logging - that means to the screen
        or into a file depending on your setup.
        Especially useful when a bot starts or resumes processing the queue."""
        stats = self.queue_stats()
        overall_workable = (stats['tasks_without_error'] +
                            stats['tasks_with_temp_errors'])
        message = (f"The queue contains {overall_workable} tasks waiting " +
                   f"to be executed. {stats['tasks_blocked_by_rate_limit']} " +
                   f"of those are stalled as the bot hit a rate limit. " +
                   f"{stats['tasks_with_permanent_errors']} cannot be " +
                   f"executed due to permanent errors.")
        logging.info(message)

    def update_host_statistics(self,
                               url: str,
                               successful_requests: int,
                               temporary_problems: int,
                               permanent_errors: int,
                               hit_rate_limit: int):
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

    def increment_processed_counter(self):
        """Count the number of actions processed.
           This function is wrapping a Counter object
           to make it accesible from different objects."""
        self.cnt['processed'] += 1

    def get_processed_counter(self) -> int:
        return self.cnt['processed']
