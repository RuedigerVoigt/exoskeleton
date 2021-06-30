#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class CrawlingErrorManager manages errors that occur while
crawling. It marks permanent errors and adds delays.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""
# standard library:
import logging

# external dependencies:
import userprovided
import pymysql

from exoskeleton import database_connection


class CrawlingErrorManager:
    """Manage errors that occur while crawling."""

    # If there is a temporary error, exoskeleton delays the next try,
    # until the configured maximum of tries is reached.
    # The time between tries is definied here to be able to overwrite it
    # in case of an automatic tests to avoid multi-hour runtimes.
    # Steps: 1/4h, 1/2h, 1h, 3h, 6h
    DELAY_TRIES = (900, 1800, 3600, 10800, 21600)

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection,
                 queue_max_retries: int,
                 rate_limit_wait_seconds: int) -> None:
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = queue_max_retries
        self.queue_max_retries = userprovided.parameters.int_in_range(
            "queue_max_retries", self.queue_max_retries, 0, 10, 3)

        # Seconds to wait before contacting the same FQDN again, after the bot
        # hit a rate limit. Defaults to 1860 seconds (i.e. 31 minutes):
        self.rate_limit_wait: int = rate_limit_wait_seconds

    def add_crawl_delay(self,
                        queue_id: str,
                        error_type: int) -> None:
        """In case of a timeout or a temporary error increment the counter for
        the number of tries by one. If the configured maximum of tries
        was reached, mark it as a permanent error. Otherwise add a delay,
        so exoskelton does not try the same task again. As multiple tasks
        may affect the same URL, the delay is added to all of them."""
        wait_time = 0

        # Increase the tries counter and get the new count
        self.cur.callproc('increment_num_tries_SP', (queue_id, ))
        response = self.cur.fetchone()
        num_tries = int(response[0]) if response else 0  # type: ignore[index]

        # Does the number of tries exceed the configured maximum?
        if num_tries == self.queue_max_retries:
            # This is treated as a *permanent* failure!
            logging.error('Giving up: too many tries for task %s', queue_id)
            self.mark_permanent_error(queue_id, 3)
            return

        logging.info('Adding crawl delay to task %s', queue_id)
        # Using the class constant DELAY_TRIES because it can be easily
        # overwritten for automatic testing!
        if num_tries == 1:
            wait_time = self.DELAY_TRIES[0]  # 15 minutes
        elif num_tries == 2:
            wait_time = self.DELAY_TRIES[1]  # 30 minutes
        elif num_tries == 3:
            wait_time = self.DELAY_TRIES[2]  # 1 hour
        elif num_tries == 4:
            wait_time = self.DELAY_TRIES[3]  # 3 hours
        elif num_tries > 4:
            wait_time = self.DELAY_TRIES[4]  # 6 hours
        self.cur.callproc('add_crawl_delay_SP',
                          (queue_id, wait_time, error_type))

    def mark_permanent_error(self,
                             queue_id: str,
                             error: int) -> None:
        """ Mark task in queue that causes a *permanent* error.
            Without this exoskeleton would try to execute it again."""
        self.cur.callproc('mark_permanent_error_SP', (queue_id, error))
        logging.info('Marked task %s as causing a permanent error.', queue_id)

    def forget_specific_error(self,
                              specific_error: int) -> None:
        """Treat all queued tasks, that are marked to cause a *specific*
           error, as if they are new tasks by removing that mark and any delay.
           The number of the error has to correspond to the errorType
           database table."""
        self.cur.callproc('forget_specific_error_type_SP', (specific_error, ))

    def forget_temporary_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *temporary*
        error, as if they are new tasks by removing that mark and any delay."""
        self.cur.callproc('forget_error_group_SP', (0, ))

    def forget_permanent_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *permanent*
           error, as if they are new tasks by removing that mark and
           any delay."""
        self.cur.callproc('forget_error_group_SP', (1, ))

    def forget_all_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause any type of
           error, as if they were new tasks by removing that mark and any
           task specific delay.
           However, this does not remove delays due to rate limit on a per host
           basis. Use corresponding functions to remove those."""
        self.cur.callproc("forget_all_errors_SP")

    def add_rate_limit(self,
                       fqdn: str) -> None:
        """If a bot receives the statuscode 429 ('too many requests') it hit
           a rate limit. Adding the fully qualified domain name to the rate
           limit list, ensures that this FQDN is not contacted for a
           predefined time."""
        msg = (f"Bot hit a rate limit with {fqdn}. Will not try to " +
               f"contact this host for {self.rate_limit_wait} seconds.")
        logging.error(msg)
        self.cur.callproc('add_rate_limit_SP', (fqdn, self.rate_limit_wait))

    def forget_specific_rate_limit(self,
                                   fqdn: str) -> None:
        "Forget that the bot hit a rate limit for a specific FQDN."
        self.cur.callproc('forget_specific_rate_limit_SP', (fqdn, ))

    def forget_all_rate_limits(self) -> None:
        """Forget all rate limits the bot hit."""
        self.cur.callproc('forget_all_rate_limits_SP')
