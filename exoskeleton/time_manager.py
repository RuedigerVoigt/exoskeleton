#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""

import logging
import random
import time


class TimeManager:
    """ Time management for the exoskeleton crawler framework:
        * Manage wait time
        * measure run time
        * estimate time need """

    def __init__(self,
                 wait_min: int = 5,
                 wait_max: int = 30):
        """Sets defaults"""

        # Check input validity:
        if type(wait_min) not in (int, float):
            raise ValueError('The value for wait_min must be numeric.')
        if type(wait_max) not in (int, float):
            raise ValueError('The value for wait_max must be numeric.')
        if wait_min > wait_max:
            raise ValueError("wait_max cannot be larger than wait_min.")
        self.wait_min = wait_min
        self.wait_max = wait_max

        # Start timers
        self.bot_start = time.monotonic()
        self.process_time_start = time.process_time()
        logging.debug('started timers')

    def random_wait(self) -> None:
        """Waits for a random time between actions
           (within the interval preset at initialization).
           This is done to avoid to accidentally overload the queried host.
           Some host actually enforce limits through IP blocking."""
        query_delay = random.randint(self.wait_min, self.wait_max)  # nosec
        logging.debug("%s seconds delay until next action", query_delay)
        time.sleep(query_delay)

    def increase_wait(self) -> None:
        """ Increases minimum and maximum wait time by one second each.
            Limited to a minimum wait time of 30 seconds
            and a maximum wait time of 50."""
        if self.wait_min < 30:
            self.wait_min = self.wait_min + 1
            logging.info("increased minimum wait time by 1 second.")
        if self.wait_max < 50:
            self.wait_max = self.wait_max + 1
            logging.info("increased maximum wait time by 1 second.")

    def absolute_run_time(self) -> float:
        """Return seconds since init. """
        return time.monotonic() - self.bot_start

    def get_process_time(self) -> float:
        """Return execution time since init"""
        return time.process_time() - self.process_time_start

    def estimate_remaining_time(self,
                                already_processed: int,
                                num_items_in_queue: int) -> int:
        """Estimate remaining seconds to finish crawl."""
        time_so_far = self.absolute_run_time()
        if already_processed == 0 and num_items_in_queue == 0:
            return -1
        if num_items_in_queue == 0:
            return 0
        if already_processed > 0:
            time_each = time_so_far / already_processed
            return round(num_items_in_queue * time_each)

        logging.warning('Cannot estimate remaining time. Not enough data.')
        return -1
