import logging
import random
import time


class TimeManager:
    u""" Time management for the exoskeleton crawler framework. """

    def __init__(self,
                 wait_min: int = 5,
                 wait_max: int = 30):
        u"""Sets defaults"""

        # Check input validity:
        if type(wait_min) not in (int, float):
            raise ValueError('The value for wait_min must be numeric.')
        if type(wait_max) not in (int, float):
            raise ValueError('The value for wait_max must be numeric.')
        self.wait_min = wait_min
        self.wait_max = wait_max

        # Start timers
        self.bot_start = time.monotonic()
        self.process_time_start = time.process_time()
        logging.debug('started timers')

    def random_wait(self):
        u"""Waits for a random time between actions
        (within the interval preset at initialization).
        This is done to avoid to accidentally overload
        the queried host. Some host actually enforce
        limits through IP blocking."""
        query_delay = random.randint(self.wait_min, self.wait_max)  # nosec
        logging.debug("%s seconds delay until next action", query_delay)
        time.sleep(query_delay)
        return

    def increase_wait(self):
        u""" Increases minimum and maximum wait time by one second each.
             Limited to a minimum wait time of 30 seconds
             and a maximum wait time of 50."""
        if self.wait_min < 30:
            self.wait_min = self.wait_min + 1
            logging.info("increased minimum wait time by 1 second.")
        if self.wait_max < 50:
            self.wait_max = self.wait_max + 1
            logging.info("increased maximum wait time by 1 second.")

    def absolute_run_time(self) -> float:
        u"""Return seconds since init. """
        return time.monotonic() - self.bot_start

    def get_process_time(self) -> float:
        u"""Return execution time since init"""
        return time.process_time() - self.process_time_start

    def estimate_remaining_time(self,
                                already_processed: int,
                                num_items_in_queue: int) -> int:
        u"""Estimate remaining seconds to finish crawl."""
        time_so_far = self.absolute_run_time()
        if already_processed > 0:
            time_each = time_so_far / already_processed
            return round(num_items_in_queue * time_each)

        logging.warning('Cannot estimate remaining time ' +
                        'as there are no data so far.')
        return -1
