"""
Manage the host statistics for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from collections import Counter
from hashlib import sha256
import logging
from typing import Literal

from sqlalchemy import func
from sqlalchemy.orm import Session

from exoskeleton import database_connection
from exoskeleton import exo_url
from exoskeleton import models

logger = logging.getLogger(__name__)


class StatisticsManager:
    """Manage the statistics like counting requests and errors,"""

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        self.db_connection = db_connection
        self.session: Session = db_connection.get_session()
        self.cnt: Counter = Counter()

    def num_tasks_wo_errors(self) -> int:
        """Number of tasks in the queue, which are *not* marked as causing
           any kind of error."""
        return self.session.query(models.Queue).filter(
            models.Queue.causesError.is_(None)
        ).count()

    def num_tasks_w_permanent_errors(self) -> int:
        "Number of tasks in the queue marked as causing a *permanent* error."
        return self.session.query(models.Queue).join(
            models.ErrorType,
            models.Queue.causesError == models.ErrorType.id
        ).filter(
            models.ErrorType.permanent.is_(True)
        ).count()

    def num_tasks_w_temporary_errors(self) -> int:
        "Number of tasks in the queue marked as causing a *temporary* error."
        return self.session.query(models.Queue).join(
            models.ErrorType,
            models.Queue.causesError == models.ErrorType.id
        ).filter(
            models.ErrorType.permanent.is_(False)
        ).count()

    def num_tasks_w_rate_limit(self) -> int:
        """Number of tasks in the queue that do not yield a permanent error,
           but are currently affected by a rate limit."""
        return self.session.query(models.Queue).join(
            models.RateLimit,
            models.Queue.fqdnHash == models.RateLimit.fqdnHash
        ).filter(
            models.RateLimit.noContactUntil > func.now()
        ).count()

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
        logger.info(message)

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
        assert url.hostname is not None, "URL hostname cannot be None"
        fqdn_hash = sha256(url.hostname.encode('utf-8')).hexdigest()

        host_stats = self.session.query(models.StatisticsHost).filter(
            models.StatisticsHost.fqdnHash == fqdn_hash
        ).first()

        if host_stats:
            host_stats.successfulRequests += successful_requests_increment  # type: ignore[assignment]
            host_stats.temporaryProblems += temporary_problems_increment  # type: ignore[assignment]
            host_stats.permamentErrors += permanent_errors_increment  # type: ignore[assignment]
            host_stats.hitRateLimit += hit_rate_limit_increment  # type: ignore[assignment]
        else:
            host_stats = models.StatisticsHost(
                fqdnHash=fqdn_hash,
                fqdn=url.hostname,
                successfulRequests=successful_requests_increment,
                temporaryProblems=temporary_problems_increment,
                permamentErrors=permanent_errors_increment,
                hitRateLimit=hit_rate_limit_increment
            )
            self.session.add(host_stats)

        self.session.commit()

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
