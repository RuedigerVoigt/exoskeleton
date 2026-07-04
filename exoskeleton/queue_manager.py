"""
The class QueueManager manages the action queue for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from datetime import datetime, timedelta
from hashlib import sha256
import logging
import time
from typing import Literal, Optional, Union
import uuid


# external dependencies:
from sqlalchemy import and_, or_, func, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
import userprovided

from exoskeleton import actions
from exoskeleton import blocklist_manager
from exoskeleton import database_connection
from exoskeleton import err
from exoskeleton import exo_url
from exoskeleton import label_manager
from exoskeleton import models
from exoskeleton import notification_manager
from exoskeleton import statistics_manager
from exoskeleton import time_manager

logger = logging.getLogger(__name__)


class QueueManager:
    "Manage the queue and labels for the exoskeleton framework."
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches

    # Seconds a claimed task stays leased to the worker that grabbed it.
    # Long enough to cover a download plus the following random_wait, so a
    # second worker does not pick up a task still in progress. If a worker
    # dies mid-task the lease expires and the task becomes claimable again.
    TASK_LEASE_SECONDS = 300

    def __init__(
            self,
            db_connection: database_connection.DatabaseConnection,
            blocklist_manager_object: blocklist_manager.BlocklistManager,
            time_manager_object: time_manager.TimeManager,
            stats_manager_object: statistics_manager.StatisticsManager,
            actions_object: actions.ExoActions,
            notification_manager_object: notification_manager.NotificationManager,
            label_manager_object: label_manager.LabelManager,
            bot_behavior: dict) -> None:
        self.db_connection = db_connection
        self.blocklist = blocklist_manager_object
        self.time = time_manager_object
        self.stats = stats_manager_object
        self.actions = actions_object
        self.notify = notification_manager_object
        self.labels = label_manager_object

        self.stop_if_queue_empty: bool = bot_behavior.get(
            'stop_if_queue_empty', False)
        userprovided.parameters.enforce_boolean(
            self.stop_if_queue_empty, 'stop_if_queue_empty')

        # Time to wait after the queue is empty to check for new elements:
        self.queue_revisit: int = bot_behavior.get('queue_revisit', 20)
        self.queue_revisit = userprovided.parameters.int_in_range(
            "queue_revisit", self.queue_revisit, 10, 50, 50)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ADDING TO AND REMOVING FROM THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_to_queue(self,
                     url: exo_url.ExoUrl,
                     action: Literal[1, 2, 3, 4],
                     labels_master: Optional[set] = None,
                     labels_version: Optional[set] = None,
                     prettify_html: bool = False,
                     force_new_version: bool = False) -> Optional[str]:
        """ More general function to add items to queue. Called by
            add_file_download, add_save_page_code and add_page_to_pdf."""
        if not isinstance(url, exo_url.ExoUrl):
            raise ValueError('url must be of class ExoUrl.')
        if action not in (1, 2, 3, 4):
            raise ValueError('Invalid value for action!')

        # Check if the FQDN of the URL is on the blocklist
        if url.hostname and self.blocklist.check_blocklist(url.hostname):
            msg = 'Cannot add URL to queue: FQDN is on blocklist.'
            logger.exception(msg)
            raise err.HostOnBlocklistError(msg)

        # One session for the whole operation: master entry, version stub,
        # labels and queue entry are committed together.
        with self.db_connection.session_scope() as session:
            # Add labels for the master entry.
            # Ignore labels for the version at this point, as it might
            # not get processed. (An early return below still commits
            # these on scope exit - intended, as labels of duplicate
            # URLs attach to the existing file.)
            if labels_master:
                self.labels.assign_labels_to_master(
                    url, labels_master, session=session)

            if not force_new_version:
                # check if the URL has already been processed
                id_in_file_master = self.get_filemaster_id_by_url(
                    url, session=session)

                if id_in_file_master:
                    # The URL has been processed in _some_ way.
                    # Check if was the _same_ as now requested.
                    file_version = session.query(models.FileVersion.id).filter(
                        models.FileVersion.fileMasterID == id_in_file_master,
                        models.FileVersion.actionAppliedID == action
                    ).first()

                    if file_version:
                        logger.info(
                            'Skipping file already processed in the same way.')
                        return None

                    # log and simply go on
                    logger.debug(
                        'File already processed, BUT not this way: Added to queue.')
                else:
                    # File has not been processed yet.
                    # If the exact same task is *not* already in the queue, add it.
                    if self.__get_queue_uuids(session, url, action):
                        logger.info('Exact same task already in queue.')
                        return None

            # generate a random uuid for the file version
            uuid_value = uuid.uuid4().hex

            # Create or get fileMaster entry
            existing_master = session.query(models.FileMaster).filter(
                models.FileMaster.urlHash == url.hash
            ).first()

            if not existing_master:
                new_master = models.FileMaster(url=str(url), urlHash=url.hash)
                session.add(new_master)
                session.flush()  # Get the ID
                file_master_id = new_master.id
            else:
                file_master_id = existing_master.id

            # Determine storage type based on action
            # action 1 (download file) -> storageTypeID 2 (disk)
            # action 2 (save page code) -> storageTypeID 1 (database)
            # action 3 (page to PDF) -> storageTypeID 3 (pdf)
            # action 4 (save page text) -> storageTypeID 1 (database)
            storage_type_map = {1: 2, 2: 1, 3: 3, 4: 1}
            storage_type_id = storage_type_map[action]

            # Create stub FileVersion record (will be updated when processed)
            new_version = models.FileVersion(
                id=uuid_value,
                fileMasterID=file_master_id,
                storageTypeID=storage_type_id,
                actionAppliedID=action
            )
            session.add(new_version)
            # Flush FileVersion first to satisfy FK constraint for label assignments
            session.flush()

            # Now assign version labels (FileVersion is flushed and visible
            # for FK checks because the same session is used)
            if labels_version:
                self.labels.assign_labels_to_uuid(
                    uuid_value, labels_version, session=session)

            # add the new task to the queue
            assert url.hostname is not None, "URL hostname cannot be None"
            fqdn_hash = sha256(url.hostname.encode('utf-8')).hexdigest()

            new_queue_item = models.Queue(
                id=uuid_value,
                action=action,
                url=str(url),
                urlHash=url.hash,
                fqdnHash=fqdn_hash,
                prettifyHtml=prettify_html
            )
            session.add(new_queue_item)

        return uuid_value

    @staticmethod
    def __get_queue_uuids(session: Session,
                          url: exo_url.ExoUrl,
                          action: int) -> set:
        """Based on the URL and action ID this returns a set of UUIDs in the
           *queue* that match those. Normally this set has a single element,
           but as you can force exoskeleton to repeat tasks on the same
           URL it can be multiple. Returns an empty set if such combination
           is not in the queue."""
        queue_uuids = session.query(models.Queue.id).filter(
            models.Queue.urlHash == url.hash,
            models.Queue.action == action
        ).order_by(models.Queue.addedToQueue.asc()).all()

        return {uuid[0] for uuid in queue_uuids} if queue_uuids else set()

    def get_filemaster_id_by_url(self,
                                 url: Union[exo_url.ExoUrl, str],
                                 session: Optional[Session] = None
                                 ) -> Optional[str]:
        "Get the id of the filemaster entry associated with this URL"
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        if session is not None:
            file_master = session.query(models.FileMaster.id).filter(
                models.FileMaster.urlHash == url.hash
            ).first()
        else:
            with self.db_connection.session_scope() as new_session:
                file_master = new_session.query(models.FileMaster.id).filter(
                    models.FileMaster.urlHash == url.hash
                ).first()

        return str(file_master[0]) if file_master else None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESSING THE QUEUE
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_next_task(self) -> Optional[tuple]:
        """Atomically claim and return the next suitable task.

        The candidate row is locked with FOR UPDATE ... SKIP LOCKED and
        immediately leased (lockedUntil set into the future) in the same
        transaction. Concurrent workers therefore skip a task that is already
        being processed and never grab the same row twice. Returns None if no
        actionable task is available.

        Requires MariaDB 10.6+ (for SKIP LOCKED). On backends without row
        locking (e.g. SQLite) the lease column still prevents re-processing.
        """
        # Converted from next_queue_object_SP stored procedure

        # Subquery for temporary errors (permanent = 0)
        temp_error_ids = select(models.ErrorType.id).where(
            models.ErrorType.permanent == 0
        )

        # Subquery for rate-limited hosts
        rate_limited_hosts = select(models.RateLimit.fqdnHash).where(
            models.RateLimit.noContactUntil > func.now()
        )

        lease_until = datetime.now() + timedelta(seconds=self.TASK_LEASE_SECONDS)

        with self.db_connection.session_scope() as session:
            candidate = session.query(
                models.Queue.id,
                models.Queue.action,
                models.Queue.url,
                models.Queue.urlHash,
                models.Queue.prettifyHtml
            ).filter(
                and_(
                    or_(
                        models.Queue.causesError.is_(None),
                        models.Queue.causesError.in_(temp_error_ids)  # type: ignore[arg-type]
                    ),
                    ~models.Queue.fqdnHash.in_(rate_limited_hosts),  # type: ignore[arg-type]
                    or_(
                        models.Queue.delayUntil.is_(None),
                        models.Queue.delayUntil < func.now()
                    ),
                    or_(
                        models.Queue.lockedUntil.is_(None),
                        models.Queue.lockedUntil < func.now()
                    ),
                    models.Queue.action.in_([1, 2, 3, 4])
                )
            ).order_by(
                models.Queue.addedToQueue.asc()
            ).with_for_update(skip_locked=True).first()

            if candidate is None:
                return None

            # Claim the row: set the lease before the lock is released on commit.
            session.query(models.Queue).filter(
                models.Queue.id == candidate[0]
            ).update(
                {models.Queue.lockedUntil: lease_until},
                synchronize_session=False)

            # Materialize before the session closes and detaches the row.
            return tuple(candidate)

    def delete_from_queue(self,
                          queue_id: str) -> None:
        """Remove all label links from item, delete FileVersion stub, and delete from queue.

        When an item is removed from the queue without being processed, we need to clean up:
        1. Label associations (LabelToVersion)
        2. FileVersion stub (created when added to queue)
        3. Queue entry itself
        """
        with self.db_connection.session_scope() as session:
            # Remove label associations
            session.query(models.LabelToVersion).filter(
                models.LabelToVersion.versionUUID == queue_id
            ).delete(synchronize_session=False)

            # Remove FileVersion stub (created when added to queue, never processed)
            session.query(models.FileVersion).filter(
                models.FileVersion.id == queue_id
            ).delete(synchronize_session=False)

            # Remove queue entry
            session.query(models.Queue).filter(
                models.Queue.id == queue_id
            ).delete(synchronize_session=False)

    def process_queue(self) -> None:
        "Process the queue"
        self.stats.log_queue_stats()

        while True:
            try:
                next_in_queue = self.get_next_task()
            except OperationalError:
                # Database connection lost. Sessions are per-operation, so
                # simply retrying is enough - the pool reconnects itself.
                logger.error('Lost database connection. '
                             'Trying to restore it in 10 seconds ...')
                time.sleep(10)
                try:
                    next_in_queue = self.get_next_task()
                    logger.info('Restored database connection!')
                except Exception as exc:
                    msg = 'Could not reestablish database connection'
                    logger.exception(msg, exc_info=True)
                    self.notify.send_msg_abort_lost_db()
                    raise ConnectionError(msg) from exc

            if next_in_queue is None:
                # no actionable item in the queue
                if self.stop_if_queue_empty:
                    # Bot is configured to stop if queue is empty
                    # => check if that is only temporary or everything is done

                    if self.stats.num_tasks_w_temporary_errors() > 0:
                        # there are still tasks, but they have to wait
                        logger.debug("Tasks with temporary errors: "
                                     "waiting %s seconds until next try.",
                                     self.queue_revisit)
                        time.sleep(self.queue_revisit)
                        continue

                    # Nothing left (i.e. num_temp_errors == 0)
                    logger.info('Queue empty. Bot stops as configured.')

                    num_permanent_errors = self.stats.num_tasks_w_permanent_errors()
                    if num_permanent_errors > 0:
                        logger.error("%s permanent errors!",
                                     num_permanent_errors)
                    self.notify.send_msg_finish()
                    break

                logger.debug(
                    "No actionable task: waiting %s seconds until next check",
                    self.queue_revisit)
                time.sleep(self.queue_revisit)
                continue

            # Got a task from the queue!
            queue_id = next_in_queue[0]
            action = next_in_queue[1]
            url = exo_url.ExoUrl(next_in_queue[2])
            prettify_html = (next_in_queue[4] == 1)

            # The FQDN might have been added to the blocklist *after*
            # the task entered into the queue!
            if self.blocklist.check_blocklist(str(url.hostname)):
                logger.error(
                    'Cannot process queue item: FQDN meanwhile on blocklist!')
                self.delete_from_queue(queue_id)
                logger.info('Removed item from queue: FQDN on blocklist.')
            else:
                if action == 1:  # download file to disk
                    self.actions.get_object(queue_id, 'file', url)
                elif action == 2:  # save page code into database
                    self.actions.get_object(queue_id, 'content', url, prettify_html)
                elif action == 3:  # headless Chrome to create PDF
                    self.actions.get_object(queue_id, 'page_to_pdf', url)
                elif action == 4:  # save page text into database
                    self.actions.get_object(queue_id, 'text', url)
                else:
                    logger.error('Unknown action id!')

                self.notify.send_msg_milestone()

                # wait some interval to avoid overloading the server
                self.time.random_wait()
