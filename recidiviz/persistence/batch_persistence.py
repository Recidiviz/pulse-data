# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Contains logic for communicating with the batch persistence layer."""
import itertools
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from typing import List, Optional, Tuple

import attr
import cattr
from flask import Blueprint, request, url_for
from google.cloud import pubsub

from recidiviz.common import queues
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import ingest_utils, scrape_phase, sessions
from recidiviz.ingest.scrape.constants import BATCH_PUBSUB_TYPE, ScrapeType
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.persistence import persistence
from recidiviz.utils import monitoring, pubsub_helper, regions
from recidiviz.utils.auth import authenticate_request

BATCH_READ_SIZE = 500
NUM_PULL_THREADS = 5
FAILED_TASK_THRESHOLD = 0.1
batch_blueprint = Blueprint('batch', __name__)


class BatchPersistError(Exception):
    """Raised when there was an error with batch persistence."""

    def __init__(self, region: str, scrape_type: ScrapeType):
        msg_template = "Error when running '{}' for region {}"
        msg = msg_template.format(scrape_type, region)
        super(BatchPersistError, self).__init__(msg)


@attr.s(frozen=True)
class BatchMessage:
    """A wrapper around a message to publish so we can batch up the writes.

    This is the object that is serialized and put on the pubsub queue.
    """
    # The task which published this message.  We use this to dedupe messages
    # That failed some number of times before finally passing, or to not
    # double count tasks that failed more than once.
    task: Task = attr.ib()

    # The ingest info object that was batched up for a write.
    ingest_info: Optional[IngestInfo] = attr.ib(default=None)

    # The error type of the task if it ended in failure.
    error: Optional[str] = attr.ib(default=None)

    # The trace id of the failing request if it failed.  Used for debugging.
    trace_id: Optional[str] = attr.ib(default=None)

    def to_serializable(self):
        return cattr.unstructure(self)

    @classmethod
    def from_serializable(cls, serializable):
        return cattr.structure(serializable, cls)


def _publish_batch_message(
        batch_message: BatchMessage, scrape_key: ScrapeKey):
    """Publishes the ingest info BatchMessage."""

    def publish():
        serialized = batch_message.to_serializable()
        response = pubsub_helper.get_publisher().publish(
            pubsub_helper.get_topic_path(scrape_key,
                                         pubsub_type=BATCH_PUBSUB_TYPE),
            data=json.dumps(serialized).encode())
        response.result()

    pubsub_helper.retry_with_create(scrape_key, publish, BATCH_PUBSUB_TYPE)


def _get_batch_messages(scrape_key) -> List[pubsub.types.ReceivedMessage]:
    """Reads all of the messages from pubsub for the scrape key.

    Args:
        scrape_key: (ScrapeKey): The scrape key to tell us which queue to
            retrieve from.
    Returns:
        A list of messages (ReceivedMessaged) containing the message data
        and the ack_id.
    """
    def async_pull() -> Tuple[List[pubsub.types.ReceivedMessage], bool]:
        """Pulls messages and returns them and whether an error occurred."""
        logging.info('Pulling messages off pubsub')
        def inner():
            subscriber = pubsub_helper.get_subscriber()
            sub_path = pubsub_helper.get_subscription_path(
                scrape_key, pubsub_type=BATCH_PUBSUB_TYPE)
            logging.info('Got the subscription path')
            return subscriber.pull(
                sub_path,
                max_messages=BATCH_READ_SIZE,
                return_immediately=True
            )
        try:
            recv_messages = pubsub_helper.retry_with_create(
                scrape_key, inner, pubsub_type=BATCH_PUBSUB_TYPE).\
                received_messages
            if recv_messages:
                logging.info('Finished pulling messages, read %s messages',
                             len(recv_messages))
                _ack_messages(recv_messages, scrape_key)
            return recv_messages, False
        # We occasionally get timeouts, we want to catch these and not kill the
        # thread.
        except Exception as e:
            logging.error('Got an error trying to pull: %s', str(e))
            return [], True

    pool = ThreadPoolExecutor(NUM_PULL_THREADS)
    messages = []
    # Pubsub provides no guarantees about when all messages have been read off
    # of pubsub.  If we don't return immediately, we might wait too long and
    # returning immediately, even if no messages were returned, does not
    # guarantee we are done.  The recommended solution is to keep trying until
    # a sufficient number of pulls returns no messages.  We therefore do 5
    # concurrent pulls and we break if and only if all 5 threads returned no
    # messages.
    while True:
        futures = [pool.submit(async_pull) for _ in range(NUM_PULL_THREADS)]
        results, failed = zip(*(future.result()
                                for future in as_completed(futures)))
        if all(failed):
            raise BatchPersistError(
                scrape_key.region_code, scrape_key.scrape_type)
        pulled_messages = list(itertools.chain.from_iterable(results))
        if not pulled_messages:
            logging.info('No pull calls had any messages, returning')
            break
        messages.extend(pulled_messages)
    return messages


def _ack_messages(messages, scrape_key):
    """Calls acknowledge on the list of messages in order to remove them
    from the queue.

    Args:
        messages: A list of pubsub messages (ReceivedMessage) which contain
            the ack_ids in them.
       scrape_key: (ScrapeKey): The scrape key to tell us which queue to
            retrieve from.
    """
    ack_ids = [message.ack_id for message in messages]
    pubsub_helper.get_subscriber().acknowledge(
        pubsub_helper.get_subscription_path(
            scrape_key, pubsub_type=BATCH_PUBSUB_TYPE), ack_ids)


def _get_proto_from_messages(messages):
    """Merges an ingest_info_proto from all of the batched messages.

    Args:
        messages: A list of pubsub messages (ReceivedMessage) which contain
            the ack_ids in them.
    Returns:
        an IngestInfo proto with data from all of the messages.
    """
    logging.info('Starting generation of proto')
    base_proto = ingest_info_pb2.IngestInfo()
    successful_tasks = set()
    failed_tasks = {}
    for message in messages:
        batch_message = BatchMessage.from_serializable(
            json.loads(message.message.data.decode()))
        # We do this because dicts are not hashable in python and we want to
        # avoid an n2 operation to see which tasks have been seen previously
        # which can be on the order of a million operations.
        task_hash = hash(json.dumps(batch_message.task.to_serializable(),
                                    sort_keys=True))
        if not batch_message.error and task_hash not in successful_tasks:
            successful_tasks.add(task_hash)
            if task_hash in failed_tasks:
                del failed_tasks[task_hash]
            task_proto = ingest_utils.convert_ingest_info_to_proto(
                batch_message.ingest_info)
            _append_to_proto(base_proto, task_proto)
        else:
            # We only add to failed if we didn't see a successful one.  This is
            # because its possible a task ran 3 times before passing, meaning we
            # don't want to fail on that when we see the failed ones.
            if task_hash not in successful_tasks:
                failed_tasks[task_hash] = batch_message
    logging.info('Generated proto for %s people', len(base_proto.people))
    return base_proto, failed_tasks


def _should_abort(failed_tasks, total_people):
    if (failed_tasks / total_people) >= FAILED_TASK_THRESHOLD:
        return True
    return False


def _append_to_proto(base, proto_to_append):
    base.people.extend(proto_to_append.people)
    base.bookings.extend(proto_to_append.bookings)
    base.charges.extend(proto_to_append.charges)
    base.arrests.extend(proto_to_append.arrests)
    base.holds.extend(proto_to_append.holds)
    base.bonds.extend(proto_to_append.bonds)
    base.sentences.extend(proto_to_append.sentences)


def write(ingest_info: IngestInfo, task: Task, scrape_key: ScrapeKey):
    """Batches up the writes using pubsub"""
    batch_message = BatchMessage(
        ingest_info=ingest_info,
        task=task,
    )
    _publish_batch_message(batch_message, scrape_key)


def write_error(
        error: str, trace_id: Optional[str], task: Task, scrape_key: ScrapeKey):
    """Batches up the errors using pubsub"""
    batch_message = BatchMessage(
        error=error,
        trace_id=trace_id,
        task=task,
    )
    _publish_batch_message(batch_message, scrape_key)


def persist_to_database(region_code, scrape_type, scraper_start_time):
    """Reads all of the messages on the pubsub queue for a region and persists
    them to the database.
    """
    region = regions.get_region(region_code)
    overrides = region.get_enum_overrides()
    scrape_key = ScrapeKey(region_code, scrape_type)

    messages = _get_batch_messages(scrape_key)

    logging.info('Received %s total messages', len(messages))
    if messages:
        proto, failed_tasks = _get_proto_from_messages(messages)

        for batch_message in failed_tasks.values():
            logging.error(
                'Task with trace_id %s failed with error %s',
                batch_message.trace_id, batch_message.error
            )
        if _should_abort(len(failed_tasks), len(proto.people)):
            logging.error(
                'Too many scraper tasks failed(%s), aborting write',
                len(failed_tasks))
            return False

        metadata = IngestMetadata(
            region=region_code, jurisdiction_id=region.jurisdiction_id,
            ingest_time=scraper_start_time,
            enum_overrides=overrides)

        did_write = persistence.write(proto, metadata)
        return did_write

    logging.error('No messages received from pubpub')
    return False


@batch_blueprint.route('/read_and_persist')
@authenticate_request
def read_and_persist():
    """Reads all of the messages on the pubsub queue for a region and persists
    them to the database.
    """
    region = request.args.get('region')
    with monitoring.push_tags({monitoring.TagKey.REGION: region}):
        session = sessions.get_most_recent_completed_session(
            region, ScrapeType.BACKGROUND)
        scrape_type = session.scrape_type
        scraper_start_time = session.start

        try:
            did_persist = persist_to_database(
                region, scrape_type, scraper_start_time)
        except:
            raise BatchPersistError(region, scrape_type)

        if did_persist:
            next_phase = scrape_phase.next_phase(request.endpoint)
            if next_phase:
                logging.info('Enqueueing %s for region %s.', region, next_phase)
                queues.enqueue_scraper_phase(
                    region_code=region, url=url_for(next_phase))
            return '', HTTPStatus.OK
        return '', HTTPStatus.ACCEPTED
