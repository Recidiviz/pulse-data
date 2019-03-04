# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
import datetime
import json
from typing import Optional

import attr
import cattr

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.utils import pubsub_helper

PUBSUB_TYPE = 'scraper_batch'


@attr.s(frozen=True)
class BatchMessage:
    """A wrapper around a message to publish so we can batch up the writes.

    This is the object that is serialized and put on the pubsub queue.
    """
    # The task which published this message.  We use this to dedupe messages
    # That failed some number of times before finally passing, or to not
    # double count tasks that failed more than once.
    task: Task = attr.ib()

    # The time at which this scraper was started, used to associate the entities
    # ingested during throughout the scrape
    scraper_start_time: Optional[datetime.datetime] = attr.ib(default=None)

    # The ingest info object that was batched up for a write.
    ingest_info: Optional[IngestInfo] = attr.ib(default=None)

    # The error type of the task if it ended in failure.
    error: Optional[str] = attr.ib(default=None)

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
        return pubsub_helper.get_publisher().publish(
            pubsub_helper.get_topic_path(scrape_key,
                                         pubsub_type=PUBSUB_TYPE),
            data=json.dumps(serialized).encode())
    pubsub_helper.retry_with_create(scrape_key, publish, PUBSUB_TYPE)


def write(ingest_info: IngestInfo, scraper_start_time: datetime.datetime,
          task: Task, scrape_key: ScrapeKey):
    """Batches up the writes using pubsub"""
    batch_message = BatchMessage(
        ingest_info=ingest_info,
        scraper_start_time=scraper_start_time,
        task=task,
    )
    _publish_batch_message(batch_message, scrape_key)


def write_error(error: str, task: Task, scrape_key: ScrapeKey):
    """Batches up the errors using pubsub"""
    batch_message = BatchMessage(
        error=error,
        task=task,
    )
    _publish_batch_message(batch_message, scrape_key)
