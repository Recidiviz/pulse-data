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
"""Helpers to use pubsub"""

# We only lease tasks for 5min, so that they pop back into the queue
# if we pause or stop the scrape for very long.
# Note: This may be the right number for us_ny snapshot scraping, but
#   if reused with another scraper the background scrapes might need
#   more time depending on e.g. # results for query 'John Doe'.
import logging
import time
from typing import Any, Callable, TypeVar

from google.api_core import exceptions  # pylint: disable=no-name-in-module
from google.cloud import pubsub
from google.protobuf import json_format

from recidiviz.common.common_utils import retry_grpc
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.utils import environment, metadata

ACK_DEADLINE_SECONDS = 300
NUM_GRPC_RETRIES = 2

# https://cloud.google.com/pubsub/docs/push#receive_push
MESSAGE = "message"

# https://cloud.google.com/storage/docs/pubsub-notifications
BUCKET_ID = "bucketId"
OBJECT_ID = "objectId"


_publisher = None
_subscriber = None


def get_publisher() -> pubsub.PublisherClient:
    global _publisher
    if not _publisher:
        _publisher = pubsub.PublisherClient()
    return _publisher


@environment.test_only
def clear_publisher() -> None:
    global _publisher
    _publisher = None


def get_subscriber() -> pubsub.SubscriberClient:
    global _subscriber
    if not _subscriber:
        _subscriber = pubsub.SubscriberClient()
    return _subscriber


@environment.test_only
def clear_subscriber() -> None:
    global _subscriber
    _subscriber = None


def get_topic_path(scrape_key: ScrapeKey, pubsub_type: str) -> str:
    return get_publisher().topic_path(
        metadata.project_id(),
        f"v1.{pubsub_type}.{scrape_key.region_code}-{scrape_key.scrape_type}",
    )


def get_subscription_path(scrape_key: ScrapeKey, pubsub_type: str) -> str:
    return get_subscriber().subscription_path(
        metadata.project_id(),
        f"v1.{pubsub_type}.{scrape_key.region_code}-{scrape_key.scrape_type}",
    )


def create_topic_and_subscription(scrape_key: ScrapeKey, pubsub_type: str) -> None:
    topic_path = get_topic_path(scrape_key, pubsub_type)
    try:
        logging.info("Creating pubsub topic: '%s'", topic_path)
        retry_grpc(NUM_GRPC_RETRIES, get_publisher().create_topic, name=topic_path)
    except exceptions.AlreadyExists:
        logging.info("Topic already exists")

    # A race condition exists sometimes where the topic doesn't exist yet and
    # therefore fails to make the subscription.
    time.sleep(1)
    subscription_path = get_subscription_path(scrape_key, pubsub_type)
    try:
        logging.info("Creating pubsub subscription: '%s'", subscription_path)
        retry_grpc(
            NUM_GRPC_RETRIES,
            get_subscriber().create_subscription,
            name=subscription_path,
            topic=topic_path,
            ack_deadline_seconds=ACK_DEADLINE_SECONDS,
        )
    except exceptions.AlreadyExists:
        logging.info("Subscription already exists")


ReturnType = TypeVar("ReturnType")


def retry_with_create(
    scrape_key: ScrapeKey, fn: Callable[..., ReturnType], pubsub_type: str
) -> ReturnType:
    try:
        result = retry_grpc(NUM_GRPC_RETRIES, fn)
    except exceptions.NotFound:
        create_topic_and_subscription(scrape_key, pubsub_type=pubsub_type)
        result = retry_grpc(NUM_GRPC_RETRIES, fn)
    return result


def publish_message_to_topic(message: str, topic: str) -> None:
    logging.info("Publishing message: '%s' to topic: %s", message, topic)
    publisher = get_publisher()
    topic_path = publisher.topic_path(metadata.project_id(), topic)
    publisher.publish(topic_path, data=message.encode("utf-8"))


def extract_pubsub_message_from_json(json: Any) -> pubsub.types.PubsubMessage:
    if not isinstance(json, dict):
        raise TypeError("Invalid Pub/Sub message")
    if MESSAGE not in json:
        raise ValueError("Invalid Pub/Sub message")

    message = json_format.ParseDict(json[MESSAGE], pubsub.types.PubsubMessage())

    return message
