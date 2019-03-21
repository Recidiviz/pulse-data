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

from google.api_core import exceptions  # pylint: disable=no-name-in-module
from google.cloud import pubsub

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.utils import metadata, environment

ACK_DEADLINE_SECONDS = 300

_publisher = None
_subscriber = None


def get_publisher():
    global _publisher
    if not _publisher:
        _publisher = pubsub.PublisherClient()
    return _publisher


@environment.test_only
def clear_publisher():
    global _publisher
    _publisher = None


def get_subscriber():
    global _subscriber
    if not _subscriber:
        _subscriber = pubsub.SubscriberClient()
    return _subscriber


@environment.test_only
def clear_subscriber():
    global _subscriber
    _subscriber = None


def get_topic_path(scrape_key, pubsub_type):
    return get_publisher().topic_path(
        metadata.project_id(),
        "v1.{}.{}-{}".format(
            pubsub_type, scrape_key.region_code, scrape_key.scrape_type))


def get_subscription_path(scrape_key, pubsub_type):
    return get_subscriber().subscription_path(
        metadata.project_id(),
        "v1.{}.{}-{}".format(
            pubsub_type, scrape_key.region_code, scrape_key.scrape_type))


def create_topic_and_subscription(scrape_key, pubsub_type):
    topic_path = get_topic_path(scrape_key, pubsub_type)
    try:
        logging.info("Creating pubsub topic: '%s'", topic_path)
        get_publisher().create_topic(topic_path)
    except exceptions.AlreadyExists:
        logging.info("Topic already exists")

    # A race condition exists sometimes where the topic doesn't exist yet and
    # therefore fails to make the subscription.
    time.sleep(1)
    subscription_path = get_subscription_path(scrape_key, pubsub_type)
    try:
        logging.info("Creating pubsub subscription: '%s'", subscription_path)
        get_subscriber().create_subscription(
            subscription_path, topic_path,
            ack_deadline_seconds=ACK_DEADLINE_SECONDS)
    except exceptions.AlreadyExists:
        logging.info("Subscription already exists")


def retry_with_create(scrape_key, fn, pubsub_type):
    try:
        result = fn()
    except exceptions.NotFound:
        create_topic_and_subscription(
            scrape_key, pubsub_type=pubsub_type)
        result = fn()
    return result


def purge(scrape_key: ScrapeKey, pubsub_type: str):
    # TODO(#342): Use subscriber().seek(subscription_path, time=timestamp)
    # once available on the emulator.
    try:
        get_subscriber().delete_subscription(
            get_subscription_path(scrape_key, pubsub_type=pubsub_type))
    except exceptions.NotFound:
        pass

    create_topic_and_subscription(scrape_key, pubsub_type=pubsub_type)
