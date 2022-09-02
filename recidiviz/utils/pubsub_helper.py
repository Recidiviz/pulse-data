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
"""Helpers to use pubsub
"""

import json
import logging
from typing import Any

from google.cloud import pubsub

from recidiviz.utils import environment, metadata

# https://cloud.google.com/pubsub/docs/push#receive_push
MESSAGE = "message"

# https://cloud.google.com/storage/docs/pubsub-notifications
BUCKET_ID = "bucketId"
OBJECT_ID = "objectId"


_publisher = None


def get_publisher() -> pubsub.PublisherClient:
    global _publisher
    if not _publisher:
        _publisher = pubsub.PublisherClient()
    return _publisher


@environment.test_only
def clear_publisher() -> None:
    global _publisher
    _publisher = None


def publish_message_to_topic(message: str, topic: str) -> None:
    logging.info("Publishing message: '%s' to topic: %s", message, topic)
    publisher = get_publisher()
    topic_path = publisher.topic_path(metadata.project_id(), topic)
    publisher.publish(topic_path, data=message.encode("utf-8"))


def extract_pubsub_message_from_json(json_request: Any) -> pubsub.types.PubsubMessage:
    if not isinstance(json_request, dict):
        raise TypeError("Invalid Pub/Sub message")
    if MESSAGE not in json_request:
        raise ValueError("Invalid Pub/Sub message")

    try:
        message = pubsub.types.PubsubMessage.from_json(
            json.dumps(json_request[MESSAGE])
        )
    except Exception as e:
        logging.info("Exception parsing pubsub message: %s", str(e))
        raise e

    return message
