# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Communication bus via Redis """
import enum
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Optional
from uuid import UUID

import attr
import redis


class MessageKind(enum.Enum):
    """MessageKind enum, used for determining when to open/update/close communication channels"""

    UPDATE = "update"
    CLOSE = "close"


@attr.s
class RedisCommunicatorMessage:
    """Dataclass for RedisCommunicator messages"""

    # The cursor can be passed to `RedisCommunicator.listen()` to query for the next message
    cursor: int = attr.ib()
    data: str = attr.ib()
    kind: MessageKind = attr.ib(default=MessageKind.UPDATE)

    def to_json(self) -> Dict[str, Any]:
        """Converts the message to json"""
        message = attr.asdict(self)
        message["kind"] = self.kind.value
        return message

    @staticmethod
    def from_json(json_string: Optional[bytes]) -> Optional["RedisCommunicatorMessage"]:
        """Builds a message from JSON"""
        if not json_string:
            return None

        try:
            message = json.loads(json_string)
        except json.JSONDecodeError:
            return None

        message["kind"] = MessageKind(message["kind"])

        return RedisCommunicatorMessage(**message)


MESSAGE_CACHE_EXPIRY_SECONDS = 60 * 60  # 1 hour
MESSAGE_LISTEN_TIMEOUT_SECONDS = 60 * 3  # 3 minutes
MESSAGE_LISTEN_INTERVAL = 1.0 / 8  # 8 times / second


class RedisCommunicator:
    """
    The RedisCommunicator facilitates messaging between processes by using a shared key
    - Communication channels are opened when a message is first sent to them
    - Communication channels are explicitly closed when a MessageKind.CLOSE is sent
    """

    CACHE_EXPIRY_SECONDS = 60 * 60  # 1 hour

    def __init__(self, cache: redis.Redis, channel_uuid: UUID):
        self.cache = cache
        self.channel_uuid = channel_uuid
        self.channel_cache_key = self.build_channel_cache_key(channel_uuid)
        self.cursor_cache_key = self.build_cursor_cache_key(channel_uuid)
        self.options_cache_key = self.build_options_cache_key(channel_uuid)
        options = cache.get(self.options_cache_key)
        if options is None:
            raise ValueError(
                "Cannot instantiate without options set; did you open the channel with .create()?"
            )

        self.options = json.loads(options)
        self.max_messages = self.options["max_messages"]

    def increment_cursor(self) -> int:
        """Increments the cursor"""
        return self.cache.incr(self.cursor_cache_key)

    @property
    def latest_message(self) -> Optional[RedisCommunicatorMessage]:
        """Returns the latest message if it exists, otherwise None"""
        try:
            # Get the message with the highest cursor
            messages = self.cache.zrange(self.channel_cache_key, -1, -1)

            if not messages:
                return None

            return RedisCommunicatorMessage.from_json(messages[0])
        except redis.exceptions.ResponseError as e:
            logging.exception(e)
            return None

    @property
    def closed(self) -> bool:
        """Returns whether the communication channel has been closed"""
        return (
            self.latest_message is not None
            and self.latest_message.kind == MessageKind.CLOSE
        )

    def communicate(
        self, data: str, kind: MessageKind = MessageKind.UPDATE
    ) -> RedisCommunicatorMessage:
        """Adds a new message to the communication channel
        Communication channels expire MESSAGE_CACHE_EXPIRY_SECONDS after the last message was added
        """
        if self.closed:
            raise ValueError("Cannot communicate on a closed channel")

        message = RedisCommunicatorMessage(
            cursor=self.increment_cursor(),
            kind=kind,
            data=data,
        )

        pipeline = self.cache.pipeline()

        pipeline.zadd(
            self.channel_cache_key, {json.dumps(message.to_json()): message.cursor}
        )
        pipeline.expire(self.channel_cache_key, MESSAGE_CACHE_EXPIRY_SECONDS)
        pipeline.expire(self.cursor_cache_key, MESSAGE_CACHE_EXPIRY_SECONDS)
        pipeline.expire(self.options_cache_key, MESSAGE_CACHE_EXPIRY_SECONDS)

        if self.max_messages is not None:
            # Trim the sorted set to the `self.max_messages` most recent
            pipeline.zremrangebyrank(
                self.channel_cache_key, 0, -(self.max_messages + 1)
            )

        pipeline.execute()

        return message

    def listen(
        self, listener_cursor: int, timeout: int = MESSAGE_LISTEN_TIMEOUT_SECONDS
    ) -> Generator[RedisCommunicatorMessage, None, None]:
        """Listens for new messages in the channel.
        Yields the latest message whenever a new message is published to the communication channel
        Args:
            listener_cursor int: The listener's current offset in the communication channel
        """
        timeout_time = datetime.now() + timedelta(seconds=timeout)

        while not self.closed:
            if datetime.now() > timeout_time:
                raise TimeoutError(
                    f"Timed out waiting for messages on channel: {self.channel_cache_key}"
                )

            time.sleep(MESSAGE_LISTEN_INTERVAL)

            if self.latest_message and self.latest_message.cursor > listener_cursor:
                break

        latest_message = self.latest_message

        if latest_message:
            yield latest_message

    @classmethod
    def build_channel_cache_key(cls, channel_uuid: UUID) -> str:
        """Builds the cache key for the channel"""
        return f"communication-channel-{str(channel_uuid)}"

    @classmethod
    def build_cursor_cache_key(cls, channel_uuid: UUID) -> str:
        """Builds the cache key for the cursor"""
        return f"communication-cursor-{str(channel_uuid)}"

    @classmethod
    def build_options_cache_key(cls, channel_uuid: UUID) -> str:
        """Builds the cache key for the channel options"""
        return f"communication-options-{str(channel_uuid)}"

    @classmethod
    def create(
        cls, cache: redis.Redis, *, max_messages: Optional[int] = None
    ) -> "RedisCommunicator":
        """Builds a new communication channel"""
        while True:
            channel_uuid = uuid.uuid4()
            channel_cache_key = cls.build_channel_cache_key(channel_uuid)

            if not cache.exists(channel_cache_key):
                break

        options_cache_key = cls.build_options_cache_key(channel_uuid)
        cache.setex(
            options_cache_key,
            MESSAGE_CACHE_EXPIRY_SECONDS,
            json.dumps({"max_messages": max_messages}),
        )

        return RedisCommunicator(cache, channel_uuid)
