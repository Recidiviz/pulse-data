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
import time
import uuid
from datetime import datetime, timedelta
from typing import Generator, Optional
from uuid import UUID

import attr
import redis


class MessageKind(enum.Enum):
    """ MessageKind enum, used for determining when to open/update/close communication channels """

    UPDATE = "update"
    CLOSE = "close"


@attr.s
class RedisCommunicatorMessage:
    """ Dataclass for RedisCommunicator messages """

    cursor: int = attr.ib()
    data: str = attr.ib()
    kind: MessageKind = attr.ib(default=MessageKind.UPDATE)

    def to_json(self) -> str:
        """ Converts the message to json"""
        message = attr.asdict(self)
        message["kind"] = self.kind.value

        return json.dumps(message)

    @staticmethod
    def from_json(json_string: Optional[bytes]) -> Optional["RedisCommunicatorMessage"]:
        """ Builds a message from JSON """
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
        self.channel_cache_key = self.build_channel_cache_key(channel_uuid)

    @property
    def length(self) -> int:
        """ Returns the length of the message list. If the key does not exist, returns 0"""
        try:
            return self.cache.llen(self.channel_cache_key)
        except redis.exceptions.ResponseError:
            return 0

    @property
    def latest_message(self) -> Optional[RedisCommunicatorMessage]:
        """ Returns the latest message if it exists, otherwise None """
        try:
            return RedisCommunicatorMessage.from_json(
                self.cache.lindex(self.channel_cache_key, -1)
            )
        except redis.exceptions.ResponseError:
            return None

    @property
    def closed(self) -> bool:
        """ Returns whether the communication channel has been closed"""
        return (
            self.latest_message is not None
            and self.latest_message.kind == MessageKind.CLOSE
        )

    def communicate(self, data: str, kind: MessageKind = MessageKind.UPDATE) -> None:
        """Adds a new message to the communication channel
        Does nothing if the the latest message contains the same data
        Communication channels expire MESSAGE_CACHE_EXPIRY_SECONDS after the last message was added
        """
        if self.closed:
            raise ValueError("Cannot communicate on a closed channel")

        if self.latest_message and self.latest_message.data == data:
            return

        message = RedisCommunicatorMessage(
            cursor=self.length,
            kind=kind,
            data=data,
        )

        self.cache.rpush(self.channel_cache_key, message.to_json())
        self.cache.expire(self.channel_cache_key, MESSAGE_CACHE_EXPIRY_SECONDS)

    def listen(
        self, current_message_cursor: int, timeout: int = MESSAGE_LISTEN_TIMEOUT_SECONDS
    ) -> Generator[Optional[RedisCommunicatorMessage], None, None]:
        """Listens for new messages in the channel.
        Yields the latest message whenever a new message is published to the communication channel
        Args:
            current_message_cursor int: The listener's current offset in the communication channel
        """
        timeout_time = datetime.now() + timedelta(seconds=timeout)

        while not self.closed:
            if datetime.now() > timeout_time:
                raise TimeoutError(
                    f"Timed out waiting for messages on channel: {self.channel_cache_key}"
                )

            next_message_cursor = self.length - 1
            if next_message_cursor > current_message_cursor:
                yield self.latest_message
                break

            time.sleep(MESSAGE_LISTEN_INTERVAL)

    @classmethod
    def build_channel_cache_key(cls, channel_uuid: UUID) -> str:
        """ Builds the """
        return f"communication-channel-{str(channel_uuid)}"

    @staticmethod
    def create(cache: redis.Redis) -> "RedisCommunicator":
        """ Builds a new communication channel """
        while True:
            channel_uuid = uuid.uuid4()
            channel_cache_key = RedisCommunicator.build_channel_cache_key(channel_uuid)

            if not cache.exists(channel_cache_key):
                break

        return RedisCommunicator(cache, channel_uuid)
