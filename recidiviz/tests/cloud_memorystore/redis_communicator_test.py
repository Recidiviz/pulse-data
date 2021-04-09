# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
""" Tests for the RedisCommunicator """
import threading
import time
import uuid
from unittest import TestCase, mock
from unittest.mock import MagicMock

import fakeredis

from recidiviz.cloud_memorystore.redis_communicator import (
    RedisCommunicator,
    MessageKind,
    RedisCommunicatorMessage,
)


class TestRedisCommunicator(TestCase):
    """TestCase for RedisCommunicator."""

    def setUp(self) -> None:
        self.cache = fakeredis.FakeRedis()
        self.max_messages = 3
        self.communicator = RedisCommunicator.create(
            self.cache, max_messages=self.max_messages
        )

    def test_communicate(self) -> None:
        message = self.communicator.communicate("a message", kind=MessageKind.CLOSE)

        self.assertEqual(
            message,
            RedisCommunicatorMessage(
                cursor=1, data="a message", kind=MessageKind.CLOSE
            ),
        )

    def test_max_messages(self) -> None:
        for r in range(self.max_messages + 7):
            self.communicator.communicate(str(r))

        self.assertEqual(
            self.cache.zcard(self.communicator.channel_cache_key), self.max_messages
        )

    def test_length(self) -> None:
        self.communicator.communicate("first message")
        self.communicator.communicate("second message")

    def test_latest_message(self) -> None:
        self.assertEqual(self.communicator.latest_message, None)
        message = self.communicator.communicate("first message")
        self.assertEqual(
            message,
            RedisCommunicatorMessage(cursor=1, data="first message"),
        )
        self.assertEqual(
            self.communicator.latest_message,
            message,
        )

    def test_close(self) -> None:
        self.assertEqual(self.communicator.closed, False)
        self.communicator.communicate("first message")
        self.assertEqual(self.communicator.closed, False)
        self.communicator.communicate("second message", kind=MessageKind.CLOSE)
        self.assertEqual(self.communicator.closed, True)

    def test_listen_timeout(self) -> None:
        # Times out when no messages are passed
        with self.assertRaises(TimeoutError):
            list(self.communicator.listen(2, timeout=1))

    def test_listen(self) -> None:
        self.communicator.communicate("first message")

        def add_second_message() -> None:
            time.sleep(0.5)
            self.communicator.communicate("second message")

        threading.Thread(target=add_second_message).start()

        # Yields latest messages when it arrives
        self.assertEqual(
            list(self.communicator.listen(1, timeout=2)),
            [
                RedisCommunicatorMessage(cursor=2, data="second message"),
            ],
        )

    def test_listen_after_closed(self) -> None:
        first_message = self.communicator.communicate("first message")
        second_message = self.communicator.communicate(
            "second message", kind=MessageKind.CLOSE
        )

        # We can listen for updates on a channel even once its closed.
        # Caller should detect the closed message and not send any further listening requests
        self.assertEqual(
            list(self.communicator.listen(first_message.cursor)), [second_message]
        )

    def test_listen_expiry(self) -> None:
        """ Channels are closed automatically if no new messages are added within the expiry """

        def expire_cache() -> None:
            time.sleep(0.5)
            self.cache.delete(self.communicator.channel_cache_key)

        threading.Thread(target=expire_cache).start()

        with self.assertRaises(TimeoutError):
            # Execute generator
            list(self.communicator.listen(1, timeout=1))

    @mock.patch("uuid.uuid4")
    def test_create(self, mock_uuid: MagicMock) -> None:
        taken_uuid = uuid.UUID("8367379ff8674b04adfb9b595b277dc3")
        new_uuid = uuid.UUID("341f826e69054da4b2f2340abd0bf279")
        mock_uuid.side_effect = [taken_uuid, new_uuid]

        # The UUID has already been taken
        self.cache.set(RedisCommunicator.build_channel_cache_key(taken_uuid), "taken")

        # We are returned a RedisCommunicator for the new channel
        communicator = RedisCommunicator.create(self.cache)
        communicator.communicate("first message")

        self.assertEqual(
            self.cache.zcard(RedisCommunicator.build_channel_cache_key(new_uuid)), 1
        )
