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

"""Tests for ingest/tracker.py."""
import json
from datetime import datetime
from typing import Any, Callable, Dict, List

import pytest
import pytz
from google.api_core import datetime_helpers  # pylint: disable=no-name-in-module
from google.cloud import datastore
from google.cloud.pubsub_v1 import types
from google.protobuf import timestamp_pb2  # pylint: disable=no-name-in-module
from mock import Mock, call, patch

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, docket, scrape_phase, sessions, tracker
from recidiviz.utils import pubsub_helper


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
class TestTracker:
    """Tests for the methods in the module."""

    @patch("recidiviz.ingest.scrape.docket.get_new_docket_item")
    @patch("recidiviz.ingest.scrape.sessions" ".add_docket_item_to_current_session")
    def test_iterate_docket_item(self, mock_session: Mock, mock_docket: Mock) -> None:
        mock_session.return_value = True
        mock_docket.return_value = create_pubsub_message(get_payload())

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )
        assert payload == get_payload()

    @patch("recidiviz.ingest.scrape.docket.get_new_docket_item")
    @patch("recidiviz.ingest.scrape.sessions" ".add_docket_item_to_current_session")
    def test_iterate_docket_item_no_open_session_to_update(
        self, mock_session: Mock, mock_docket: Mock
    ) -> None:
        mock_session.return_value = False
        mock_docket.return_value = create_pubsub_message(get_payload())

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )
        assert not payload

    @patch("recidiviz.ingest.scrape.docket.get_new_docket_item")
    def test_iterate_docket_item_no_matching_items(self, mock_docket: Mock) -> None:
        mock_docket.return_value = None

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_fl", constants.ScrapeType.BACKGROUND)
        )
        assert not payload

    @patch("recidiviz.ingest.scrape.docket.ack_docket_item")
    @patch("recidiviz.ingest.scrape.sessions.remove_docket_item_from_session")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_remove_item_from_session_and_docket(
        self, mock_current: Mock, mock_remove: Mock, mock_ack: Mock
    ) -> None:
        scrape_key = ScrapeKey("us_va", constants.ScrapeType.BACKGROUND)

        mock_current.return_value = "us_va_1"
        mock_remove.return_value = "a"

        tracker.remove_item_from_session_and_docket(scrape_key)

        mock_ack.assert_called_with(scrape_key, "a")

    @patch("recidiviz.ingest.scrape.docket.ack_docket_item")
    @patch("recidiviz.ingest.scrape.sessions.remove_docket_item_from_session")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_remove_item_from_session_and_docket_no_open_sessions(
        self, mock_current: Mock, mock_remove: Mock, mock_ack: Mock
    ) -> None:
        scrape_key = ScrapeKey("us_va", constants.ScrapeType.BACKGROUND)
        mock_current.return_value = None
        mock_remove.return_value = None

        tracker.remove_item_from_session_and_docket(scrape_key)

        mock_ack.assert_not_called()

    @patch("recidiviz.ingest.scrape.docket.purge_query_docket")
    @patch("recidiviz.ingest.scrape.sessions.remove_docket_item_from_session")
    @patch("recidiviz.ingest.scrape.sessions" ".get_sessions_with_leased_docket_items")
    def test_purge_docket_and_session(
        self, mock_sessions: Mock, mock_remove: Mock, mock_purge: Mock
    ) -> None:
        scrape_key = ScrapeKey("us_va", constants.ScrapeType.BACKGROUND)
        mock_sessions.return_value = ["us_va_1", "us_va_2"]

        tracker.purge_docket_and_session(scrape_key)

        mock_purge.assert_called_with(scrape_key)
        mock_remove.assert_has_calls([call("us_va_1"), call("us_va_2")])


def get_payload() -> List[Dict[str, str]]:
    return [{"name": "Jacoby, Mackenzie"}, {"name": "Jacoby, Clementine"}]


PUBLISHED_SECONDS = (
    datetime_helpers.to_milliseconds(datetime(2012, 4, 21, 15, 0, tzinfo=pytz.utc))
    // 1000
)


def create_pubsub_message(
    content: List[Dict[str, Any]],
    ack_id: str = "ACKID",
    published: int = PUBLISHED_SECONDS,
    **attrs: Any
) -> types.ReceivedMessage:
    return types.ReceivedMessage(
        message=types.PubsubMessage(
            attributes=attrs,
            data=json.dumps(content).encode(),
            message_id="message_id",
            publish_time=timestamp_pb2.Timestamp(seconds=published),
        ),
        ack_id=ack_id,
    )


REGIONS = ["us_ny", "us_va"]


@pytest.mark.usefixtures("emulator")
class TestTrackerIntegration:
    """Tests for how the tracker integrates with sessions and the docket

    These are similar to the tests above, but use the emulators to verify that
    the systems work together correctly.
    """

    def setup_method(self, _test_method: Callable) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "test-project"
        self.sessions_to_delete: List[datastore.key.Key] = []
        for region in REGIONS:
            pubsub_helper.create_topic_and_subscription(
                ScrapeKey(region, constants.ScrapeType.BACKGROUND), docket.PUBSUB_TYPE
            )

    def teardown_method(self, _test_method: Callable) -> None:
        for region in REGIONS:
            docket.purge_query_docket(
                ScrapeKey(region, constants.ScrapeType.BACKGROUND)
            )
        sessions.ds().delete_multi(self.sessions_to_delete)
        self.project_id_patcher.stop()

    def test_iterate_docket_item(self) -> None:
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        self.create_session(scrape_key)
        docket.add_to_query_docket(scrape_key, get_payload()[0]).result()
        docket.add_to_query_docket(scrape_key, get_payload()[1]).result()

        assert tracker.iterate_docket_item(scrape_key) == get_payload()[0]
        assert tracker.iterate_docket_item(scrape_key) == get_payload()[1]

    def test_iterate_docket_item_no_open_session_to_update(self) -> None:
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        docket.add_to_query_docket(scrape_key, get_payload()).result()

        assert not tracker.iterate_docket_item(scrape_key)

    def test_iterate_docket_item_no_matching_items(self) -> None:
        docket_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        docket.add_to_query_docket(docket_key, get_payload()).result()

        session_key = ScrapeKey(REGIONS[1], constants.ScrapeType.BACKGROUND)
        self.create_session(session_key)
        assert not tracker.iterate_docket_item(session_key)

    def test_remove_item_from_session_and_docket(self) -> None:
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        docket.add_to_query_docket(scrape_key, get_payload()).result()
        self.create_session(scrape_key)
        tracker.iterate_docket_item(scrape_key)

        tracker.remove_item_from_session_and_docket(scrape_key)

        scrape_session = sessions.get_current_session(scrape_key)
        assert scrape_session is not None
        assert not scrape_session.docket_ack_id

    def test_purge_docket_and_session(self) -> None:
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        docket.add_to_query_docket(scrape_key, get_payload()).result()
        self.create_session(scrape_key)

        tracker.purge_docket_and_session(scrape_key)

        assert not tracker.iterate_docket_item(scrape_key)

    def create_session(self, scrape_key: ScrapeKey) -> None:
        session = sessions.ScrapeSession.new(
            key=sessions.ds().key(sessions.SCRAPE_SESSION_KIND),
            region=scrape_key.region_code,
            scrape_type=scrape_key.scrape_type,
            phase=scrape_phase.ScrapePhase.START,
        )
        sessions.ds().put(session.to_entity())
        self.sessions_to_delete.append(session.to_entity().key)
