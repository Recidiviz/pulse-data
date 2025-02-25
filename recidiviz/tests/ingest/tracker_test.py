# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

import pytest
import pytz
from google.api_core import datetime_helpers  # pylint: disable=no-name-in-module
from google.cloud.pubsub_v1 import types
from google.protobuf import timestamp_pb2  # pylint: disable=no-name-in-module
from mock import call, patch

from recidiviz.ingest import constants, docket, sessions, tracker
from recidiviz.ingest.models.scrape_key import ScrapeKey


class TestTracker:
    """Tests for the methods in the module."""

    @patch('recidiviz.ingest.docket.get_new_docket_item')
    @patch('recidiviz.ingest.sessions.add_docket_item_to_current_session')
    def test_iterate_docket_item(self, mock_session, mock_docket):
        mock_session.return_value = True
        mock_docket.return_value = create_pubsub_message(get_payload())

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert payload == get_payload()

    @patch('recidiviz.ingest.docket.get_new_docket_item')
    @patch('recidiviz.ingest.sessions.add_docket_item_to_current_session')
    def test_iterate_docket_item_no_open_session_to_update(
            self, mock_session, mock_docket):
        mock_session.return_value = False
        mock_docket.return_value = create_pubsub_message(get_payload())

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert not payload

    @patch('recidiviz.ingest.docket.get_new_docket_item')
    def test_iterate_docket_item_no_matching_items(self, mock_docket):
        mock_docket.return_value = None

        payload = tracker.iterate_docket_item(
            ScrapeKey("us_fl", constants.BACKGROUND_SCRAPE))
        assert not payload

    @patch('recidiviz.ingest.docket.ack_docket_item')
    @patch('recidiviz.ingest.sessions.remove_docket_item_from_session')
    @patch('recidiviz.ingest.sessions.get_current_session')
    def test_remove_item_from_session_and_docket(
            self, mock_current, mock_remove, mock_ack):
        scrape_key = ScrapeKey("us_va", constants.BACKGROUND_SCRAPE)

        mock_current.return_value = 'us_va_1'
        mock_remove.return_value = 'a'

        tracker.remove_item_from_session_and_docket(scrape_key)

        mock_ack.assert_called_with(scrape_key, 'a')

    @patch('recidiviz.ingest.docket.ack_docket_item')
    @patch('recidiviz.ingest.sessions.remove_docket_item_from_session')
    @patch('recidiviz.ingest.sessions.get_current_session')
    def test_remove_item_from_session_and_docket_no_open_sessions(
            self, mock_current, mock_remove, mock_ack):
        scrape_key = ScrapeKey("us_va", constants.BACKGROUND_SCRAPE)
        mock_current.return_value = None
        mock_remove.return_value = None

        tracker.remove_item_from_session_and_docket(scrape_key)

        mock_ack.assert_not_called()

    @patch('recidiviz.ingest.docket.purge_query_docket')
    @patch('recidiviz.ingest.sessions.remove_docket_item_from_session')
    @patch('recidiviz.ingest.sessions.get_sessions_with_leased_docket_items')
    def test_purge_docket_and_session(
            self, mock_sessions, mock_remove, mock_purge):
        scrape_key = ScrapeKey("us_va", constants.BACKGROUND_SCRAPE)
        mock_sessions.return_value = ['us_va_1', 'us_va_2']

        tracker.purge_docket_and_session(scrape_key)

        mock_purge.assert_called_with(scrape_key)
        mock_remove.assert_has_calls([call('us_va_1'), call('us_va_2')])


def get_payload():
    return [{'name': 'Jacoby, Mackenzie'}, {'name': 'Jacoby, Clementine'}]


PUBLISHED_SECONDS = datetime_helpers.to_milliseconds(
    datetime(2012, 4, 21, 15, 0, tzinfo=pytz.utc)) // 1000

def create_pubsub_message(content, ack_id="ACKID", published=PUBLISHED_SECONDS,
                          **attrs):
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
    def setup_method(self, _test_method):
        self.sessions_to_delete = []

    def teardown_method(self, _test_method):
        for region in REGIONS:
            docket.purge_query_docket(ScrapeKey(region,
                                                constants.BACKGROUND_SCRAPE))
        sessions.ds().delete_multi(self.sessions_to_delete)

    def test_iterate_docket_item(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.BACKGROUND_SCRAPE)

        self.create_session(scrape_key)
        docket.add_to_query_docket(scrape_key, get_payload()[0]).result()
        docket.add_to_query_docket(scrape_key, get_payload()[1]).result()

        assert tracker.iterate_docket_item(scrape_key) == get_payload()[0]
        assert tracker.iterate_docket_item(scrape_key) == get_payload()[1]

    def test_iterate_docket_item_no_open_session_to_update(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.BACKGROUND_SCRAPE)

        docket.add_to_query_docket(scrape_key, get_payload()).result()

        assert not tracker.iterate_docket_item(scrape_key)

    def test_iterate_docket_item_no_matching_items(self):
        docket_key = ScrapeKey(REGIONS[0], constants.BACKGROUND_SCRAPE)
        docket.add_to_query_docket(docket_key, get_payload()).result()

        session_key = ScrapeKey(REGIONS[1], constants.BACKGROUND_SCRAPE)
        self.create_session(session_key)
        assert not tracker.iterate_docket_item(session_key)

    def test_remove_item_from_session_and_docket(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.BACKGROUND_SCRAPE)
        docket.add_to_query_docket(scrape_key, get_payload()).result()
        self.create_session(scrape_key)
        tracker.iterate_docket_item(scrape_key)

        tracker.remove_item_from_session_and_docket(scrape_key)

        assert not sessions.get_current_session(scrape_key).docket_ack_id

    def test_purge_docket_and_session(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.BACKGROUND_SCRAPE)
        docket.add_to_query_docket(scrape_key, get_payload()).result()
        self.create_session(scrape_key)

        tracker.purge_docket_and_session(scrape_key)

        assert not tracker.iterate_docket_item(scrape_key)

    def create_session(self, scrape_key):
        session = sessions.ScrapeSession.new(
            key=sessions.ds().key(sessions.SCRAPE_SESSION_KIND),
            region=scrape_key.region_code, scrape_type=scrape_key.scrape_type)
        sessions.ds().put(session.to_entity())
        self.sessions_to_delete.append(session.to_entity().key)
