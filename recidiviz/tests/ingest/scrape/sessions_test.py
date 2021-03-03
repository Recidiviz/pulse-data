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

"""Tests for ingest/sessions.py."""


from datetime import datetime

import pytest
import pytz
from google.cloud import datastore
from mock import patch

from recidiviz.ingest.scrape import constants, sessions, scrape_phase
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape.sessions import ScrapeSession

fixed_now = datetime(2000, 1, 1)


class TestWriteSessions:
    """Tests for the create_session, close_session, and update_session methods
    in the module."""

    def setup_method(self, _test_method):
        sessions.clear_ds()

    def teardown_method(self, _test_method):
        sessions.clear_ds()

    @patch("google.cloud.datastore.Client")
    @patch("recidiviz.ingest.scrape.sessions.datetime")
    def test_create_session(self, mock_datetime, mock_client):
        mock_datetime.now.return_value = fixed_now

        # Must use a full key so that the entities are equal.
        key = datastore.key.Key("session", "key", project=0)

        client = mock_client.return_value
        client.key.return_value = key

        scrape_key = ScrapeKey("us_ok", constants.ScrapeType.SNAPSHOT)
        sessions.create_session(scrape_key)

        session = ScrapeSession.new(
            key=datastore.key.Key("session", "key", project=0),
            start=fixed_now,
            scrape_type=constants.ScrapeType.SNAPSHOT,
            region="us_ok",
            phase=scrape_phase.ScrapePhase.START,
        )
        client.put.assert_called_with(session.to_entity())

    @patch("google.cloud.datastore.Query")
    @patch("google.cloud.datastore.Client")
    @patch("recidiviz.ingest.scrape.sessions.datetime")
    def test_create_session_with_existing(self, mock_datetime, mock_client, mock_query):
        mock_datetime.now.return_value = fixed_now

        existing_session = ScrapeSession.new(
            key=datastore.key.Key("session", "existing", project=0),
            start=fixed_now,
            scrape_type=constants.ScrapeType.BACKGROUND,
            region="us_ny",
            phase=scrape_phase.ScrapePhase.START,
        )
        new_key = datastore.key.Key("session", "new", project=0)
        new_session = ScrapeSession.new(
            key=new_key,
            start=fixed_now,
            scrape_type=constants.ScrapeType.BACKGROUND,
            region="us_wy",
            phase=scrape_phase.ScrapePhase.START,
        )

        client = mock_client.return_value
        client.key.return_value = new_key
        wire_sessions_to_query(mock_client, mock_query, [existing_session])

        scrape_key = ScrapeKey("us_wy", constants.ScrapeType.BACKGROUND)
        sessions.create_session(scrape_key)

        existing_session.end = fixed_now
        client.put.assert_any_call(existing_session.to_entity())
        client.put.assert_any_call(new_session.to_entity())
        assert client.put.call_count == 2

    @patch("google.cloud.datastore.Query")
    @patch("google.cloud.datastore.Client")
    @patch("recidiviz.ingest.scrape.sessions.datetime")
    def test_update_session(self, mock_datetime, mock_client, mock_query):
        mock_datetime.now.return_value = fixed_now

        key = datastore.key.Key("session", "key", project=0)
        session = ScrapeSession.new(
            key,
            start=fixed_now,
            scrape_type=constants.ScrapeType.SNAPSHOT,
            region="us_sd",
            phase=scrape_phase.ScrapePhase.START,
        )

        wire_sessions_to_query(mock_client, mock_query, [session])

        scrape_key = ScrapeKey("us_sd", constants.ScrapeType.SNAPSHOT)
        assert sessions.update_session("CAMUS, ALBERT", scrape_key)

        session.last_scraped = "CAMUS, ALBERT"
        mock_client.return_value.put.assert_called_with(session.to_entity())

    @patch("google.cloud.datastore.Client")
    def test_update_session_nothing_current(self, _mock_client):
        scrape_key = ScrapeKey("us_sd", constants.ScrapeType.BACKGROUND)
        assert not sessions.update_session("VONNEGUT, KURT", scrape_key)

    @patch("google.cloud.datastore.Query")
    @patch("google.cloud.datastore.Client")
    @patch("recidiviz.ingest.scrape.sessions.datetime")
    def test_close_session(self, mock_datetime, mock_client, mock_query):
        mock_datetime.now.return_value = fixed_now

        key = datastore.key.Key("session", "key", project=0)
        session = ScrapeSession.new(
            key,
            start=fixed_now,
            scrape_type=constants.ScrapeType.SNAPSHOT,
            region="us_sd",
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )

        wire_sessions_to_query(mock_client, mock_query, [session])
        session.end_time = fixed_now

        scrape_key = ScrapeKey("us_sd", constants.ScrapeType.SNAPSHOT)
        assert to_entities(sessions.close_session(scrape_key)) == to_entities([session])

        mock_client.return_value.put.assert_called_with(session.to_entity())

    @patch("google.cloud.datastore.Client")
    def test_close_session_nothing_current(self, _mock_client):
        scrape_key = ScrapeKey("us_sd", constants.ScrapeType.BACKGROUND)
        assert not sessions.close_session(scrape_key)


class TestAddDocketItemToCurrentSession:
    """Tests for the add_docket_item_to_current_session method in the module."""

    def setup_method(self, _test_method):
        sessions.clear_ds()

    def teardown_method(self, _test_method):
        sessions.clear_ds()

    @patch("google.cloud.datastore.Query")
    @patch("google.cloud.datastore.Client")
    def test_add_item_happy_path(self, mock_client, mock_query):
        current_session_key = datastore.key.Key("session", "current", project=0)
        current_session_vars = {
            "region": "us_va",
            "scrape_type": constants.ScrapeType.SNAPSHOT,
            "phase": scrape_phase.ScrapePhase.START,
            "start": fix_dt(datetime(2014, 8, 31)),
        }
        current_session = ScrapeSession.new(current_session_key, **current_session_vars)
        prior_session = ScrapeSession.new(
            datastore.key.Key("session", "prior", project=0),
            region="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            start=fix_dt(datetime(2014, 8, 17)),
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )

        wire_sessions_to_query(
            mock_client, mock_query, [current_session, prior_session]
        )

        assert sessions.add_docket_item_to_current_session(
            "alpha", ScrapeKey("us_va", constants.ScrapeType.SNAPSHOT)
        )

        current_session_vars.update({"docket_ack_id": "alpha"})
        expected_session = ScrapeSession.new(
            current_session_key, **current_session_vars
        )
        mock_client.return_value.put.assert_called_with(expected_session.to_entity())

    @patch("google.cloud.datastore.Client")
    def test_add_item_no_open_sessions(self, _mock_client):
        assert not sessions.add_docket_item_to_current_session(
            "alpha", ScrapeKey("us_va", constants.ScrapeType.SNAPSHOT)
        )


def wire_sessions_to_query(mock_client, mock_query, session_list):
    client = mock_client.return_value
    query = mock_query.return_value
    client.query.return_value = query
    query.fetch.return_value = (session.to_entity() for session in session_list)


def fix_dt(dt):
    return dt.replace(tzinfo=pytz.UTC)


@pytest.mark.usefixtures("emulator")
class TestSessionManager:
    """Tests for various query methods in the module."""

    def setup_method(self, _test_method):
        self.keys_to_delete = []

    def teardown_method(self, _test_method):
        sessions.ds().delete_multi(self.keys_to_delete)

    def test_get_sessions_defaults(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            start=fix_dt(datetime(2009, 6, 17)),
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            start=fix_dt(datetime(2009, 6, 18)),
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        third = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            start=fix_dt(datetime(2009, 6, 19)),
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        results = sessions.get_sessions("us_ny")
        assert to_entities(results) == to_entities([third, second, first])

    def test_get_sessions_defaults_with_order(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
            end=fix_dt(datetime(2009, 6, 18)),
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
            end=fix_dt(datetime(2009, 6, 19)),
        )
        third = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions("us_ny")
        assert to_entities(results) == to_entities([third, second, first])

    def test_get_sessions_most_recent_only(self):
        # older
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        third = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions("us_ny", most_recent_only=True)
        assert to_entities(results) == to_entities([third])

    def test_get_sessions_open_most_recent_only(self):
        # older
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions(
            "us_ny", include_closed=False, most_recent_only=True
        )
        assert to_entities(results) == to_entities([second])

    def test_get_sessions_open_only(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions("us_ny", include_closed=False)
        assert to_entities(results) == to_entities([second, first])

    def test_get_sessions_background_only(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # snapshot
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        third = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions(
            "us_ny", scrape_type=constants.ScrapeType.BACKGROUND
        )
        assert to_entities(results) == to_entities([third, first])

    def test_get_sessions_background_and_open_only(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # snapshot
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region, scrape type
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions(
            "us_ny", include_closed=False, scrape_type=constants.ScrapeType.BACKGROUND
        )
        assert to_entities(results) == to_entities([first])

    def test_get_sessions_background_and_open_and_most_recent_only(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # different scrape type
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region, scrape type
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        # older
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 14)),
        )
        results = sessions.get_sessions(
            "us_ny",
            include_closed=False,
            most_recent_only=True,
            scrape_type=constants.ScrapeType.BACKGROUND,
        )
        assert to_entities(results) == to_entities([first])

    def test_get_sessions_none_for_region(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions("us_mo")
        assert not to_entities(results)

    def test_get_sessions_none_for_scrape_type(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions(
            "us_fl", scrape_type=constants.ScrapeType.BACKGROUND
        )
        assert not to_entities(results)

    def test_get_sessions_none_open(self):
        # different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed, different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # closed
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        results = sessions.get_sessions("us_fl", include_closed=False)
        assert not to_entities(results)

    def test_get_sessions_none_closed(self):
        # open, different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # open, different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # open
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        results = sessions.get_sessions("us_fl", include_open=False)
        assert not to_entities(results)

    def test_get_sessions_none_at_all(self):
        results = sessions.get_sessions("us_ny")
        assert not to_entities(results)

    def test_get_sessions_not_open_or_closed(self):
        # different region
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # open
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        results = sessions.get_sessions(
            "us_fl", include_open=False, include_closed=False
        )
        assert not to_entities(results)

    def test_get_most_recently_closed_session(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
            end=fix_dt(datetime(2009, 6, 18)),
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )

        result = sessions.get_most_recent_completed_session("us_ny")
        assert result.to_entity() == second.to_entity()

    def test_get_most_recently_closed_session_when_empty(self):
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
            end=fix_dt(datetime(2009, 6, 18)),
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )

        assert not sessions.get_most_recent_completed_session("us_ny")

    def test_get_current_session(self):
        # older
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        current = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        # closed
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different scrape type
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )
        # different region
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )

        result = sessions.get_current_session(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )

        assert result.to_entity() == current.to_entity()

    def test_get_recent_sessions(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 17)),
        )
        # different scrape type
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 18)),
        )
        third = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
            end=fix_dt(datetime(2009, 6, 21)),
        )
        # different region, scrape type
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2009, 6, 19)),
        )

        results = sessions.get_recent_sessions(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )
        assert to_entities(results) == to_entities([third, first])

    def test_get_sessions_with_leased_happy_path(self):
        first = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="a",
        )
        second = self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="b",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="c",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id=None,
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="d",
        )

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )
        assert to_entities(results) == to_entities([first, second])

    def test_get_sessions_with_leased_none_for_region(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="a",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="b",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="c",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id=None,
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="d",
        )

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_mo", constants.ScrapeType.BACKGROUND)
        )
        assert not to_entities(results)

    def test_get_sessions_with_leased_none_for_scrape_type(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="a",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="b",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="c",
        )
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id=None,
        )
        self.create_session(
            region_code="us_fl",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id="d",
        )

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_fl", constants.ScrapeType.SNAPSHOT)
        )
        assert not to_entities(results)

    def test_get_sessions_with_leased_none_with_docket_ack_id(self):
        self.create_session(
            region_code="us_ny",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.START,
            start=fix_dt(datetime(2016, 11, 20)),
            docket_ack_id=None,
        )

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_ny", constants.ScrapeType.BACKGROUND)
        )
        assert not to_entities(results)

    def create_session(
        self, region_code, scrape_type, phase, start, end=None, docket_ack_id=None
    ):
        session = ScrapeSession.new(
            key=sessions.ds().key("ScrapeSession"),
            region=region_code,
            scrape_type=scrape_type,
            phase=phase,
            docket_ack_id=docket_ack_id,
            start=start,
            end=end,
        )
        sessions.ds().put(session.to_entity())
        self.keys_to_delete.append(session.to_entity().key)
        return session


def to_entities(session_list):
    return [session.to_entity() for session in session_list]
