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

"""Tests for ingest/sessions.py."""


from datetime import datetime
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from recidiviz.ingest import constants
from recidiviz.ingest import sessions
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.sessions import ScrapeSession


class TestWriteSessions(object):
    """Tests for the create_session, end_session, and update_session methods
    in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_create_session(self):
        scrape_key = ScrapeKey("us_ok", constants.SNAPSHOT_SCRAPE)
        sessions.create_session(scrape_key)

        session = sessions.get_current_session(scrape_key)
        assert session.region == "us_ok"
        assert session.scrape_type == constants.SNAPSHOT_SCRAPE
        assert session.docket_item is None
        assert session.end is None
        assert session.last_scraped is None

    def test_create_session_something_to_end(self):
        scrape_key = ScrapeKey("us_wy", constants.BACKGROUND_SCRAPE)
        sessions.create_session(scrape_key)
        sessions.create_session(scrape_key)
        sessions.create_session(scrape_key)

        results = sessions.get_open_sessions(
            "us_wy", open_only=False, scrape_type=constants.BACKGROUND_SCRAPE)

        assert len(results) == 3

        current_session = results[0]
        assert current_session.region == "us_wy"
        assert current_session.scrape_type == constants.BACKGROUND_SCRAPE
        assert current_session.docket_item is None
        assert current_session.end is None
        assert current_session.last_scraped is None

        for i in range(1, 3):
            session = results[i]
            assert session.region == "us_wy"
            assert session.scrape_type == constants.BACKGROUND_SCRAPE
            assert session.end

    def test_update_session(self):
        scrape_key = ScrapeKey("us_sd", constants.SNAPSHOT_SCRAPE)
        sessions.create_session(scrape_key)

        assert sessions.update_session("CAMUS, ALBERT", scrape_key)
        session = sessions.get_current_session(scrape_key)
        assert session.region == "us_sd"
        assert session.scrape_type == constants.SNAPSHOT_SCRAPE
        assert session.last_scraped == "CAMUS, ALBERT"

    def test_update_session_nothing_current(self):
        scrape_key = ScrapeKey("us_sd", constants.BACKGROUND_SCRAPE)
        assert not sessions.update_session("VONNEGUT, KURT", scrape_key)


class TestAddDocketItemToCurrentSession(object):
    """Tests for the add_docket_item_to_current_session method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_add_item_happy_path(self):
        current = create_open_session("us_va", constants.SNAPSHOT_SCRAPE,
                                      datetime(2014, 8, 31), None)
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2014, 8, 17), None)

        success = sessions.add_docket_item_to_current_session(
            "alpha", ScrapeKey("us_va", constants.SNAPSHOT_SCRAPE))

        assert success

        session = sessions.get_open_sessions(current.region,
                                             most_recent_only=True,
                                             scrape_type=current.scrape_type)

        assert session.region == current.region
        assert session.scrape_type == current.scrape_type
        assert session.start == current.start
        assert session.end == current.end
        assert session.docket_item == "alpha"

    def test_add_item_no_open_sessions(self):
        create_closed_session(
            "us_va", constants.SNAPSHOT_SCRAPE, datetime(2014, 8, 31),
            datetime(2014, 9, 4), None)

        success = sessions.add_docket_item_to_current_session(
            "alpha", ScrapeKey("us_va", constants.SNAPSHOT_SCRAPE))

        assert not success


class TestGetOpenSessions(object):
    """Tests for the get_open_sessions method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_get_open_sessions_defaults(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        second = create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                                     datetime(2009, 6, 18), "b")
        create_closed_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 19),
            datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_open_sessions("us_ny")
        assert results == [second, first]

    def test_get_open_sessions_most_recent_only(self):
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 17), "a")
        second = create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                                     datetime(2009, 6, 18), "b")
        create_closed_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 19),
            datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        result = sessions.get_open_sessions("us_ny", most_recent_only=True)
        assert result == second

    def test_get_most_recently_closed_session(self):
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 17), datetime(2009, 6,
                                                              18),
                              "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        third = create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                                      datetime(2009, 6, 19),
                                      datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_most_recent_completed_session("us_ny")
        assert results == third

    def test_get_open_sessions_open_or_closed(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        second = create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                                     datetime(2009, 6, 18), "b")
        third = create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                                      datetime(2009, 6, 19),
                                      datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_open_sessions("us_ny", open_only=False)
        assert results == [third, second, first]

    def test_get_open_sessions_background_only(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        third = create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                                      datetime(2009, 6, 19),
                                      datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_open_sessions(
            "us_ny", open_only=False, scrape_type=constants.BACKGROUND_SCRAPE)
        assert results == [third, first]

    def test_get_open_sessions_background_and_open_only(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        result = sessions.get_open_sessions(
            "us_ny", scrape_type=constants.BACKGROUND_SCRAPE)
        assert result == [first]

    def test_get_open_sessions_background_and_open_and_most_recent_only(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 14), "e")

        results = sessions.get_open_sessions(
            "us_ny", most_recent_only=True,
            scrape_type=constants.BACKGROUND_SCRAPE)
        assert results == first

    def test_get_open_sessions_none_for_region(self):
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_open_sessions("us_mo")
        assert not results

    def test_get_open_sessions_none_for_scrape_type(self):
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_open_sessions(
            "us_fl", scrape_type=constants.BACKGROUND_SCRAPE)
        assert not results

    def test_get_open_sessions_none_open(self):
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_closed_session(
            "us_fl", constants.SNAPSHOT_SCRAPE, datetime(2009, 6, 19),
            datetime(2009, 6, 21), "d")

        results = sessions.get_open_sessions("us_fl")
        assert not results

    def test_get_open_sessions_none_at_all(self):
        results = sessions.get_open_sessions("us_ny")
        assert not results

    def test_get_current_session(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                              datetime(2009, 6, 19),
                              datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")
        create_open_session(
            "us_ny", constants.BACKGROUND_SCRAPE, datetime(2009, 6, 14), "e")

        result = sessions.get_current_session(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert result == first

    def test_get_recent_sessions(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2009, 6, 17), "a")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 18), "b")
        third = create_closed_session("us_ny", constants.BACKGROUND_SCRAPE,
                                      datetime(2009, 6, 19),
                                      datetime(2009, 6, 21), "c")
        create_open_session("us_fl", constants.SNAPSHOT_SCRAPE,
                            datetime(2009, 6, 19), "d")

        results = sessions.get_recent_sessions(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert results == [third, first]


class TestGetSessionsWithWithLeasedDocketItems(object):
    """Tests for the get_sessions_with_leased_docket_items
    method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_get_sessions_happy_path(self):
        first = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                    datetime(2016, 11, 20), "a")
        second = create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                                     datetime(2016, 11, 20), "b")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2016, 11, 20), "c")
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), None)
        create_open_session("us_fl", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "d")

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert results == [first, second]

    def test_get_sessions_none_for_region(self):
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "a")
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "b")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2016, 11, 20), "c")
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), None)
        create_open_session("us_fl", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "d")

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_mo", constants.BACKGROUND_SCRAPE))
        assert not results

    def test_get_sessions_none_for_scrape_type(self):
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "a")
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "b")
        create_open_session("us_ny", constants.SNAPSHOT_SCRAPE,
                            datetime(2016, 11, 20), "c")
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), None)
        create_open_session("us_fl", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), "d")

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_fl", constants.SNAPSHOT_SCRAPE))
        assert not results

    def test_get_sessions_none_with_docket_item(self):
        create_open_session("us_ny", constants.BACKGROUND_SCRAPE,
                            datetime(2016, 11, 20), None)

        results = sessions.get_sessions_with_leased_docket_items(
            ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE))
        assert not results


def create_open_session(region_code, scrape_type, start, docket_item):
    session = ScrapeSession(region=region_code,
                            scrape_type=scrape_type,
                            docket_item=docket_item,
                            start=start)
    session.put()
    return session


def create_closed_session(region_code, scrape_type, start, end, docket_item):
    session = ScrapeSession(region=region_code,
                            scrape_type=scrape_type,
                            docket_item=docket_item,
                            start=start,
                            end=end)
    session.put()
    return session
