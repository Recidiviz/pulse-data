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

"""Tests for ingest/scraper_control.py."""
from datetime import datetime
from unittest import TestCase

from flask import Flask
from mock import Mock, call, patch

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.ingest.scrape import constants, infer_release, scrape_phase
from recidiviz.ingest.scrape.sessions import ScrapeSession
from recidiviz.utils import regions

APP_ID = "recidiviz-infer-release-test"

app = Flask(__name__)
app.register_blueprint(infer_release.infer_release_blueprint)
app.config["TESTING"] = True

_REGIONS = [
    regions.Region(
        region_code="us_ut",
        agency_name="agency_name",
        agency_type="jail",
        base_url="base_url",
        queue={"rate": "5/m"},
        timezone="America/New_York",
        jurisdiction_id="jid",
        environment="production",
    ),
    regions.Region(
        region_code="us_wy",
        agency_name="agency_name",
        agency_type="jail",
        base_url="base_url",
        timezone="America/New_York",
        removed_from_website="UNKNOWN_SIGNIFICANCE",
        jurisdiction_id="jid",
        environment="production",
    ),
    regions.Region(
        region_code="us_nc",
        agency_name="agency_name",
        agency_type="prison",
        base_url="base_url",
        timezone="America/New_York",
        jurisdiction_id="jid",
        environment="production",
    ),
]


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
class TestInferRelease(TestCase):
    """Tests for requests to the infer release API"""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.persistence.persistence.infer_release_on_open_bookings")
    @patch("recidiviz.ingest.scrape.sessions.get_most_recent_completed_session")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.infer_release.validate_regions")
    @patch("recidiviz.ingest.scrape.infer_release.get_region")
    def test_infer_release(
        self,
        mock_get_region,
        mock_validate_regions,
        mock_update_phase,
        mock_get_most_recent_session,
        mock_infer_release,
    ):
        headers = {"X-Appengine-Cron": "test-cron"}
        mock_validate_regions.return_value = [r.region_code for r in _REGIONS]
        mock_get_region.side_effect = _REGIONS

        time = datetime(2014, 8, 31)
        recent_session = ScrapeSession.new(
            key=None,
            start=time,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.RELEASE,
        )
        mock_get_most_recent_session.return_value = recent_session

        response = self.client.get(
            "/release?region=us_ut&region=us_wy", headers=headers
        )
        assert response.status_code == 200
        mock_infer_release.assert_has_calls(
            [
                call("us_ut", time, CustodyStatus.INFERRED_RELEASE),
                call("us_wy", time, CustodyStatus.REMOVED_WITHOUT_INFO),
            ]
        )

        mock_update_phase.assert_called_with(
            recent_session, scrape_phase.ScrapePhase.DONE
        )
