# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the scheduled eOMIS writeback flow registry."""
import unittest
from unittest.mock import patch

from recidiviz.eomis.scheduled_flows import SCHEDULED_FLOWS
from recidiviz.eomis.us_ar.program_referral_flow import ArProgramReferralFlow
from recidiviz.eomis.us_co import sentence_credit_flow as co
from recidiviz.eomis.us_co.sentence_credit_flow import CoSentenceCreditFlow
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestScheduledFlows(unittest.TestCase):
    """Tests the registry's per-flow config and factories."""

    def test_base_url_for_project_picks_by_project(self) -> None:
        for scheduled in SCHEDULED_FLOWS.values():
            self.assertEqual(
                scheduled.base_url_for_project(GCP_PROJECT_STAGING),
                scheduled.test_base_url,
            )
            self.assertEqual(
                scheduled.base_url_for_project(GCP_PROJECT_PRODUCTION),
                scheduled.prod_base_url,
            )

    def test_build_returns_the_expected_flow_types(self) -> None:
        self.assertIsInstance(
            SCHEDULED_FLOWS["ar_ged"].build(bq_view=None, project_id="p"),
            ArProgramReferralFlow,
        )
        self.assertIsInstance(
            SCHEDULED_FLOWS["co_edovo"].build(bq_view=None, project_id="p"),
            CoSentenceCreditFlow,
        )

    def test_co_build_defaults_to_the_flow_view(self) -> None:
        with patch.object(co, "CoSentenceCreditFlow") as mock_flow_cls:
            SCHEDULED_FLOWS["co_edovo"].build(bq_view=None, project_id="proj")
        mock_flow_cls.assert_called_once_with(
            bq_view=co.default_view("proj"), project_id="proj", limit=None
        )

    def test_co_build_passes_bq_view_override(self) -> None:
        with patch.object(co, "CoSentenceCreditFlow") as mock_flow_cls:
            SCHEDULED_FLOWS["co_edovo"].build(bq_view="a.b.c", project_id="proj")
        mock_flow_cls.assert_called_once_with(
            bq_view="a.b.c", project_id="proj", limit=None
        )


if __name__ == "__main__":
    unittest.main()
