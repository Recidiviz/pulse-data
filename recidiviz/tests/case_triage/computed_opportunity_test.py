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
"""Implements tests for opportunities derived from client model properties"""
from datetime import date, timedelta
from unittest import TestCase

from recidiviz.case_triage.opportunities.models import ComputedOpportunity
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_client_info,
)


class TestComputedOpportunity(TestCase):
    """Tests that opportunities are computed correctly for clients."""

    def test_employment(self) -> None:
        client = generate_fake_client("abc")
        client.client_info = generate_fake_client_info(client)
        print(ComputedOpportunity.build_all_for_client(client))
        opp = ComputedOpportunity.build_all_for_client(client).get(
            OpportunityType.EMPLOYMENT
        )

        assert opp is not None
        self.assertIsInstance(opp, ComputedOpportunity)

        # no unemployment alert if client is on SSI/Disability
        client.client_info.receiving_ssi_or_disability_income = True
        self.assertIsNone(
            ComputedOpportunity.build_all_for_client(client).get(
                OpportunityType.EMPLOYMENT
            )
        )

        client.client_info.receiving_ssi_or_disability_income = False
        client.employer = "Test Company Ltd."
        self.assertIsNone(
            ComputedOpportunity.build_all_for_client(client).get(
                OpportunityType.EMPLOYMENT
            )
        )

    def test_assessment(self) -> None:
        today = date.today()
        overdue_date = today - timedelta(days=500)
        upcoming_date = today - timedelta(days=335)
        no_opp_date = today - timedelta(days=334)

        client = generate_fake_client("abc", last_assessment_date=overdue_date)
        opp = ComputedOpportunity.build_all_for_client(client).get(
            OpportunityType.ASSESSMENT
        )

        assert opp is not None
        self.assertIsInstance(opp, ComputedOpportunity)
        self.assertEqual(opp.opportunity_metadata, {"status": "OVERDUE"})

        client.most_recent_assessment_date = upcoming_date
        opp = ComputedOpportunity.build_all_for_client(client).get(
            OpportunityType.ASSESSMENT
        )

        assert opp is not None
        self.assertIsInstance(opp, ComputedOpportunity)
        self.assertEqual(opp.opportunity_metadata, {"status": "UPCOMING"})

        client.most_recent_assessment_date = no_opp_date
        self.assertIsNone(
            ComputedOpportunity.build_all_for_client(client).get(
                OpportunityType.ASSESSMENT
            )
        )

    def test_contact(self) -> None:
        today = date.today()
        overdue_date = today - timedelta(days=50)
        upcoming_date = today - timedelta(days=40)
        no_opp_date = today - timedelta(days=10)

        client = generate_fake_client("abc", last_face_to_face_date=overdue_date)
        opp = ComputedOpportunity.build_all_for_client(client).get(
            OpportunityType.CONTACT
        )

        assert opp is not None
        self.assertIsInstance(opp, ComputedOpportunity)
        self.assertEqual(opp.opportunity_metadata, {"status": "OVERDUE"})

        client.most_recent_face_to_face_date = upcoming_date
        opp = ComputedOpportunity.build_all_for_client(client).get(
            OpportunityType.CONTACT
        )

        assert opp is not None
        self.assertIsInstance(opp, ComputedOpportunity)
        self.assertEqual(opp.opportunity_metadata, {"status": "UPCOMING"})

        client.most_recent_face_to_face_date = no_opp_date
        self.assertIsNone(
            ComputedOpportunity.build_all_for_client(client).get(
                OpportunityType.CONTACT
            )
        )
