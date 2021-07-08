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
"""Implements tests for the OpportunityPresenter class."""
from datetime import datetime, timedelta
from typing import List
from unittest.case import TestCase

import pytz

from recidiviz.case_triage.opportunities.types import Opportunity
from recidiviz.case_triage.querier.opportunity_presenter import OpportunityPresenter
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_computed_opportunity,
    generate_fake_etl_opportunity,
    generate_fake_reminder,
)


class TestOpportunityPresenter(TestCase):
    """Implements tests for the OpportunityPresenter class."""

    def setUp(self) -> None:
        self.mock_etl_opportunity = generate_fake_etl_opportunity(
            officer_id="officer_id_1", person_external_id="person_id_1"
        )

        self.mock_opportunity = generate_fake_computed_opportunity(
            officer_id="officer_id_1", person_external_id="person_id_1"
        )

        self.mock_opportunities: List[Opportunity] = [
            self.mock_etl_opportunity,
            self.mock_opportunity,
        ]

    def test_no_deferral(self) -> None:
        for opportunity in self.mock_opportunities:
            presenter = OpportunityPresenter(opportunity, None)
            now = datetime.now()
            self.assertEqual(
                presenter.to_json(now),
                {
                    "personExternalId": opportunity.person_external_id,
                    "supervisingOfficerExternalId": opportunity.supervising_officer_external_id,
                    "stateCode": opportunity.state_code,
                    "opportunityType": opportunity.opportunity_type,
                    "opportunityMetadata": opportunity.opportunity_metadata,
                },
            )

    def test_deferral(self) -> None:
        now = datetime.now(tz=pytz.UTC)
        tomorrow = now + timedelta(days=1)
        day_after_tomorrow = now + timedelta(days=2)

        deferral = generate_fake_reminder(self.mock_etl_opportunity, tomorrow)
        presenter = OpportunityPresenter(self.mock_etl_opportunity, deferral)

        self.assertTrue("deferredUntil" in presenter.to_json(now))
        self.assertFalse("deferredUntil" in presenter.to_json(day_after_tomorrow))
