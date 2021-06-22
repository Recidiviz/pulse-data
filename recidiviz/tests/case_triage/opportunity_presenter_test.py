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
from unittest.case import TestCase

import pytz

from recidiviz.case_triage.opportunities.types import OpportunityDeferralType
from recidiviz.case_triage.querier.opportunity_presenter import OpportunityPresenter
from recidiviz.persistence.database.schema.case_triage.schema import OpportunityDeferral
from recidiviz.tests.case_triage.case_triage_helpers import generate_fake_opportunity


class TestOpportunityPresenter(TestCase):
    """Implements tests for the OpportunityPresenter class."""

    def setUp(self) -> None:
        self.mock_opportunity = generate_fake_opportunity(
            officer_id="officer_id_1", person_external_id="person_id_1"
        )

    def test_no_deferral(self) -> None:
        presenter = OpportunityPresenter(self.mock_opportunity, None)
        now = datetime.now()
        self.assertEqual(
            presenter.to_json(now),
            {
                "personExternalId": self.mock_opportunity.person_external_id,
                "supervisingOfficerExternalId": self.mock_opportunity.supervising_officer_external_id,
                "stateCode": self.mock_opportunity.state_code,
                "opportunityType": self.mock_opportunity.opportunity_type,
                "opportunityMetadata": self.mock_opportunity.opportunity_metadata,
            },
        )

    def test_deferral(self) -> None:
        now = datetime.now(tz=pytz.UTC)
        tomorrow = now + timedelta(days=1)
        day_after_tomorrow = now + timedelta(days=2)

        deferral = OpportunityDeferral.from_etl_opportunity(
            self.mock_opportunity,
            OpportunityDeferralType.REMINDER.value,
            tomorrow,
            True,
        )
        presenter = OpportunityPresenter(self.mock_opportunity, deferral)

        self.assertTrue("deferredUntil" in presenter.to_json(now))
        self.assertFalse("deferredUntil" in presenter.to_json(day_after_tomorrow))
