# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for StateSpecificEntityMatchingDelegateFactory"""
import datetime
import unittest

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.instance_database_key import database_key_for_state
from recidiviz.persistence.entity_matching.state.state_specific_entity_matching_delegate_factory import (
    StateSpecificEntityMatchingDelegateFactory,
)


class StateSpecificEntityMatchingDelegateFactoryTest(unittest.TestCase):
    """Tests for StateSpecificEntityMatchingDelegateFactory"""

    def test_correct_state_codes(self) -> None:
        """Checks that each state is using the correct delegate."""
        for state_code in get_existing_direct_ingest_states():
            state_code_lower = state_code.value.lower()
            delegate = StateSpecificEntityMatchingDelegateFactory.build(
                region_code=state_code_lower,
                ingest_metadata=IngestMetadata(
                    region=state_code_lower,
                    ingest_time=datetime.datetime.now(),
                    database_key=database_key_for_state(
                        direct_ingest_instance=DirectIngestInstance.PRIMARY,
                        state_code=state_code,
                    ),
                ),
            )

            self.assertEqual(delegate.get_region_code(), state_code.value)

            capital_case_state_code = "".join(
                s.capitalize() for s in state_code.value.split("_")
            )
            self.assertEqual(
                delegate.__class__.__name__,
                f"{capital_case_state_code}MatchingDelegate",
            )
