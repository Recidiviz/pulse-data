# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for state_violation_matching_utils.py"""

from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching.state.state_violation_matching_utils import revoked_to_prison
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import \
    BaseStateMatchingUtilsTest


# pylint: disable=protected-access
class TestStateMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state violation matching utils"""

    def test_completeEnumSet_revokedToPrison(self):
        svr = schema.StateSupervisionViolationResponse()
        for revocation_type in StateSupervisionViolationResponseRevocationType:
            svr.revocation_type = revocation_type.value
            revoked_to_prison(svr)
