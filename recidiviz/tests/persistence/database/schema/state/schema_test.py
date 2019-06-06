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
"""Tests for state-specific SQLAlchemy enums."""
from recidiviz.common.constants.state import state_assessment
from recidiviz.persistence.database.schema.state import schema
from recidiviz.tests.persistence.database.schema.schema_test import (
    TestSchemaEnums
)


class TestStateSchemaEnums(TestSchemaEnums):
    """Tests for validating state schema enums are defined correctly"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        state_enums_mapping = {
            'state_assessment_class': state_assessment.StateAssessmentClass,
            'state_assessment_type': state_assessment.StateAssessmentType,
            # TODO(1625): Add more state schema enum mappings here
        }

        merged_mapping = {**self.SHARED_ENUMS_TEST_MAPPING,
                          **state_enums_mapping}

        self.check_persistence_and_schema_enums_match(merged_mapping, schema)
