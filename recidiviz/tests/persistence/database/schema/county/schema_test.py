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
"""Tests for county-specific SQLAlchemy enums."""

from recidiviz.common.constants.county import (
    bond,
    booking,
    charge,
    hold,
    person_characteristics,
    sentence,
)
from recidiviz.persistence.database.schema.county import schema
from recidiviz.tests.persistence.database.schema.schema_test import TestSchemaEnums


class TestCountySchemaEnums(TestSchemaEnums):
    """Tests for validating state schema enums are defined correctly"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        county_enums_mapping = {
            "admission_reason": booking.AdmissionReason,
            "classification": booking.Classification,
            "custody_status": booking.CustodyStatus,
            "release_reason": booking.ReleaseReason,
            "hold_status": hold.HoldStatus,
            "sentence_status": sentence.SentenceStatus,
            "sentence_relationship_type": None,
            "charge_class": charge.ChargeClass,
            "gender": person_characteristics.Gender,
            "race": person_characteristics.Race,
            "ethnicity": person_characteristics.Ethnicity,
            "residency_status": person_characteristics.ResidencyStatus,
            "bond_type": bond.BondType,
            "bond_status": bond.BondStatus,
            "degree": charge.ChargeDegree,
            "charge_status": charge.ChargeStatus,
        }

        self.check_persistence_and_schema_enums_match(county_enums_mapping, schema)
