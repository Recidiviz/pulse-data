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
"""Tests for converting charges."""

import unittest

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import charge

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class ChargeConverterTest(unittest.TestCase):
    """Tests for converting charges."""

    def setUp(self):
        self.subject = entities.Charge.builder()

    def testParseCharge(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge(
            charge_id="CHARGE_ID",
            attempted="True",
            degree="FIRST",
            charge_class="FELONY",
            status="DROPPED",
            court_type="DISTRICT",
        )

        # Act
        charge.copy_fields_to_builder(self.subject, ingest_charge, _EMPTY_METADATA)
        result = self.subject.build()

        # Assert
        expected_result = entities.Charge.new_with_defaults(
            external_id="CHARGE_ID",
            attempted=True,
            degree=ChargeDegree.FIRST,
            degree_raw_text="FIRST",
            charge_class=ChargeClass.FELONY,
            class_raw_text="FELONY",
            status=ChargeStatus.DROPPED,
            status_raw_text="DROPPED",
            court_type="DISTRICT",
        )

        self.assertEqual(result, expected_result)

    def testParseCharge_classInName(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge(name="Felony Murder")

        # Act
        charge.copy_fields_to_builder(self.subject, ingest_charge, _EMPTY_METADATA)
        result = self.subject.build()

        # Assert
        expected_result = entities.Charge.new_with_defaults(
            name="FELONY MURDER",
            charge_class=ChargeClass.FELONY,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(result, expected_result)

    def testParseCharge_classWithSpaceInName(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge(
            name="Failed Drug Test - Probation Violation"
        )

        # Act
        charge.copy_fields_to_builder(self.subject, ingest_charge, _EMPTY_METADATA)
        result = self.subject.build()

        # Assert
        expected_result = entities.Charge.new_with_defaults(
            name="FAILED DRUG TEST - PROBATION VIOLATION",
            charge_class=ChargeClass.PROBATION_VIOLATION,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        self.assertEqual(result, expected_result)

    def testParseCharge_SetsDefaults(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge()

        # Act
        charge.copy_fields_to_builder(self.subject, ingest_charge, _EMPTY_METADATA)
        result = self.subject.build()

        # Assert
        expected_result = entities.Charge.new_with_defaults(
            status=ChargeStatus.PRESENT_WITHOUT_INFO
        )
        self.assertEqual(result, expected_result)

    def testParseCharge_MapAcrossFields(self):
        # Arrange
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("FELONY", ChargeClass.FELONY, ChargeDegree)
        overrides_builder.add("FIRST DEGREE", ChargeDegree.FIRST, ChargeClass)
        metadata = IngestMetadata.new_with_defaults(
            enum_overrides=overrides_builder.build()
        )
        ingest_charge = ingest_info_pb2.Charge(
            charge_class="first degree", degree="felony"
        )

        # Act
        charge.copy_fields_to_builder(self.subject, ingest_charge, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Charge.new_with_defaults(
            degree=ChargeDegree.FIRST,
            degree_raw_text="FELONY",
            charge_class=ChargeClass.FELONY,
            class_raw_text="FIRST DEGREE",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        self.assertEqual(result, expected_result)
