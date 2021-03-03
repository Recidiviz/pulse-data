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
"""Tests for enum_mappings.py"""
import unittest

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.ingest.models.ingest_info_pb2 import Charge, StateIncarcerationPeriod
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


class EnumMappingsTest(unittest.TestCase):
    """Tests for EnumMappings"""

    def testEnumFromOriginalFieldIsPreferred(self):
        enum_fields = {
            "charge_class": ChargeClass,
            "status": ChargeStatus,
        }
        proto = Charge(charge_class="O", status="VIOLATION")

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("O", ChargeClass.PROBATION_VIOLATION)
        overrides_builder.add("VIOLATION", ChargeClass.INFRACTION, ChargeStatus)
        enum_mappings = EnumMappings(proto, enum_fields, overrides_builder.build())

        self.assertEqual(
            ChargeClass.PROBATION_VIOLATION, enum_mappings.get(ChargeClass)
        )

    def testEnumMultipleFieldShareEnumType(self):
        enum_fields = {
            "admission_reason": StateIncarcerationPeriodAdmissionReason,
            "projected_release_reason": StateIncarcerationPeriodReleaseReason,
            "release_reason": StateIncarcerationPeriodReleaseReason,
        }

        proto = StateIncarcerationPeriod(
            admission_reason="PAROLE_REVOCATION",
            projected_release_reason="CONDITIONAL_RELEASE",
            release_reason="SERVED",
        )

        enum_mappings = EnumMappings(
            proto, enum_fields, EnumOverrides.Builder().build()
        )

        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            enum_mappings.get(
                StateIncarcerationPeriodReleaseReason,
                field_name="projected_release_reason",
            ),
        )

        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            enum_mappings.get(
                StateIncarcerationPeriodReleaseReason, field_name="release_reason"
            ),
        )

    def testMultipleMappingsFails(self):
        enum_fields = {
            "degree": ChargeDegree,
            "status": ChargeStatus,
        }
        proto = Charge(degree="O", status="VIOLATION")

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("O", ChargeClass.PROBATION_VIOLATION, ChargeDegree)
        overrides_builder.add("VIOLATION", ChargeClass.INFRACTION, ChargeStatus)
        enum_mappings = EnumMappings(proto, enum_fields, overrides_builder.build())

        with self.assertRaises(ValueError):
            enum_mappings.get(ChargeClass)
