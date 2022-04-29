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
"""Tests for proto_enum_mapper.py"""
import unittest

from recidiviz.common.constants.county.charge import (
    ChargeClass,
    ChargeDegree,
    ChargeStatus,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.models.ingest_info_pb2 import Charge
from recidiviz.persistence.ingest_info_converter.utils.ingest_info_proto_enum_mapper import (
    IngestInfoProtoEnumMapper,
)


class IngestInfoProtoEnumMapperTest(unittest.TestCase):
    """Tests for IngestInfoProtoEnumMapper"""

    def testEnumFromOriginalFieldIsPreferred(self) -> None:
        enum_fields = {
            "charge_class": ChargeClass,
            "status": ChargeStatus,
        }
        proto = Charge(charge_class="O", status="VIOLATION")

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("O", ChargeClass.PROBATION_VIOLATION)
        overrides_builder.add("VIOLATION", ChargeClass.INFRACTION, ChargeStatus)
        proto_enum_mapper = IngestInfoProtoEnumMapper(
            proto, enum_fields, overrides_builder.build()
        )

        self.assertEqual(
            ChargeClass.PROBATION_VIOLATION, proto_enum_mapper.get(ChargeClass)
        )

    def testMultipleMappingsFails(self) -> None:
        enum_fields = {
            "degree": ChargeDegree,
            "status": ChargeStatus,
        }
        proto = Charge(degree="O", status="VIOLATION")

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("O", ChargeClass.PROBATION_VIOLATION, ChargeDegree)
        overrides_builder.add("VIOLATION", ChargeClass.INFRACTION, ChargeStatus)
        proto_enum_mapper = IngestInfoProtoEnumMapper(
            proto, enum_fields, overrides_builder.build()
        )

        with self.assertRaises(ValueError):
            proto_enum_mapper.get(ChargeClass)
