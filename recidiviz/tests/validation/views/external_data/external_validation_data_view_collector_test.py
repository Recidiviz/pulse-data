# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for ExternalValidationDataBigQueryViewCollector"""
from unittest import TestCase

from recidiviz.validation.views.external_data.external_validation_data_view_collector import (
    ExternalValidationDataBigQueryViewCollector,
)


class ExternalValidationDataBigQueryViewCollectorTest(TestCase):
    """Tests for ExternalValidationDataBigQueryViewCollector"""

    def test_collects(self) -> None:
        _ = (
            ExternalValidationDataBigQueryViewCollector().collect_state_specific_and_build_state_agnostic_external_validation_view_builders()
        )
