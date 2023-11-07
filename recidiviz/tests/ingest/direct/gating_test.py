#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for dataflow ingest gating."""
import unittest
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import (
    ingest_pipeline_can_run_in_dag,
    is_ingest_in_dataflow_enabled,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_ENVIRONMENTS


class TestGating(unittest.TestCase):
    """Tests for gating related functions"""

    def test_gating_method_equality(self) -> None:
        for environment in GCP_ENVIRONMENTS:
            with patch(
                "recidiviz.utils.environment.get_gcp_environment",
                Mock(return_value=environment),
            ):
                for state in StateCode:
                    for instance in DirectIngestInstance:
                        if is_ingest_in_dataflow_enabled(state, instance):
                            self.assertTrue(
                                ingest_pipeline_can_run_in_dag(state, instance)
                            )
