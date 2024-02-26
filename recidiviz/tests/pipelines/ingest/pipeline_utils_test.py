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
"""Tests for pipeline_utils.py"""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_utils import ingest_pipeline_name


class TestPipelineUtils(unittest.TestCase):
    """Tests for pipeline_utils.py"""

    def test_ingest_pipeline_name(self) -> None:
        self.assertEqual(
            "us-xx-ingest-primary",
            ingest_pipeline_name(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            ),
        )

        self.assertEqual(
            "us-yy-ingest-primary",
            ingest_pipeline_name(
                state_code=StateCode.US_YY,
                instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            ),
        )

        self.assertEqual(
            "us-xx-ingest-secondary",
            ingest_pipeline_name(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.SECONDARY,
                sandbox_prefix=None,
            ),
        )

    def test_ingest_pipeline_name_sandbox(self) -> None:
        self.assertEqual(
            "myprefix-us-xx-ingest-secondary-test",
            ingest_pipeline_name(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.SECONDARY,
                sandbox_prefix="myprefix",
            ),
        )

        self.assertEqual(
            "my-prefix-us-xx-ingest-secondary-test",
            ingest_pipeline_name(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.SECONDARY,
                sandbox_prefix="my_prefix",
            ),
        )

        self.assertEqual(
            "my-prefix-us-xx-ingest-secondary-test",
            ingest_pipeline_name(
                state_code=StateCode.US_XX,
                instance=DirectIngestInstance.SECONDARY,
                sandbox_prefix="My_preFIx",
            ),
        )
