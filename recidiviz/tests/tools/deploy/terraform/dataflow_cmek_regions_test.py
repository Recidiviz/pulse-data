# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests that the Dataflow CMEK key regions in dataflow-cmek.tf stay in sync
with the regions the pipelines can launch in."""
import os
import re
import unittest

import recidiviz.tools.deploy as _deploy_pkg
from recidiviz.pipelines.ingest.activity.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)

_TF_PATH = os.path.join(
    os.path.dirname(_deploy_pkg.__file__), "terraform", "dataflow-cmek.tf"
)

# Captures the quoted region list inside
# dataflow_cmek_regions = toset(<condition> ? [ "us-central1", ... ] : [])
_REGIONS_BLOCK = re.compile(
    r"dataflow_cmek_regions\s*=\s*toset\([^\[]*\[(?P<regions>[^\]]*)\]", re.S
)


class DataflowCmekRegionsTest(unittest.TestCase):
    """A pipeline launched in a region with no key fails at launch, so the
    Terraform region list must cover every region a pipeline can use."""

    def test_terraform_regions_match_pipeline_regions(self) -> None:
        with open(_TF_PATH, encoding="utf-8") as f:
            match = _REGIONS_BLOCK.search(f.read())
        self.assertIsNotNone(
            match, "dataflow_cmek_regions list not found in dataflow-cmek.tf"
        )
        assert match is not None
        tf_regions = sorted(re.findall(r'"([a-z0-9-]+)"', match.group("regions")))
        pipeline_regions = sorted(set(DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE.values()))
        self.assertEqual(
            pipeline_regions,
            tf_regions,
            "Add the missing region's key ring and key to dataflow-cmek.tf, or "
            "remove the region from DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE.",
        )
