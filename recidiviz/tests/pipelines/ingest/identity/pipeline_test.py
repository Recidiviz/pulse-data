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
"""Tests the identity ingest pipeline end-to-end."""
import json
from types import ModuleType

from recidiviz.common.constants.tenants import Tenant
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.ingest.identity.identity_ingest_pipeline_test_case import (
    IdentityIngestPipelineTestCase,
)

INTEGRATION_TEST_NAME = "identity_pipeline_integration_test"


class TestIdentityIngestPipeline(IdentityIngestPipelineTestCase):
    """Tests the identity ingest pipeline end-to-end."""

    @classmethod
    def tenant(cls) -> Tenant:
        return Tenant.US_DD

    @classmethod
    def region_module_override(cls) -> ModuleType | None:
        return fake_regions

    def setUp(self) -> None:
        super().setUp()
        # Explicit upper-bound dates keep `__upper_bound_datetime_inclusive` in
        # the ingest-view-results tables deterministic across runs (the loader's
        # default is `datetime.now()`).
        self.us_dd_upper_bound_dates_json = json.dumps(
            {
                "identity_person": "2026-06-15T00:00:00.000000",
                "identity_staff": "2026-06-15T00:00:00.000000",
            }
        )

    def test_identity_ingest_pipeline(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name=INTEGRATION_TEST_NAME)
        self.run_test_identity_ingest_pipeline(
            test_name=INTEGRATION_TEST_NAME,
            raw_data_upper_bound_dates_json_override=self.us_dd_upper_bound_dates_json,
        )
