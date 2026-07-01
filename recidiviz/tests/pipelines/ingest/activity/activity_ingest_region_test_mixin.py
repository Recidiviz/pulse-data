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
"""Helper mixin for tests that run the activity ingest pipeline against a
specific region."""
import os
from datetime import datetime
from typing import Any, Dict, Iterable

from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.tests.ingest.constants import DEFAULT_UPDATE_DATETIME
from recidiviz.tests.ingest.direct.fixture_util import (
    DIRECT_INGEST_FIXTURES_ROOT,
    LEGACY_INTEGRATION_INPUT_SUBDIR,
    load_dataframe_from_path,
)
from recidiviz.tests.pipelines.ingest.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class ActivityIngestRegionTestMixin(IngestRegionTestMixin):
    """Helper mixin for tests that run the activity ingest pipeline against a
    specific region.
    """

    @classmethod
    def ingest_pipeline_type(cls) -> IngestPipelineType:
        return IngestPipelineType.ACTIVITY

    @classmethod
    def manifest_compiler_delegate(cls) -> IngestViewManifestCompilerDelegate:
        return ActivityIngestViewManifestCompilerDelegate(cls.region())

    # TODO(#36159): Delete this when all tests are based on real data and ingest view results
    # TODO(#22059): Remove this method and replace with the implementation on
    # ActivityIngestPipelineTestCase when fixture formats and data loading is standardized.
    def read_legacy_extract_and_merge_fixture(
        self,
        *,
        ingest_view_name: str,
        test_name: str,
    ) -> Iterable[Dict[str, Any]]:
        """
        Reads in an "extract and merge fixture" to be used in
        legacy integration tests.
        """
        fixture_columns = (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .input_columns
        )
        fixture_path = os.path.join(
            DIRECT_INGEST_FIXTURES_ROOT,
            self.state_code().value.lower(),
            LEGACY_INTEGRATION_INPUT_SUBDIR,
            f"{test_name}.csv",
        )

        try:
            df = load_dataframe_from_path(
                fixture_path,
                fixture_columns,
            )
            df[MATERIALIZATION_TIME_COL_NAME] = datetime.now().isoformat()
            df[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat()
            return df.to_dict("records")
        except Exception as ex:
            raise ValueError(f"Failed to read fixture file at {fixture_path}") from ex
