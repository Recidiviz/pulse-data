# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Testing the GetRootExternalIdClusterEdges PTransform."""
from types import ModuleType
from typing import Iterable, Optional

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import read_ingest_view_results_fixture
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class TestGetRootExternalIdClusterEdges(
    BigQueryEmulatorTestCase, IngestRegionTestMixin
):
    """Tests the GetRootExternalIds PTransform."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return fake_regions

    def build_root_entities_from_fixture(
        self,
        *,
        ingest_view_name: str,
        file_name_w_suffix: str,
    ) -> Iterable[Entity]:
        df = read_ingest_view_results_fixture(
            self.state_code(),
            ingest_view_name,
            file_name_w_suffix,
            generate_metadata=False,
        )
        return (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .parse_contents(
                contents_iterator=df.to_dict("records"),
                context=IngestViewContentsContext.build_for_tests(),
            )
        )

    def test_get_root_external_ids(self) -> None:
        expected_output = [
            (("ID1", "US_DD_ID_TYPE"), None),
            (("ID2", "US_DD_ID_TYPE"), None),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                self.build_root_entities_from_fixture(
                    ingest_view_name="ingest12",
                    file_name_w_suffix="for_get_root_external_id_cluster_edges_test.csv",
                )
            )
            | beam.ParDo(pipeline.GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_get_root_external_ids_multiple_external_ids(self) -> None:
        expected_output = [
            (("ID1", "US_DD_ID_TYPE"), ("VALUE3", "US_DD_ID_ANOTHER_TYPE")),
            (
                ("VALUE3", "US_DD_ID_ANOTHER_TYPE"),
                ("ID1", "US_DD_ID_TYPE"),
            ),
            (("ID3", "US_DD_ID_TYPE"), ("VALUE4", "US_DD_ID_ANOTHER_TYPE")),
            (
                ("VALUE4", "US_DD_ID_ANOTHER_TYPE"),
                ("ID3", "US_DD_ID_TYPE"),
            ),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                self.build_root_entities_from_fixture(
                    ingest_view_name="ingestMultipleRootExternalIds",
                    file_name_w_suffix="for_get_root_external_id_cluster_edges_test.csv",
                )
            )
            | beam.ParDo(pipeline.GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
