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
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class TestGetRootExternalIdClusterEdges(
    BigQueryEmulatorTestCase, IngestRegionTestMixin
):
    """Tests the GetRootExternalIds PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

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
        test_name: str,
    ) -> Iterable[Entity]:
        rows = list(
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name,
                test_name=test_name,
                fixture_has_metadata_columns=False,
                generate_default_metadata=False,
            )
        )
        return (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .parse_contents(
                contents_iterator=iter(rows),
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
                    ingest_view_name="ingest12", test_name="ingest12"
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
                    test_name="ingestMultipleRootExternalIds",
                )
            )
            | beam.ParDo(pipeline.GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
