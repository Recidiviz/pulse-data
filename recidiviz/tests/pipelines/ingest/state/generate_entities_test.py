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
"""Testing the GenerateEntities PTransform."""
from types import ModuleType
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state.generate_entities import GenerateEntities
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.constants import DEFAULT_UPDATE_DATETIME
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fake_regions.us_dd.ingest_views import view_ingest12
from recidiviz.tests.ingest.direct.fixture_util import read_ingest_view_results_fixture
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class TestGenerateEntities(BigQueryEmulatorTestCase, IngestRegionTestMixin):
    """Tests the GenerateEntities PTransform."""

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

    def test_generate_entities(self) -> None:
        view_builder = view_ingest12.VIEW_BUILDER
        ingest_view_name = view_builder.ingest_view_name
        expected_output = [
            (
                DEFAULT_UPDATE_DATETIME.timestamp(),
                StatePerson(
                    state_code="US_DD",
                    external_ids=[
                        StatePersonExternalId(
                            state_code="US_DD",
                            external_id="ID1",
                            id_type="US_DD_ID_TYPE",
                        )
                    ],
                    full_name='{"given_names": "VALUE1", "middle_names": "", "name_suffix": "", "surname": "VALUE1"}',
                ),
            ),
            (
                DEFAULT_UPDATE_DATETIME.timestamp(),
                StatePerson(
                    state_code="US_DD",
                    external_ids=[
                        StatePersonExternalId(
                            state_code="US_DD",
                            external_id="ID2",
                            id_type="US_DD_ID_TYPE",
                        )
                    ],
                    full_name='{"given_names": "VALUE2", "middle_names": "", "name_suffix": "", "surname": "VALUE2"}',
                ),
            ),
        ]
        manifest_compiler = IngestViewManifestCompiler(
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=self.region())
        )
        ingest_view_manifest = manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        )
        output = (
            self.test_pipeline
            | beam.Create(
                read_ingest_view_results_fixture(
                    self.state_code(),
                    ingest_view_name,
                    "for_generate_entities_test.csv",
                ).to_dict("records")
            )
            | GenerateEntities(
                ingest_view_manifest=ingest_view_manifest,
                ingest_view_context=IngestViewContentsContext.build_for_tests(),
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
