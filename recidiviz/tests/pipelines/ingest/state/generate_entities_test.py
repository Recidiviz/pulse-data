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
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestGenerateEntities(StateIngestPipelineTestCase):
    """Tests the GenerateEntities PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def test_generate_entities(self) -> None:
        expected_output = [
            (
                datetime.fromisoformat("2022-07-02T00:00:00").timestamp(),
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
                datetime.fromisoformat("2022-07-04T00:00:00").timestamp(),
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
        output = (
            self.test_pipeline
            | beam.Create(
                self.get_expected_ingest_view_results(
                    ingest_view_name="ingest12", test_name="ingest12"
                )
            )
            | pipeline.GenerateEntities(
                state_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                ingest_view_name="ingest12",
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
