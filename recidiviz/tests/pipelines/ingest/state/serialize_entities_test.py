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
"""Tests the SerializeEntities DoFn."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that
from apache_beam.testing.util import is_not_empty

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import get_state_entity_names
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestSerializeEntities(StateIngestPipelineTestCase):
    """Tests the SerializeEntities DoFn."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def test_serialize_entities(self) -> None:
        root_entities = [
            generate_full_graph_state_person(
                set_back_edges=True, include_person_back_edges=True, set_ids=True
            ),
            generate_full_graph_state_staff(set_back_edges=True, set_ids=True),
        ]
        state_tables = get_state_entity_names()

        output = (
            self.test_pipeline
            | beam.Create(root_entities)
            | beam.ParDo(
                pipeline.SerializeEntities(state_code=StateCode.US_DD)
            ).with_outputs(*state_tables)
        )
        for state_table in state_tables:
            assert_that(getattr(output, state_table), is_not_empty())
        self.test_pipeline.run()
