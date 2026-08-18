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
"""Tests for activity_pipeline_output_table_collector."""
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.activity import entities, normalized_entities
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.source_tables.activity_pipeline_output_table_collector import (
    build_normalized_state_output_source_table_collection,
    build_state_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineOutputSourceTableLabel,
    StateSpecificSourceTableLabel,
)

_STATE_CODE = StateCode.US_OZ


class TestBuildStateOutputSourceTableCollection(unittest.TestCase):
    """Tests for build_state_output_source_table_collection."""

    def setUp(self) -> None:
        super().setUp()
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_dataset_id_is_state_specific(self) -> None:
        collection = build_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(collection.dataset_id, "us_oz_state")

    def test_carries_ingest_pipeline_and_state_labels(self) -> None:
        collection = build_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(
            [
                DataflowPipelineOutputSourceTableLabel(INGEST_PIPELINE_NAME),
                StateSpecificSourceTableLabel(state_code=_STATE_CODE),
            ],
            collection.labels,
        )

    def test_passes_entity_tables_through_unmodified(self) -> None:
        """The collector must emit exactly the tables and schemas defined by the
        state entities module, unfiltered and unmodified."""
        collection = build_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(
            get_bq_schema_for_entities_module(entities),
            {t.address.table_id: t.schema_fields for t in collection.source_tables},
        )


class TestBuildNormalizedStateOutputSourceTableCollection(unittest.TestCase):
    """Tests for build_normalized_state_output_source_table_collection."""

    def setUp(self) -> None:
        super().setUp()
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_dataset_id_is_state_specific(self) -> None:
        collection = build_normalized_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(collection.dataset_id, "us_oz_normalized_state")

    def test_carries_ingest_pipeline_and_state_labels(self) -> None:
        collection = build_normalized_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(
            [
                DataflowPipelineOutputSourceTableLabel(INGEST_PIPELINE_NAME),
                StateSpecificSourceTableLabel(state_code=_STATE_CODE),
            ],
            collection.labels,
        )

    def test_passes_entity_tables_through_unmodified(self) -> None:
        """The collector must emit exactly the tables and schemas defined by the
        normalized state entities module, unfiltered and unmodified."""
        collection = build_normalized_state_output_source_table_collection(_STATE_CODE)
        self.assertEqual(
            get_bq_schema_for_entities_module(normalized_entities),
            {t.address.table_id: t.schema_fields for t in collection.source_tables},
        )
