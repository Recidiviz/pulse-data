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
"""Tests for update_llm_extraction_managed_view_graph.py"""

import os
import unittest
from unittest.mock import patch

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_graph import BigQueryViewGraph
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.views.llm_extraction_view_graphs import (
    LLMExtractionViewGraphStage,
)
from recidiviz.entrypoints.view_update.update_llm_extraction_managed_view_graph import (
    UpdateLLMExtractionManagedViewGraphEntrypoint,
)
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA
from recidiviz.utils.airflow_dag import AirflowDag
from recidiviz.utils.environment import DAG_ID
from recidiviz.utils.metadata import local_project_id_override

_PROJECT_ID = "recidiviz-456"
_COLLECTION = "CASE_NOTE_EMPLOYMENT_INFO"


def _llm_graph(
    update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
) -> BigQueryViewGraph:
    return BigQueryViewGraph.build(
        project_id=_PROJECT_ID,
        name="llm_extraction__case_note_employment_info__first_order",
        input_source_table_update_group=update_group,
        view_builder_candidates=[
            SimpleBigQueryViewBuilder(
                dataset_id="us_xx_document_extraction_results_unvalidated",
                view_id="case_note_employment_info",
                description="description",
                view_query_template="SELECT * FROM `{project_id}.src.table`",
                should_materialize=False,
                schema=MINIMAL_SCHEMA,
            )
        ],
    )


class TestUpdateLLMExtractionManagedViewGraphEntrypoint(unittest.TestCase):
    """Tests for UpdateLLMExtractionManagedViewGraphEntrypoint."""

    def setUp(self) -> None:
        # The resolved config is handed straight to the graph builder, which is
        # itself mocked, so a sentinel config stands in for it.
        self.config_patcher = patch(
            "recidiviz.entrypoints.view_update.update_llm_extraction_managed_view_graph"
            ".get_first_order_llm_extractor_config"
        )
        self.mock_get_config = self.config_patcher.start()

        self.graph_patcher = patch(
            "recidiviz.entrypoints.view_update.update_llm_extraction_managed_view_graph"
            ".build_llm_extraction_view_graph_for_stage",
            return_value=_llm_graph(),
        )
        self.mock_build_graph = self.graph_patcher.start()

        self.dag_id_patcher = patch.dict(
            os.environ,
            {DAG_ID: AirflowDag.LLM_DOCUMENT_EXTRACTION.dag_id(_PROJECT_ID)},
        )
        self.dag_id_patcher.start()

        self.execute_patcher = patch(
            "recidiviz.entrypoints.view_update.update_llm_extraction_managed_view_graph"
            ".execute_view_graph_update"
        )
        self.mock_execute = self.execute_patcher.start()

    def tearDown(self) -> None:
        self.config_patcher.stop()
        self.graph_patcher.stop()
        self.dag_id_patcher.stop()
        self.execute_patcher.stop()

    def _run_entrypoint(
        self, *, state_code: str, extractor_collection_name: str, stage: str
    ) -> None:
        """Runs the entrypoint as the entrypoint executor would, parsing the given
        args through the entrypoint's own parser."""
        args = UpdateLLMExtractionManagedViewGraphEntrypoint.get_parser().parse_args(
            [
                f"--state_code={state_code}",
                f"--extractor_collection_name={extractor_collection_name}",
                f"--stage={stage}",
            ]
        )
        with local_project_id_override(_PROJECT_ID):
            UpdateLLMExtractionManagedViewGraphEntrypoint.run_entrypoint(args=args)

    def test_args_select_that_extractors_stage_graph(self) -> None:
        self._run_entrypoint(
            state_code="US_XX",
            extractor_collection_name=_COLLECTION,
            stage="entity_resolution",
        )
        self.mock_get_config.assert_called_once_with(
            state_code=StateCode.US_XX, collection_name=_COLLECTION
        )
        self.mock_build_graph.assert_called_once_with(
            first_order_config=self.mock_get_config.return_value,
            stage=LLMExtractionViewGraphStage.ENTITY_RESOLUTION,
            project_id=_PROJECT_ID,
        )
        self.mock_execute.assert_called_once_with(
            view_graph=self.mock_build_graph.return_value
        )

    def test_no_graph_for_stage_raises(self) -> None:
        self.mock_build_graph.return_value = None
        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor collection \[CASE_NOTE_EMPLOYMENT_INFO\] in state \[US_XX\] "
            r"has no \[entity_resolution\] view graph\.$",
        ):
            self._run_entrypoint(
                state_code="US_XX",
                extractor_collection_name=_COLLECTION,
                stage="entity_resolution",
            )
        self.mock_execute.assert_not_called()

    def test_graph_owned_by_other_dag_raises(self) -> None:
        with patch.dict(
            os.environ, {DAG_ID: AirflowDag.CALCULATION.dag_id(_PROJECT_ID)}
        ):
            with self.assertRaisesRegex(
                ValueError,
                r"^\[llm_extraction__case_note_employment_info__first_order\] read "
                r"source tables in update group \[LLM_DOCUMENT_EXTRACTION\], but DAG "
                r"\[recidiviz-456_calculation_dag\] owns update group \[CALC\]\.",
            ):
                self._run_entrypoint(
                    state_code="US_XX",
                    extractor_collection_name=_COLLECTION,
                    stage="first_order",
                )
        self.mock_execute.assert_not_called()

    def test_outside_airflow_pod_raises(self) -> None:
        with patch.dict(os.environ, clear=True):
            with self.assertRaisesRegex(
                RuntimeError, r"^This entrypoint must be run from within"
            ):
                self._run_entrypoint(
                    state_code="US_XX",
                    extractor_collection_name=_COLLECTION,
                    stage="first_order",
                )
        self.mock_build_graph.assert_not_called()
        self.mock_execute.assert_not_called()
