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
"""Tests for update_managed_view_graph.py"""
import os
import unittest
from unittest.mock import patch

from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    BigQueryViewGraphRegistry,
    ResolvedBigQueryViewGraph,
)
from recidiviz.entrypoints.view_update.update_managed_view_graph import (
    UpdateManagedViewGraphEntrypoint,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA
from recidiviz.utils.airflow_dag import AirflowDag
from recidiviz.utils.environment import DAG_ID
from recidiviz.utils.metadata import local_project_id_override

_PROJECT_ID = "recidiviz-456"


def _view_builder(dataset_id: str, view_id: str) -> SimpleBigQueryViewBuilder:
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_id,
        view_id=view_id,
        description=f"{view_id} description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        should_materialize=False,
        schema=MINIMAL_SCHEMA,
    )


def _resolved_graph(
    name: str,
    dataset_id: str,
    update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.CALC,
) -> ResolvedBigQueryViewGraph:
    return ResolvedBigQueryViewGraph(
        project_id=_PROJECT_ID,
        view_graph=BigQueryViewGraph(
            name=name,
            view_builder_candidates=[_view_builder(dataset_id, "table")],
            input_source_table_update_group=update_group,
        ),
        input_source_table_collections=[_source_table_collection(update_group)],
    )


def _source_table_collection(
    update_group: SourceTableUpdateGroup,
) -> SourceTableCollection:
    collection = SourceTableCollection(
        dataset_id="source_dataset",
        description="source_dataset description",
        update_config=SourceTableCollectionUpdateConfig.protected(),
        update_groups={update_group},
    )
    collection.add_source_table(
        table_id="source_table", schema_fields=[SchemaField("col", "STRING")]
    )
    return collection


class TestUpdateManagedViewGraphEntrypoint(unittest.TestCase):
    """Tests for UpdateManagedViewGraphEntrypoint."""

    def setUp(self) -> None:
        self.graph_1 = _resolved_graph("graph_1", "dataset_1")
        self.graph_2 = _resolved_graph("graph_2", "dataset_2")
        self.llm_graph = _resolved_graph(
            "llm_graph",
            "dataset_3",
            update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        self.registry_patcher = patch(
            "recidiviz.entrypoints.view_update.update_managed_view_graph"
            ".deployed_view_graph_registry",
            return_value=BigQueryViewGraphRegistry(
                project_id=_PROJECT_ID,
                view_graphs=[self.graph_1, self.graph_2, self.llm_graph],
            ),
        )
        self.registry_patcher.start()

        self.dag_id_patcher = patch.dict(
            os.environ, {DAG_ID: AirflowDag.CALCULATION.dag_id(_PROJECT_ID)}
        )
        self.dag_id_patcher.start()

        self.execute_patcher = patch(
            "recidiviz.entrypoints.view_update.update_managed_view_graph"
            ".execute_view_graph_update"
        )
        self.mock_execute = self.execute_patcher.start()

    def tearDown(self) -> None:
        self.registry_patcher.stop()
        self.dag_id_patcher.stop()
        self.execute_patcher.stop()

    def _run_entrypoint(self, view_graph_name: str) -> None:
        """Runs the entrypoint as the entrypoint executor would, parsing the given
        graph name through the entrypoint's own parser."""
        args = UpdateManagedViewGraphEntrypoint.get_parser().parse_args(
            [f"--view_graph_name={view_graph_name}"]
        )
        with local_project_id_override(_PROJECT_ID):
            UpdateManagedViewGraphEntrypoint.run_entrypoint(args=args)

    def test_view_graph_name_selects_named_graph(self) -> None:
        self._run_entrypoint("graph_1")
        self.mock_execute.assert_called_once_with(self.graph_1)

    def test_view_graph_name_selects_other_named_graph(self) -> None:
        self._run_entrypoint("graph_2")
        self.mock_execute.assert_called_once_with(self.graph_2)

    def test_unknown_view_graph_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found no view graph with name \[unknown_graph\]$"
        ):
            self._run_entrypoint("unknown_graph")
        self.mock_execute.assert_not_called()

    def test_graph_owned_by_other_dag_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^View graph \[llm_graph\] reads source tables in update group "
            r"\[LLM_DOCUMENT_EXTRACTION\], but DAG \[recidiviz-456_calculation_dag\] "
            r"owns update group \[CALC\]\.",
        ):
            self._run_entrypoint("llm_graph")
        self.mock_execute.assert_not_called()

    def test_graph_owned_by_current_dag_runs(self) -> None:
        with patch.dict(
            os.environ,
            {DAG_ID: AirflowDag.LLM_DOCUMENT_EXTRACTION.dag_id(_PROJECT_ID)},
        ):
            self._run_entrypoint("llm_graph")
        self.mock_execute.assert_called_once_with(self.llm_graph)

    def test_outside_airflow_pod_raises(self) -> None:
        with patch.dict(os.environ, clear=True):
            with self.assertRaisesRegex(
                RuntimeError, r"^This entrypoint must be run from within"
            ):
                self._run_entrypoint("graph_1")
        self.mock_execute.assert_not_called()

    def test_view_graph_name_is_required(self) -> None:
        with self.assertRaises(SystemExit):
            UpdateManagedViewGraphEntrypoint.get_parser().parse_args([])
