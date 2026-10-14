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
"""Entrypoint for updating a single LLM extractor's view graph at a single stage."""

import argparse

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.views.llm_extraction_view_graphs import (
    LLMExtractionViewGraphStage,
    build_llm_extraction_view_graph_for_stage,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.source_table_update_group_dag import (
    source_table_update_group_for_dag_id,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    AirflowKubernetesPodEnvironment,
    NotInAirflowKubernetesPodError,
)
from recidiviz.view_registry.execute_view_graph_update import execute_view_graph_update


class UpdateLLMExtractionManagedViewGraphEntrypoint(EntrypointInterface):
    """Entrypoint for updating a single LLM extractor's view graph at a single
    stage."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the LLM extraction view graph update process."""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--state_code",
            help="The state whose extractor view graph to update.",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )
        parser.add_argument(
            "--extractor_collection_name",
            help="The extractor collection whose view graph to update (e.g. "
            "CASE_NOTE_EMPLOYMENT_INFO).",
            type=str,
            required=True,
        )
        parser.add_argument(
            "--stage",
            help="The pipeline stage whose views to update: the first-order views "
            "after first-order extraction, or the entity-resolution views after the "
            "entity-resolution pass.",
            type=LLMExtractionViewGraphStage,
            choices=list(LLMExtractionViewGraphStage),
            required=True,
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        project_id = metadata.project_id()

        try:
            dag_id = AirflowKubernetesPodEnvironment.get_dag_id()
        except NotInAirflowKubernetesPodError as exc:
            raise RuntimeError(
                "This entrypoint must be run from within an Airflow DAG's "
                "KubernetesPodOperator."
            ) from exc

        view_graph = build_llm_extraction_view_graph_for_stage(
            first_order_config=get_first_order_llm_extractor_config(
                state_code=args.state_code,
                collection_name=args.extractor_collection_name,
            ),
            stage=args.stage,
            project_id=project_id,
        )
        if view_graph is None:
            raise ValueError(
                f"Extractor collection [{args.extractor_collection_name}] in state "
                f"[{args.state_code.value}] has no [{args.stage.value}] view graph."
            )

        dag_update_group = source_table_update_group_for_dag_id(
            dag_id, project_id=project_id
        )
        if dag_update_group is not view_graph.input_source_table_update_group:
            raise ValueError(
                f"[{view_graph.name}] read source tables in update group "
                f"[{view_graph.input_source_table_update_group.value}], but DAG "
                f"[{dag_id}] owns update group [{dag_update_group.value}]. "
                "These values must match. Update the view graph update group "
                "or run in the correct DAG."
            )

        execute_view_graph_update(
            view_graph=view_graph,
        )
