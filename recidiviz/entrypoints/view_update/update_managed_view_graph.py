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
"""Entrypoint for updating the views in a named deployed view graph - to be called
only within an Airflow DAG's KubernetesPodOperator."""
import argparse

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.source_table_update_group_dag import (
    source_table_update_group_for_dag_id,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    AirflowKubernetesPodEnvironment,
    NotInAirflowKubernetesPodError,
)
from recidiviz.view_registry.deployed_view_graphs import deployed_view_graph_registry
from recidiviz.view_registry.execute_view_graph_update import execute_view_graph_update


class UpdateManagedViewGraphEntrypoint(EntrypointInterface):
    """Entrypoint for updating the views in a named deployed view graph. The graph's
    input source tables must be owned by the DAG this entrypoint runs in."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the view graph update process."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--view_graph_name", type=str, required=True)
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        project_id = metadata.project_id()
        view_graph = deployed_view_graph_registry(project_id).graph_for_name(
            args.view_graph_name
        )

        try:
            # Ensure the view graph expects this DAG to update it
            dag_id = AirflowKubernetesPodEnvironment.get_dag_id()
            dag_update_group = source_table_update_group_for_dag_id(
                dag_id, project_id=project_id
            )
            if view_graph.input_source_table_update_group is not dag_update_group:
                raise ValueError(
                    f"View graph [{view_graph.name}] reads source tables in update group "
                    f"[{view_graph.input_source_table_update_group.value}], but DAG "
                    f"[{dag_id}] owns update group [{dag_update_group.value}]. Update this "
                    f"graph from the DAG that owns its input source tables."
                )
        except NotInAirflowKubernetesPodError as exc:
            raise RuntimeError(
                "This entrypoint must be run from within an Airflow DAG's "
                "KubernetesPodOperator."
            ) from exc

        execute_view_graph_update(view_graph)
