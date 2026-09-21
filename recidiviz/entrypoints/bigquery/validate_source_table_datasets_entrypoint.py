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
"""Entrypoint that validates the source table datasets owned by the update group of
the DAG this task runs in contain exactly the tables we expect."""

import argparse

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.source_table_cleanup_validation import (
    validate_clean_source_table_datasets,
)
from recidiviz.source_tables.source_table_update_group_dag import (
    source_table_update_group_for_dag_id,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    AirflowKubernetesPodEnvironment,
    in_airflow_kubernetes_pod,
)
from recidiviz.view_registry.deployed_source_table_repository import (
    build_source_table_repository_for_collected_schemata,
)


class ValidateSourceTableDatasetsEntrypoint(EntrypointInterface):
    """Entrypoint for validating source table datasets."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        return argparse.ArgumentParser()

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        if not in_airflow_kubernetes_pod():
            raise RuntimeError(
                "This entrypoint may only be run within the Airflow DAG's "
                "KubernetesPodOperator."
            )

        project_id = metadata.project_id()

        # Validate only the source tables in this DAG's own update group. Each DAG
        # creates its group's tables on its own schedule, so validating any other
        # group's tables here would report them missing until that DAG has run.
        update_group = source_table_update_group_for_dag_id(
            AirflowKubernetesPodEnvironment.get_dag_id(), project_id=project_id
        )
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=project_id
        ).filter_to_update_group(update_group)
        validate_clean_source_table_datasets(
            bq_client=BigQueryClientImpl(),
            source_table_repository=source_table_repository,
        )
