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
"""Script for updating BigQuery source table schemata - to be called only within the Airflow DAG's
KubernetesPodOperator."""
import argparse

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.update_big_query_table_schemas import (
    SourceTableCheckType,
    check_source_table_schemas,
    update_all_managed_source_table_schemas,
)
from recidiviz.utils import metadata


class UpdateBigQuerySourceTableSchemataEntrypoint(EntrypointInterface):
    """Entrypoint for updating source table schemata"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the BigQuery schema update process."""
        # TODO(#27373): We likely will want to support a sandbox_dataset_prefix argument here for creation of sandbox
        # output tables
        parser = argparse.ArgumentParser()
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        repository = build_source_table_repository_for_collected_schemata(
            project_id=metadata.project_id(),
        )

        # Verify that none of the externally managed schemas have changed under us
        # without a corresponding YAML change.
        check_source_table_schemas(
            source_table_repository=repository,
            source_table_check_type=SourceTableCheckType.EXTERNALLY_MANAGED,
        )

        update_all_managed_source_table_schemas(
            source_table_repository=repository,
        )
