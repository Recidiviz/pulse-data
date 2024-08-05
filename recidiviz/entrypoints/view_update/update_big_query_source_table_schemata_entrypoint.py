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
from recidiviz.source_tables.update_big_query_table_schemas import (
    perform_bigquery_table_schema_update,
)
from recidiviz.utils.params import str_to_bool


class UpdateBigQuerySourceTableSchemataEntrypoint(EntrypointInterface):
    """Entrypoint for updating source table schemata"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the BigQuery schema update process."""
        # TODO(#27373): We likely will want to support a sandbox_dataset_prefix argument here for creation of sandbox
        # output tables
        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", type=str_to_bool, default=False)

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        perform_bigquery_table_schema_update(dry_run=args.dry_run, log_output=True)
