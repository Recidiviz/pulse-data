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
"""Entrypoint for CloudSQL to BigQuery refresh to occur for a given CloudSQL instance"""
import argparse

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_control import (
    execute_cloud_sql_to_bq_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType


class BigQueryRefreshEntrypoint(EntrypointInterface):
    """Entrypoint for CloudSQL to BigQuery refresh to occur for a given CloudSQL instance"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the Cloud SQL to BQ refresh process."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--schema_type",
            help="The schema type that the refresh should occur for",
            type=SchemaType,
            choices=list(SchemaType),
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        execute_cloud_sql_to_bq_refresh(schema_type=args.schema_type)
