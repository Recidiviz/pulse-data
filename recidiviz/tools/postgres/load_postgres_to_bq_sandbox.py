# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A script for loading the current state of a Postgres instance into a sandbox BigQuery
dataset. Can be used to test changes to the CloudSQL -> BigQuery refresh. Or when
debugging issues in Postgres when the data has not yet been loaded to BigQuery (either
because refresh for a given region is paused, or the daily refresh just hasn't run yet).

Usage:
    python -m recidiviz.tools.postgres.load_postgres_to_bq_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
        --schema [SCHEMA]
Example:
    python -m recidiviz.tools.postgres.load_postgres_to_bq_sandbox \
        --project_id recidiviz-staging \
        --sandbox_dataset_prefix ageiduschek \
        --schema OPERATIONS

"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def main(sandbox_dataset_prefix: str, schema_type: SchemaType) -> None:
    """Defines the main function responsible for moving data from Postgres to BQ."""

    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        raise ValueError(f"Unsupported schema type: [{schema_type}]")

    logging.info("Prefixing all output datasets with [%s_].", sandbox_dataset_prefix)
    federated_bq_schema_refresh(
        schema_type=schema_type,
        dataset_override_prefix=sandbox_dataset_prefix,
    )
    config = CloudSqlToBQConfig.for_schema_type(schema_type)
    final_destination_dataset = config.multi_region_dataset(
        dataset_override_prefix=sandbox_dataset_prefix
    )

    logging.info(
        "Load complete. Data loaded to dataset [%s].", final_destination_dataset
    )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the datasets where data will be loaded.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--schema",
        type=SchemaType,
        choices=list(SchemaType),
        help="Specifies which schema to generate migrations for.",
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        main(
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            schema_type=known_args.schema,
        )
