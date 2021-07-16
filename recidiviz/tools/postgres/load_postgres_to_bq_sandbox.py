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
        [--direct-ingest-instance [PRIMARY,SECONDARY]]
Example:
    python -m recidiviz.tools.postgres.load_postgres_to_bq_sandbox \
        --project_id recidiviz-staging \
        --sandbox_dataset_prefix my_prefix \
        --schema STATE
        --direct-ingest-instance SECONDARY

"""
import argparse
import logging
import sys
from typing import List, Optional, Tuple
from unittest import mock

from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_config
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STANDARD_YAML_CONTENTS = """
region_codes_to_exclude: []
state_history_tables_to_include:
- state_person_history
county_columns_to_exclude:
person:
- full_name
- birthdate_inferred_from_age
"""


def main(
    sandbox_dataset_prefix: str,
    schema_type: SchemaType,
    direct_ingest_instance: Optional[DirectIngestInstance],
) -> None:
    """Defines the main function responsible for moving data from Postgres to BQ."""
    logging.info(
        "Prefixing all output datasets with [%s_].", known_args.sandbox_dataset_prefix
    )
    fake_gcs = FakeGCSFileSystem()

    # We mock the export config to a version that does not have any paused regions.
    with mock.patch(
        f"{cloud_sql_to_bq_refresh_config.__name__}.GcsfsFactory.build",
        return_value=fake_gcs,
    ):
        fake_gcs.upload_from_string(
            path=CloudSqlToBQConfig.default_config_path(),
            contents=STANDARD_YAML_CONTENTS,
            content_type="text/yaml",
        )
        federated_bq_schema_refresh(
            schema_type=schema_type,
            direct_ingest_instance=direct_ingest_instance,
            dataset_override_prefix=sandbox_dataset_prefix,
        )
        config = CloudSqlToBQConfig.for_schema_type(schema_type)
        final_destination_dataset = config.unioned_multi_region_dataset(
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
    parser.add_argument(
        "--direct-ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="When using the STATE schema, allows the user to specify which instance to use. "
        "If unspecified, this defaults to the PRIMARY instance.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        main(
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            schema_type=known_args.schema,
            direct_ingest_instance=known_args.direct_ingest_instance,
        )
