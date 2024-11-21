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
"""
Module responsible for updating BigQuery source table schemata.
This is primarily called via the `update_big_query_table_schemata` Airflow calculation DAG task,
but in case it needs to be run outside of that context, a CLI is provided.

# To validate the `recidiviz/source_tables/schema` YAML files run
python -m recidiviz.source_tables.update_big_query_table_schemas \
  --project-id [project_id] \
  --action validate_externally_managed_table_schemata

# To perform a dry run of all source table schema updates
python -m recidiviz.source_tables.update_big_query_table_schemas \
  --project-id recidiviz-staging \
  --action dry_run
"""
import argparse
import logging
import os
from pprint import pprint

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_view_source_table_configs,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tools.deploy.logging import get_deploy_logs_dir
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id


def collect_managed_source_table_collections(
    source_table_repository: SourceTableRepository,
) -> list[SourceTableCollection]:
    """Builds a list of source table collections to manage"""

    return [
        source_table_collection
        for source_table_collection in source_table_repository.source_table_collections
        if source_table_collection.update_config.attempt_to_manage
    ]


def update_all_source_table_schemas(
    source_table_collections: list[SourceTableCollection],
    update_manager: SourceTableUpdateManager | None = None,
    *,
    dry_run: bool = True,
    log_output: bool = False,
) -> None:
    """Given a repository of source tables, update BigQuery to match"""
    if not update_manager:
        update_manager = SourceTableUpdateManager(client=BigQueryClientImpl())

    if dry_run:
        changes = update_manager.dry_run(
            source_table_collections=source_table_collections,
            log_file=os.path.join(
                get_deploy_logs_dir(), "update_all_source_table_schemas.log"
            ),
        )
        if changes:
            logging.info("Dry run found the following changes:")
            pprint(changes)
        else:
            logging.info("Dry run found no changes to be made.")
    else:
        update_manager.update_async(
            source_table_collections=source_table_collections,
            log_file=os.path.join(
                get_deploy_logs_dir(), "update_all_source_table_schemas.log"
            ),
            log_output=log_output,
        )


def validate_externally_managed_table_schemata(
    source_table_repository: SourceTableRepository,
) -> None:
    update_manager = SourceTableUpdateManager()
    logging.info("Validating source table YAML definitions match BigQuery resources...")
    changes = update_manager.dry_run(
        source_table_collections=[
            source_table_collection
            for source_table_collection in source_table_repository.source_table_collections
            if not source_table_collection.update_config.attempt_to_manage
        ],
        log_file=os.path.join(
            get_deploy_logs_dir(), "update_all_source_table_schemas_dry_run.log"
        ),
    )

    if changes:
        pprint(changes)
        raise ValueError(
            "Cannot continue with BigQuery schema update, source tables do not match YAML definitions.\n"
            f"If the schemas in {project_id()} are as we expect them to be for these tables,"
            " resync their definitions using the recidiviz.tools.update_source_table_yaml script.\n"
            "Otherwise, update these tables manually in BigQuery so their schemas match the YAML definitions in"
            " recidiviz/source_tables/schema"
        )


def perform_bigquery_table_schema_update(dry_run: bool, log_output: bool) -> None:
    repository = build_source_table_repository_for_collected_schemata(
        project_id=project_id(),
    )

    validate_externally_managed_table_schemata(
        source_table_repository=repository,
    )

    update_all_source_table_schemas(
        source_table_collections=collect_managed_source_table_collections(
            source_table_repository=repository
        ),
        dry_run=dry_run,
        log_output=log_output,
    )

    # TODO(#30495): These will not need to be added separately once ingest views define
    #  their schemas in the YAML mappings definitions and we can collect these ingest
    #  view tables with all the other source tables.
    logging.info("Building source table configs for ingest views...")
    update_all_source_table_schemas(
        source_table_collections=build_ingest_view_source_table_configs(
            bq_client=BigQueryClientImpl(),
            state_codes=get_direct_ingest_states_existing_in_env(),
        ),
        dry_run=dry_run,
        log_output=log_output,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--action",
        type=str,
        choices=["dry_run", "validate_externally_managed_table_schemata"],
    )
    args = parser.parse_args()
    with local_project_id_override(args.project_id):
        if args.action == "dry_run":
            perform_bigquery_table_schema_update(
                dry_run=True,
                log_output=True,
            )
        elif args.action == "validate_externally_managed_table_schemata":
            validate_externally_managed_table_schemata(
                source_table_repository=build_source_table_repository_for_collected_schemata(
                    project_id=args.project_id,
                )
            )
