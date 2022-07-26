#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
Remote helper script run on the prod-data-client for clearing out redundant raw data on BQ for states with frequent
historical uploads (and updates Postgres metadata accordingly).

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq_remote_helper --dry-run=True --project-id=recidiviz-staging
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Tuple

import sqlalchemy

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import dao
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.string import StrictStringFormatter

DATASET_ID_TEMPLATE = "{project_id}.{state_code}_raw_data.{table_name}"

RAW_FILE_QUERY_TEMPLATE = (
    """DELETE FROM `{table}` WHERE file_id in {file_ids_to_delete}"""
)


def get_min_and_max_datetimes_contained_by_file_tag(
    session: Session,
    state_code: StateCode,
) -> Dict[str, Tuple[str, str]]:
    """Returns a dictionary of file tags to their associated (non-invalidated) raw file min and max
    datetimes_contained_upper_bound_inclusive.

    Note: datetimes_contained_upper_bound_inclusive in Postgres has the same value as update_datetime in the raw data
    tables on BQ."""
    command = (
        "SELECT file_tag, min(datetimes_contained_upper_bound_inclusive) as min_datetimes_contained, "
        "max(datetimes_contained_upper_bound_inclusive) as max_datetimes_contained "
        "FROM direct_ingest_raw_file_metadata "
        f"WHERE region_code = '{state_code.value}' "
        "AND is_invalidated is False "
        f"GROUP BY file_tag;"
    )
    results = session.execute(sqlalchemy.text(command))
    return {result[0]: (result[1], result[2]) for result in results}


def get_redundant_raw_file_ids(
    session: Session,
    state_code: StateCode,
    file_tag: str,
    min_datetimes_contained: str,
    max_datetimes_contained: str,
) -> List[int]:
    """For a given file_tag, returns a list of file_ids whose `datetimes_contained_upper_bound_inclusive` times are
    within the bounds the associated (non-invalidated) min and max datetimes_contained_upper_bound_inclusive."""
    command = (
        "SELECT DISTINCT file_id "
        "FROM direct_ingest_raw_file_metadata "
        f"WHERE region_code = '{state_code.value}' "
        f"AND file_tag = '{file_tag}' "
        f"AND datetimes_contained_upper_bound_inclusive > '{min_datetimes_contained}' "
        f"AND datetimes_contained_upper_bound_inclusive < '{max_datetimes_contained}' "
        "AND is_invalidated is False;"
    )
    results = session.execute(sqlalchemy.text(command))
    return [r[0] for r in results]


def get_raw_file_configs_for_state(
    state_code: StateCode,
) -> Dict[str, DirectIngestRawFileConfig]:
    region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)

    sorted_file_tags = sorted(region_config.raw_file_tags)

    raw_file_configs = {
        file_tag: region_config.raw_file_configs[file_tag]
        for file_tag in sorted_file_tags
    }

    return raw_file_configs


# TODO(#14127): delete this script once raw data pruning is live.
def main(dry_run: bool, state_code: StateCode, project_id: str) -> None:
    """Executes the main flow of the script.

    Iterates through each raw data table in the project and state specific raw data dataset
    (ex: recidiviz-staging.us_tn_raw_data.*), and using `direct_ingest_raw_file_metadata` table, identifies which file
     IDs have rows in BQ that should be deleted. If not in dry-run, it then deletes the rows associated with these
     file ids on BQ as well as marks the associated metadata as invalidated in `direct_ingest_raw_file_metadata` in
     Postgres."""
    raw_file_configs: Dict[
        str, DirectIngestRawFileConfig
    ] = get_raw_file_configs_for_state(state_code)

    is_prod = project_id == GCP_PROJECT_PRODUCTION
    ssl_cert_path = os.path.expanduser(
        f"~/{'prod' if is_prod else 'dev'}_operations_data_certs"
    )

    bq_client: BigQueryClient = BigQueryClientImpl()
    with SessionFactory.for_prod_data_client(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS),
        os.path.abspath(ssl_cert_path),
    ) as session:
        file_tag_to_min_and_max_discovery_time: Dict[
            str, Tuple[str, str]
        ] = get_min_and_max_datetimes_contained_by_file_tag(session, state_code)
        for file_tag, (
            min_datetimes_contained,
            max_datetimes_contained,
        ) in file_tag_to_min_and_max_discovery_time.items():
            if (
                file_tag in raw_file_configs.keys()
                and raw_file_configs[file_tag].always_historical_export is False
            ):
                logging.info(
                    "Skipping data clearing for %s since it has `always_historical_export` set to False.",
                    file_tag,
                )
                continue

            file_ids_to_delete: List[int] = get_redundant_raw_file_ids(
                session,
                state_code,
                file_tag,
                min_datetimes_contained,
                max_datetimes_contained,
            )

            logging.info(
                "%s: min_datetimes_contained=%s, max_datetimes_contained=%s. %d file_ids have associated "
                "`datetimes_contained_upper_bound_inclusive` between these bounds.",
                file_tag,
                min_datetimes_contained,
                max_datetimes_contained,
                len(file_ids_to_delete),
            )

            table_bq_path = StrictStringFormatter().format(
                DATASET_ID_TEMPLATE,
                project_id=project_id,
                state_code=state_code.value.lower(),
                table_name=file_tag,
            )

            deletion_query = StrictStringFormatter().format(
                RAW_FILE_QUERY_TEMPLATE,
                table=table_bq_path,
                file_ids_to_delete=file_ids_to_delete,
            )

            if dry_run:
                logging.info("[DRY RUN] Would run %s", deletion_query)
                logging.info(
                    "[DRY RUN] Would set direct_ingest_raw_file_metadata.is_invalidated to True for %d rows for %s.",
                    len(file_ids_to_delete),
                    file_tag,
                )
            else:
                logging.info("Running deletion query in BQ for %s...", file_tag)
                bq_client.run_query_async(deletion_query)
                logging.info(
                    "Marking %d metadata rows as invalidated...",
                    len(file_ids_to_delete),
                )
                for file_id in file_ids_to_delete:
                    dao.mark_raw_file_as_invalidated(
                        session,
                        state_code.value,
                        file_id,
                        DirectIngestInstance.PRIMARY,
                    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument("--state-code", type=StateCode, required=True)

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.dry_run, args.state_code, args.project_id)
