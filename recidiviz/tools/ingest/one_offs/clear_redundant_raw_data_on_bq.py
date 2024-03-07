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
"""
Local script for clearing out redundant raw data on BQ for states with frequent historical uploads (and updates Postgres
metadata accordingly).

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run True --project-id=recidiviz-staging --state-code=US_XX
"""

import argparse
import logging
import sys
from typing import Dict, List, Tuple

import sqlalchemy
from google.cloud import exceptions

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.string import StrictStringFormatter

DATASET_ID_TEMPLATE = "{project_id}.{state_code}_raw_data.{table_name}"

RAW_FILE_QUERY_TEMPLATE = (
    """DELETE FROM `{table}` WHERE file_id not in ({min_file_id}, {max_file_id})"""
)

POSTGRES_FILE_ID_IN_BQ = """SELECT DISTINCT file_id FROM `{table}` WHERE file_id in ({min_file_id}, {max_file_id})"""


def get_postgres_min_and_max_update_datetime_by_file_tag(
    session: Session,
    state_code: StateCode,
) -> Dict[str, Tuple[str, str]]:
    """Returns a dictionary of file tags to their associated (non-invalidated) raw file min and max
    update_datetime.
    """
    command = (
        "SELECT file_tag, min(update_datetime) as min_datetimes_contained, "
        "max(update_datetime) as max_datetimes_contained "
        "FROM direct_ingest_raw_file_metadata "
        f"WHERE region_code = '{state_code.value}' "
        "AND is_invalidated is False "
        "AND raw_data_instance = 'PRIMARY'"
        f"GROUP BY file_tag;"
    )
    results = session.execute(sqlalchemy.text(command))
    results_dict = {result[0]: (result[1], result[2]) for result in results}
    return dict(sorted(results_dict.items()))


def postgres_file_ids_present_in_bq(
    session: Session,
    state_code: StateCode,
    file_tag: str,
    bq_client: BigQueryClient,
    table_bq_path: str,
    min_datetimes_contained: str,
    max_datetimes_contained: str,
) -> List[str]:
    """Validate whether the file_ids associated with min and max `update_datetime` on
    Postgres are also present on BQ."""
    logging.info(
        "[%s] Generating Postgres query to identify file_ids from min and max dates...",
        file_tag,
    )
    command = (
        "SELECT DISTINCT file_id "
        "FROM direct_ingest_raw_file_metadata "
        f"WHERE region_code = '{state_code.value}' "
        f"AND file_tag = '{file_tag}' "
        f"AND update_datetime in ('{min_datetimes_contained}', "
        f"'{max_datetimes_contained}')"
        "AND raw_data_instance = 'PRIMARY';"
    )
    postgres_results = session.execute(sqlalchemy.text(command))
    logging.info("[%s] %s", file_tag, command)
    postgres_file_ids = [result[0] for result in postgres_results]
    if len(postgres_file_ids) < 2:
        return []

    logging.info(
        "[%s] Postgres min and max file_ids: min=(%s, %s), max=(%s, %s)",
        file_tag,
        postgres_file_ids[0],
        min_datetimes_contained,
        postgres_file_ids[1],
        max_datetimes_contained,
    )

    postgres_confirmation_query = StrictStringFormatter().format(
        POSTGRES_FILE_ID_IN_BQ,
        table=table_bq_path,
        min_file_id=postgres_file_ids[0],
        max_file_id=postgres_file_ids[1],
    )
    logging.info(
        "[%s] Running query to see if Postgres file_ids are present on BQ.", file_tag
    )
    try:
        logging.info("[%s] %s", file_tag, postgres_confirmation_query)
        query_job = bq_client.run_query_async(
            query_str=postgres_confirmation_query, use_query_cache=True
        )
        query_job.result()
        bq_file_ids = [row["file_id"] for row in query_job]
        logging.info(
            "[%s] Postgres file_ids: %s. BQ file_ids: %s.",
            file_tag,
            postgres_file_ids,
            bq_file_ids,
        )
        if set(postgres_file_ids) == set(bq_file_ids):
            return bq_file_ids
        return []
    except exceptions.NotFound as e:
        logging.info("[%s] Table not found: %s", file_tag, str(e))
        return []


def get_redundant_raw_file_ids(
    session: Session,
    state_code: StateCode,
    file_tag: str,
    min_datetimes_contained: str,
    max_datetimes_contained: str,
) -> List[int]:
    """For a given file_tag, returns a list of file_ids whose `update_datetime` times are
    within the bounds the associated (non-invalidated) min and max update_datetime."""
    command = (
        "SELECT DISTINCT file_id "
        "FROM direct_ingest_raw_file_metadata "
        f"WHERE region_code = '{state_code.value}' "
        f"AND file_tag = '{file_tag}' "
        f"AND update_datetime > '{min_datetimes_contained}' "
        f"AND update_datetime < '{max_datetimes_contained}' "
        "AND raw_data_instance = 'PRIMARY'"
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

    bq_client: BigQueryClient = BigQueryClientImpl()
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    raw_data_metadata_manager = DirectIngestRawFileMetadataManager(
        state_code.value, DirectIngestInstance.PRIMARY
    )

    with SessionFactory.for_proxy(database_key) as session:
        file_tag_to_min_and_max_update_datetimes: Dict[
            str, Tuple[str, str]
        ] = get_postgres_min_and_max_update_datetime_by_file_tag(session, state_code)
        for file_tag, (
            min_update_datetime,
            max_update_datetime,
        ) in file_tag_to_min_and_max_update_datetimes.items():
            if file_tag not in raw_file_configs.keys():
                logging.info(
                    "[%s][Skipping] File tag found in Postgres but not in raw YAML files.",
                    file_tag,
                )
                continue

            if raw_file_configs[file_tag].always_historical_export is False:
                logging.info(
                    "[%s][Skipping] `always_historical_export` set to False.",
                    file_tag,
                )
                continue
            logging.info(
                "[%s] `always_historical_export` set to True. Moving forward with raw data pruning.",
                file_tag,
            )

            file_ids_to_delete: List[int] = get_redundant_raw_file_ids(
                session,
                state_code,
                file_tag,
                min_update_datetime,
                max_update_datetime,
            )

            table_bq_path = StrictStringFormatter().format(
                DATASET_ID_TEMPLATE,
                project_id=project_id,
                state_code=state_code.value.lower(),
                table_name=file_tag,
            )

            min_and_max_file_ids_in_bq = postgres_file_ids_present_in_bq(
                session=session,
                state_code=state_code,
                file_tag=file_tag,
                bq_client=bq_client,
                table_bq_path=table_bq_path,
                min_datetimes_contained=min_update_datetime,
                max_datetimes_contained=max_update_datetime,
            )

            if not min_and_max_file_ids_in_bq:
                logging.error(
                    "[%s] Skipping deletion because the file_ids identified as min and max on Postgres "
                    "were not found on BQ.",
                    file_tag,
                )
                continue

            deletion_query = StrictStringFormatter().format(
                RAW_FILE_QUERY_TEMPLATE,
                table=table_bq_path,
                min_file_id=min_and_max_file_ids_in_bq[0],
                max_file_id=min_and_max_file_ids_in_bq[1],
            )

            logging.info(
                "[%s] Postgres file ids found on BQ! Proceeding with deletion.",
                file_tag,
            )
            if dry_run:
                logging.info("[%s][DRY RUN] Would run %s", file_tag, deletion_query)
                logging.info(
                    "[%s][DRY RUN] Would set direct_ingest_raw_file_metadata.is_invalidated to True for"
                    " %d rows.",
                    file_tag,
                    len(file_ids_to_delete),
                )
            else:
                logging.info("[%s] Running deletion query in BQ...", file_tag)
                query_job = bq_client.run_query_async(
                    query_str=deletion_query, use_query_cache=True
                )
                query_job.result()
                logging.info(
                    "[%s] Marking %d metadata rows as invalidated...",
                    file_tag,
                    len(file_ids_to_delete),
                )
                for file_id in file_ids_to_delete:
                    raw_data_metadata_manager.mark_file_as_invalidated_by_file_id(
                        session, file_id
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

    if not args.dry_run:
        prompt_for_confirmation(
            f"[{args.state_code.value}][{args.project_id}] Execute raw data pruning? [n] will skip this state/project pair",
            exit_code=0,
        )

        prompt_for_confirmation(
            f"Have you confirmed that there are NO tasks running for this state in {args.project_id}: "
            f" https://console.cloud.google.com/cloudtasks?referrer=search&project={args.project_id}?"
        )
        prompt_for_confirmation(
            "Pause queues: "
            f"https://{args.project_id}.ue.r.appspot.com/admin/ingest_operations/key_actions/{args.state_code.value}/ingest_queues?"
        )
        prompt_for_confirmation(
            f"Are you sure this state receives frequent historical uploads {args.state_code.value}?"
        )

    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            main(args.dry_run, args.state_code, args.project_id)

    if not args.dry_run:
        prompt_for_confirmation(
            f"Unpause queues: "
            f"https://{args.project_id}.ue.r.appspot.com/admin/ingest_operations/key_actions={args.state_code.value}/ingest_queues?"
        )
