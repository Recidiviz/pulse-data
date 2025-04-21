# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Script to update the tables to sync from their BQ each day.

Example usage:

To see what tables are currently included, run

python -m recidiviz.tools.ingest.regions.us_ut.update_sync_tables \
    --dry-run True

If you recently added a raw file config that is not included in the normal sync and want
to sync that table regularly, run (n.b. file tag names need to exactly match table id
names)

python -m recidiviz.tools.ingest.regions.us_ut.update_sync_tables \
    --add-missing-file-configs
    --dry-run True

If you want to remove specific table(s)

python -m recidiviz.tools.ingest.regions.us_ut.update_sync_tables \
    --table-ids-to-remove [table1,table2]
    --dry-run True

If you want to add specific tables(s)

python -m recidiviz.tools.ingest.regions.us_ut.update_sync_tables \
    --table-ids-to-add [table1,table2]
    --dry-run True



"""
import argparse
import datetime
import logging
import sys
from enum import Enum

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool, str_to_list

US_UT_INGEST_PROJECT_ID = "recidiviz-ingest-us-ut"
LOGGING_WIDTH = 80
FILL_CHAR = "#"

# ledger-style table that tracks all changes to sync tables. in theory, this script is
# the only process that writes to this table.
TABLES_TO_SYNC_ADDRESS = BigQueryAddress(
    dataset_id="metadata", table_id="tables_to_sync"
)
# view on top of `tables_to_sync` that just displays the "current" set of tables that are
# active. the sync job uses this view to determine which tables to copy over.
ACTIVE_TABLES_TO_SYNC_ADDRESS = BigQueryAddress(
    dataset_id="metadata", table_id="tables_to_sync__current"
)


class TableUpdateAction(Enum):

    ADD = "Add"
    REMOVE = "Remove"

    def present_participle(self) -> str:
        return self.value.rstrip("e") + "ing"

    def past_participle(self) -> str:
        return self.value.rstrip("e") + "ed"

    def included(self) -> bool:
        return self == TableUpdateAction.ADD


def _get_active_tables(client: BigQueryClient) -> list[str]:
    return sorted(
        [
            row["table_name"]
            for row in bq_query_job_result_to_list_of_row_dicts(
                client.run_query_async(
                    query_str=f"SELECT * FROM `{ACTIVE_TABLES_TO_SYNC_ADDRESS.to_str()}`",
                    use_query_cache=False,
                ).result()
            )
        ]
    )


def _update_tables(
    *,
    table_names: list[str],
    action: TableUpdateAction,
    client: BigQueryClient,
    dry_run: bool,
) -> None:
    """Updates the tables included in the sync, either adding or removing the contents of
    |table_names|, depending on |action|.
    """
    logging.info(
        "%s the following table(s) to the sync:\n%s",
        action.present_participle(),
        "\n".join(f"\t-{table}" for table in table_names),
    )
    if action == TableUpdateAction.ADD:
        prompt_for_confirmation(
            "Have you confirmed that all of the above tables exist and are regularly "
            "updated in Utah's BQ? If you're not sure, please run `fetch-list-of-current-tables-in-utah-bq` "
            "transfer config in `recidiviz-ingest-us-ut` BQ and look at the results in "
            "`metadata.current_tables_in_utah_bq`"
        )

    if not dry_run:
        now = datetime.datetime.now(tz=datetime.UTC)
        client.stream_into_table(
            address=TABLES_TO_SYNC_ADDRESS,
            rows=[
                {
                    "table_name": table_name,
                    "included_in_sync": action.included(),
                    "row_write_datetime": now,
                }
                for table_name in table_names
            ],
        )
        logging.info(
            "Successfully %s [%s] table(s)",
            action.past_participle().lower(),
            len(table_names),
        )
    else:
        logging.info("*Would* %s [%s] table(s)", action.value.lower(), len(table_names))


def main(
    table_ids_to_add: list[str] | None,
    table_ids_to_remove: list[str] | None,
    add_missing_file_configs: bool,
    dry_run: bool,
) -> None:
    """Logic for adding or removing tables from tha Utah BQ sync."""

    bq_client = BigQueryClientImpl()
    active_tables = _get_active_tables(bq_client)

    logging.info("".center(LOGGING_WIDTH, FILL_CHAR))
    logging.info(
        f" ACTIVE TABLES ({len(active_tables)}) ".center(LOGGING_WIDTH, FILL_CHAR)
    )
    logging.info("".center(LOGGING_WIDTH, FILL_CHAR))
    logging.info("%s", "\n".join(f"\t-{table}" for table in active_tables))
    logging.info("".center(LOGGING_WIDTH, FILL_CHAR))

    if table_ids_to_add:
        _update_tables(
            table_names=table_ids_to_add,
            action=TableUpdateAction.ADD,
            client=bq_client,
            dry_run=dry_run,
        )

    if table_ids_to_remove:
        if not_active_tables := set(table_ids_to_remove) - set(active_tables):
            raise ValueError(
                f"Cannot remove the following tables as they are not current active: {not_active_tables}"
            )

        _update_tables(
            table_names=table_ids_to_remove,
            action=TableUpdateAction.REMOVE,
            client=bq_client,
            dry_run=dry_run,
        )

    if add_missing_file_configs:
        region_config = get_region_raw_file_config(StateCode.US_UT.value)
        missing_file_configs = region_config.raw_file_tags - set(active_tables)

        logging.info(
            "Found [%s] file tags not present in sync...", len(missing_file_configs)
        )

        if missing_file_configs:
            _update_tables(
                table_names=list(missing_file_configs),
                action=TableUpdateAction.ADD,
                client=bq_client,
                dry_run=dry_run,
            )


def _create_parser() -> argparse.ArgumentParser:
    """Builds an argument parser for this script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--table-ids-to-add",
        type=str_to_list,
        required=False,
    )

    parser.add_argument(
        "--table-ids-to-remove",
        type=str_to_list,
        required=False,
    )

    parser.add_argument("--add-missing-file-configs", action="store_true")

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = _create_parser().parse_args()

    if (
        sum(
            map(
                bool,
                [
                    args.table_ids_to_add,
                    args.table_ids_to_remove,
                    args.add_missing_file_configs is True,
                ],
            )
        )
        > 1
    ):
        raise ValueError(
            "You can specify at most one of [--table-ids-to-add], [--table-ids-to-remove] and [--add-missing-file-configs]"
        )

    with local_project_id_override(US_UT_INGEST_PROJECT_ID):
        main(
            table_ids_to_add=args.table_ids_to_add,
            table_ids_to_remove=args.table_ids_to_remove,
            add_missing_file_configs=args.add_missing_file_configs,
            dry_run=args.dry_run,
        )
