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
"""This manages backing up the data from the dataflow metrics tables and deleting the
tables themselves ("backup" mode) prior to the deploy, as well as restoring the data
("restore" mode) to the newly created tables after the deploy.

*** THIS SCRIPT SHOULD ONLY BE RUN BY THE ON-CALL ENGINEER AT THE TIME OF THE FIRST DEPLOY AFTER THIS LANDS ***

Backup mode (pre deploy):
python -m recidiviz.big_query.migrations.2021_09_01_backup_restore_clustered_dataflow_tables \
    --project_id [PROJECT_ID] \
    --mode backup \
    --dry_run [DRY_RUN]

Restore mode (post deploy):
python -m recidiviz.big_query.migrations.2021_09_01_backup_restore_clustered_dataflow_tables \
    --project_id [PROJECT_ID] \
    --mode restore \
    --backup_dataset_id dataflow_metrics_backup_2021_XX_XX \
    --dry_run [DRY_RUN]
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.tools.calculator.copy_table_data_from_dataset import (
    copy_table_data_from_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

BACKUP = "backup"
RESTORE = "restore"


def backup(dry_run: bool) -> None:
    """Backs up the dataflow metrics tables and deletes the original dataset."""
    bq_client = BigQueryClientImpl()

    dry_run_prefix = "[DRY RUN] " if dry_run else ""

    logging.info(
        "%sBacking up the dataset [%s].", dry_run_prefix, DATAFLOW_METRICS_DATASET
    )
    if not dry_run:
        backup_dataset = bq_client.backup_dataset_tables_if_dataset_exists(
            DATAFLOW_METRICS_DATASET
        )

        if not backup_dataset:
            logging.error(
                "Dataset [%s] did not exist or could not be backed up.",
                DATAFLOW_METRICS_DATASET,
            )
            sys.exit(1)

        logging.info("Dataset backed up to [%s].", backup_dataset.dataset_id)

        i = input(
            f"Confirm that all expected tables exist in the backup dataset [{backup_dataset.dataset_id}].\n"
            "Type 'confirm' to proceed and delete the original dataset.\n"
        )
        if i != "confirm":
            sys.exit(1)

    logging.info(
        "%sDeleting the dataset [%s].", dry_run_prefix, DATAFLOW_METRICS_DATASET
    )
    if not dry_run:
        original_dataset = bq_client.dataset_ref_for_id(DATAFLOW_METRICS_DATASET)
        bq_client.delete_dataset(original_dataset, delete_contents=True)


def restore(backup_dataset_id: str, dry_run: bool) -> None:
    dry_run_prefix = "[DRY RUN] " if dry_run else ""

    logging.info(
        "%sCopying data from backup [%s] to [%s].",
        dry_run_prefix,
        backup_dataset_id,
        DATAFLOW_METRICS_DATASET,
    )
    if not dry_run:
        copy_table_data_from_dataset(backup_dataset_id, DATAFLOW_METRICS_DATASET)

        i = input(
            f"Confirm that all data from [{backup_dataset_id}] now exists in [{DATAFLOW_METRICS_DATASET}].\n"
            "Type 'confirm' to proceed and delete the backup.\n"
        )
        if i != "confirm":
            sys.exit(1)

    logging.info("%sDeleting the dataset [%s].", dry_run_prefix, backup_dataset_id)
    if not dry_run:
        bq_client = BigQueryClientImpl()
        backup_dataset_ref = bq_client.dataset_ref_for_id(backup_dataset_id)
        bq_client.delete_dataset(backup_dataset_ref, delete_contents=True)


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

    parser.add_argument("--dry_run", type=str_to_bool, required=True)
    parser.add_argument("--mode", choices=[BACKUP, RESTORE], required=True)
    parser.add_argument("--backup_dataset_id")

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    continue_running = input(
        "This script should only be run by the on-call engineer doing a deploy.\n"
        f"Are you the on-call engineer running a deploy for [{known_args.project_id}]? (y/N) "
    )
    if continue_running.upper() != "Y":
        sys.exit(1)

    with local_project_id_override(known_args.project_id):
        if known_args.mode == BACKUP:
            backup(known_args.dry_run)
        elif known_args.mode == RESTORE:
            if known_args.backup_dataset_id is None:
                logging.error("Backup dataset required in 'restore' mode.")
                sys.exit(1)
            restore(known_args.backup_dataset_id, known_args.dry_run)
        else:
            logging.error("Unknown mode: %s", known_args.mode)
