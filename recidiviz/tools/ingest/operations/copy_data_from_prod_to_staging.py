"""
Script to copy raw state data files from production to staging.

This script automates the process of copying raw data files from the production
environment to staging for testing and development purposes. It performs two main
operations: copying files from prod storage to staging storage, then moving them
to the staging ingest bucket.

Key Features:
- Automatically determines the last stored date for each state in staging
- Prompts user to confirm or override start dates for each state
- Warns about states with low file counts that may need date overrides
- Supports dry-run mode for safe testing (enabled by default)

Basic Usage:
    # Dry run (default) - shows what would happen without making changes
    python copy_data_from_prod_to_staging.py --states US_ND US_TN

    # Actual execution - copies data for real
    python copy_data_from_prod_to_staging.py --states US_ND US_TN --dry-run False
"""

import argparse
import datetime
import logging
from typing import Dict, TypedDict, cast

from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_date
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.operations.copy_raw_state_files_between_projects import (
    copy_raw_state_files,
)
from recidiviz.tools.ingest.operations.get_last_stored_date_for_states import (
    get_latest_date_folders,
)
from recidiviz.tools.ingest.operations.helpers.operate_on_raw_storage_directories_controller import (
    IngestFilesOperationType,
)
from recidiviz.tools.ingest.operations.move_raw_state_files_from_storage import (
    OperateOnRawStorageFilesController,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

MIN_NUM_FILES = 5


class StateData(TypedDict):
    date: str
    file_count: int
    path: str


def increment_date(date_str: str) -> str:
    # Assumes date_str is YYYY/MM/DD
    date_obj = datetime.datetime.strptime(date_str, "%Y/%m/%d")
    next_day = date_obj + datetime.timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def validate_date_format(date_str: str) -> bool:
    """Validate that date string is in YYYY-MM-DD format and is a valid date."""
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def confirm_dates_with_user(
    dates_by_state: Dict[StateCode, StateData],
) -> Dict[StateCode, str]:
    """
    Prompt user to confirm or override start dates for each state.
    Returns dictionary of confirmed dates in YYYY-MM-DD format.
    """
    confirmed_dates = {}

    print("\n" + "=" * 50)
    print("DATE CONFIRMATION")
    print("=" * 50)
    print("Please review and confirm the start dates for each state.")
    print(
        "Press Enter to use the default (last stored date + 1 day), or enter a custom date in YYYY-MM-DD format."
    )
    print()

    for state, state_data in dates_by_state.items():
        last_date = state_data["date"]
        file_count = state_data["file_count"]
        default_start_date = increment_date(last_date)

        print(f"State: {state.value}")
        print(f"  Last stored date: {last_date}")
        print(f"  Files in last upload: {file_count}")
        print(f"  Default start date: {default_start_date}")

        # Add warning for low file counts
        if file_count <= MIN_NUM_FILES:
            print(
                f"  ⚠️  WARNING: Only {file_count} files found in last upload. This may\n"
                "      indicate that someone only transfered a few files from prod to\n"
                "      staging on this date rather than all the files, which means many files\n"
                "      may be missing from this date. Unless we expect this state to only\n"
                f"      usually receive {file_count} files, you may want to override this date.\n"
            )

        while True:
            prompt = f"  Start date for {state.value}"
            if file_count <= 5:
                prompt += " (⚠️ LOW FILE COUNT - consider override)"
            prompt += f" (default: {default_start_date}): "

            user_input = input(prompt).strip()

            if not user_input:
                # Use default
                confirmed_dates[state] = default_start_date
                print(f"  ✓ Using default: {default_start_date}")
                break
            # Validate user input
            if validate_date_format(user_input):
                confirmed_dates[state] = user_input
                print(f"  ✓ Using custom date: {user_input}")
                break
            print(
                "  ✗ Invalid date format. Please use YYYY-MM-DD format (e.g., 2025-01-15)"
            )

        print()

    print("=" * 50)
    print("CONFIRMED START DATES:")
    for state, date in confirmed_dates.items():
        print(f"  {state.value}: {date}")
    print("=" * 50)
    print()

    return confirmed_dates


def main() -> None:
    """Main function to copy raw state data files from production to staging."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Copy and move raw state files between projects."
    )
    parser.add_argument(
        "--states",
        nargs="+",
        required=True,
        type=StateCode,
        help="List of states, e.g. US_ND US_TN US_MI",
    )
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Set to True for dry run, False for actual transfer (default: True).",
    )
    parser.add_argument(
        "--skip-confirmation",
        default=False,
        type=str_to_bool,
        help="Skip confirmation prompts for each state (default: False).",
    )
    args = parser.parse_args()

    states = args.states
    dry_run_flag = args.dry_run
    skip_confirmation_flag = args.skip_confirmation
    source_project_id = GCP_PROJECT_PRODUCTION
    destination_project_id = GCP_PROJECT_STAGING
    source_instance = DirectIngestInstance.PRIMARY
    destination_instance = DirectIngestInstance.PRIMARY

    if dry_run_flag:
        print(
            "Note: This is a dry-run!!! This will do nothing! If you actually want to do any moving, execute this command with `--dry-run False`"
        )

    # Step 1: Get last stored dates for states
    print("\nStep 1: Getting last stored dates for states...")
    dates_by_state: Dict[StateCode, StateData] = cast(
        Dict[StateCode, StateData],
        get_latest_date_folders(states, destination_project_id, destination_instance),
    )

    # Get user confirmation for start dates
    confirmed_dates = confirm_dates_with_user(dates_by_state)

    for state, start_date in confirmed_dates.items():
        region = state.value.lower()

        # Step 2: Copy files from prod to staging
        print(f"\nStep 2: Copying files for {state.value}...")
        copy_raw_state_files(
            region=region,
            source_project_id=source_project_id,
            source_raw_data_instance=source_instance,
            destination_project_id=destination_project_id,
            destination_raw_data_instance=destination_instance,
            start_date_bound=start_date,
            dry_run=dry_run_flag,
            skip_confirmation=skip_confirmation_flag,
        )

        # Step 3: Move files from staging storage to ingest bucket
        print(f"\nStep 3: Moving files for {state.value}...")
        with local_project_id_override(destination_project_id):
            with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
                OperateOnRawStorageFilesController(
                    source_project_id=destination_project_id,
                    destination_project_id=destination_project_id,
                    source_raw_data_instance=destination_instance,
                    destination_raw_data_instance=destination_instance,
                    region=region,
                    start_date_bound=parse_date(start_date),
                    end_date_bound=None,
                    dry_run=dry_run_flag,
                    file_filter=None,
                    operation_type=IngestFilesOperationType.MOVE,
                    skip_confirmation=skip_confirmation_flag,
                ).run()


if __name__ == "__main__":
    main()
