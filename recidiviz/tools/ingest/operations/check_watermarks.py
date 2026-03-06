# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
Local script that replicates the watermark-checking logic from the ingest calculation
pipeline (get_max_update_datetimes -> get_watermarks -> check_for_valid_watermarks)
for a given state. Useful for diagnosing watermark errors and verifying that corrective
actions (e.g. deleting rows from direct_ingest_dataflow_raw_table_upper_bounds) will
have the intended effect before the next pipeline run.

By default, prints PASS or a description of the failure, mirroring what the pipeline
would do. On failure, also prints the SQL that would fix the issue and prompts to apply
it if --fix is set.

Use --list to print a table of each file tag with its watermark datetime (last pipeline
run) and current max update_datetime. Useful when deprecating raw data or auditing
update_datetime values.

Use --fix to apply corrective deletes from direct_ingest_dataflow_raw_table_upper_bounds
after confirmation. Cannot be combined with --list.

Example Usage:
    python -m recidiviz.tools.ingest.operations.check_watermarks \
        --project-id=recidiviz-staging --state-code=US_NE

    python -m recidiviz.tools.ingest.operations.check_watermarks \
        --project-id=recidiviz-staging --state-code=US_NE --list

    python -m recidiviz.tools.ingest.operations.check_watermarks \
        --project-id=recidiviz-staging --state-code=US_NE --fix
"""

import argparse
import datetime
import logging
import sys

import sqlalchemy.orm
from sqlalchemy import bindparam, text
from tabulate import tabulate

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.watermark_utils import (
    MAX_UPDATE_DATETIMES_QUERY_TEMPLATE,
    WATERMARKS_QUERY_TEMPLATE,
    get_problematic_watermark_tags,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

_PROBLEMATIC_STATUSES = frozenset({"MISSING", "STALE"})


def _get_max_update_datetimes(
    session: sqlalchemy.orm.Session,
    region_code: str,
    ingest_instance: str,
) -> dict[str, datetime.datetime]:
    query_str = StrictStringFormatter().format(
        MAX_UPDATE_DATETIMES_QUERY_TEMPLATE,
        region_code=region_code.upper(),
        raw_data_instance=ingest_instance.upper(),
    )
    results = session.execute(text(query_str))
    return {row.file_tag: row.max_update_datetime for row in results}


def get_watermarks(
    session: sqlalchemy.orm.Session,
    region_code: str,
    ingest_instance: str,
) -> tuple[dict[str, datetime.datetime], str | None]:
    """Returns (watermarks_by_tag, job_id) for the latest non-invalidated job."""
    query_str = StrictStringFormatter().format(
        WATERMARKS_QUERY_TEMPLATE,
        region_code=region_code.upper(),
        ingest_instance=ingest_instance.upper(),
    )
    rows = list(session.execute(text(query_str)))
    if not rows:
        return {}, None
    job_id: str = rows[0].job_id
    return {row.raw_data_file_tag: row.watermark_datetime for row in rows}, job_id


def _get_data_needed_for_checking(
    db_key: SQLAlchemyDatabaseKey, region_code: str, ingest_instance: str
) -> tuple[dict[str, datetime.datetime], dict[str, datetime.datetime], str | None]:
    with SessionFactory.for_proxy(db_key) as session:
        watermarks, job_id = get_watermarks(session, region_code, ingest_instance)
        max_update_datetimes = _get_max_update_datetimes(
            session, region_code, ingest_instance
        )
    return watermarks, max_update_datetimes, job_id


def _build_tag_rows(
    watermarks: dict[str, datetime.datetime],
    max_update_datetimes: dict[str, datetime.datetime],
) -> list[tuple[str, datetime.datetime | None, datetime.datetime | None, str]]:
    """Returns (tag, watermark, current_max, status) rows, problematic tags first."""
    missing, stale = get_problematic_watermark_tags(watermarks, max_update_datetimes)
    missing_set = set(missing)
    stale_set = set(stale)
    rows: list[tuple[str, datetime.datetime | None, datetime.datetime | None, str]] = []
    for tag in sorted(set(watermarks) | set(max_update_datetimes)):
        watermark = watermarks.get(tag)
        current = max_update_datetimes.get(tag)
        if tag in missing_set:
            status = "MISSING"
        elif tag in stale_set:
            status = "STALE"
        elif watermark is None:
            status = "NO_WATERMARK"
        else:
            status = "OK"
        rows.append((tag, watermark, current, status))
    rows.sort(key=lambda r: (0 if r[3] in _PROBLEMATIC_STATUSES else 1, r[0]))
    return rows


def _get_fix_watermark_query() -> sqlalchemy.sql.expression.TextClause:
    """Returns the parameterized DELETE query used to remove problematic watermarks."""
    return text(
        "DELETE FROM direct_ingest_dataflow_raw_table_upper_bounds"
        " WHERE job_id = :job_id AND raw_data_file_tag IN :tags"
    ).bindparams(bindparam("tags", expanding=True))


def _print_check_results(
    rows: list[tuple[str, datetime.datetime | None, datetime.datetime | None, str]],
    num_watermarks: int,
    job_id: str,
    list_mode: bool,
) -> None:
    """Prints a status table and summary. In list mode shows all tags; otherwise only
    problematic ones. Problematic tags are always sorted to the top of the table."""
    display_rows = (
        rows if list_mode else [r for r in rows if r[3] in _PROBLEMATIC_STATUSES]
    )
    if display_rows:
        print(
            "\n"
            + tabulate(
                display_rows,
                headers=[
                    "FILE_TAG",
                    "WATERMARK",
                    "CURRENT_MAX_UPDATE_DATETIME",
                    "STATUS",
                ],
            )
            + "\n"
        )

    tags_to_fix = [r[0] for r in rows if r[3] in _PROBLEMATIC_STATUSES]
    if not tags_to_fix:
        print(
            f"\nPASS: All {num_watermarks} watermarks are valid "
            f"(current data is at least as new as the last pipeline run).\n"
        )
        return

    print(f"\nFAIL: {len(tags_to_fix)} of {num_watermarks} watermark(s) have issues.\n")
    if not list_mode:
        fix_query = _get_fix_watermark_query()
        print(
            f"\nTo remove these watermarks and allow the pipeline to proceed, run:\n\n"
            f"  {fix_query.text}\n\n"
            f"  with job_id='{job_id}', tags={tags_to_fix}\n"
            f"\nOr re-run this script with --fix to apply interactively.\n"
        )


def check_watermarks(
    db_key: SQLAlchemyDatabaseKey,
    region_code: str,
    ingest_instance: str,
    list_mode: bool,
) -> tuple[list[str], str]:
    """Checks watermarks for a state. Returns (tags_to_fix, job_id)."""

    watermarks, max_update_datetimes, job_id = _get_data_needed_for_checking(
        db_key, region_code, ingest_instance
    )

    if not watermarks:
        raise ValueError(f"No watermarks found for {region_code}.")

    assert job_id is not None

    rows = _build_tag_rows(watermarks, max_update_datetimes)
    _print_check_results(rows, len(watermarks), job_id, list_mode)

    if list_mode:
        return [], job_id

    return sorted(r[0] for r in rows if r[3] in _PROBLEMATIC_STATUSES), job_id


def main(
    project_id: str,
    state_code: StateCode,
    list_mode: bool,
    fix: bool,
) -> None:
    """Entry point for the watermark check script."""

    ingest_instance = DirectIngestInstance.PRIMARY.value
    region_code = state_code.value

    with local_project_id_override(project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

            tags_to_fix, job_id = check_watermarks(
                db_key, region_code, ingest_instance, list_mode
            )

            if not fix or not tags_to_fix:
                return

            fix_query = _get_fix_watermark_query()
            prompt_for_confirmation(
                f"\nAbout to delete watermark entries for {len(tags_to_fix)} tag(s) "
                f"from job {job_id}:\n  {tags_to_fix}\n"
                f"\nSQL:\n  {fix_query.text}\n"
                f"  with job_id='{job_id}', tags={tags_to_fix}\n\nProceed?"
            )

            with SessionFactory.for_proxy(db_key, autocommit=False) as session:
                result = session.execute(
                    fix_query,
                    {"job_id": job_id, "tags": tags_to_fix},
                )
                session.commit()

            print(f"\nDeleted {result.rowcount} row(s). Re-running check...")

            tags_remaining, _ = check_watermarks(
                db_key, region_code, ingest_instance, list_mode=False
            )

            if tags_remaining:
                sys.exit(1)


def create_parser() -> argparse.ArgumentParser:
    """Builds the argument parser for the watermark check script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        required=True,
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="the project to pull data from, e.g. 'recidiviz-staging'",
    )
    parser.add_argument(
        "--state-code",
        required=True,
        type=StateCode,
        metavar="STATE_CODE",
        help="state code to check (e.g. US_NE)",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--list",
        default=False,
        action="store_true",
        help=(
            "prints every file tag with its watermark and current max update_datetime instead "
            "of just checking for bad watermarks (cannot be combined with --fix)"
        ),
    )
    group.add_argument(
        "--fix",
        default=False,
        action="store_true",
        help=(
            "when the check fails, prompt to delete the offending watermark rows from "
            "direct_ingest_dataflow_raw_table_upper_bounds (cannot be combined with --list)"
        ),
    )
    return parser


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    args = create_parser().parse_args()

    main(args.project_id, args.state_code, args.list, args.fix)
