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
Development/testing utility that plants a deliberately bad watermark in
direct_ingest_dataflow_raw_table_upper_bounds so you can test check_watermarks.py.
Only works for recidiviz-staging.

Supports two modes (--mode):

   stale (default): inserts a watermark far in the future for an existing file tag,
   causing check_watermarks.py to report that tag as STALE.

   missing: inserts a watermark for a fake file tag that has no rows in
   direct_ingest_raw_big_query_file_metadata, causing check_watermarks.py to report
   that tag as MISSING.

In both modes the fake job_id sorts higher than any real job so it becomes the
"latest" job.

Example Usage:

    python -m recidiviz.tools.ingest.operations.plant_test_watermark \
       --state-code=US_OZ

    python -m recidiviz.tools.ingest.operations.plant_test_watermark \
       --state-code=US_OZ \
       --mode=missing

    python -m recidiviz.tools.ingest.operations.plant_test_watermark \
       --state-code=US_OZ \
       --file-tag=test

Clean up afterward with:

    python -m recidiviz.tools.ingest.operations.check_watermarks \
       --project-id=recidiviz-staging \
       --state-code=<state-code> \
       --fix
"""

import argparse
import datetime
import logging
import sys

from sqlalchemy import text

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.ingest.operations.check_watermarks import get_watermarks
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Sorts lexicographically higher than any real job_id, which are formatted
# as YYYY-MM-DD_HH_MM_SS-NNNNNNNNNNNNNNNNNNN.
_FAKE_JOB_ID = "9999-12-31_00_00_00-0000000000000000000"


_FUTURE_WATERMARK = datetime.datetime(2099, 1, 1)
_FAKE_MISSING_TAG = "ZZZZ_FAKE_MISSING_TAG"
# A realistic-looking past watermark for the missing case.
_PAST_WATERMARK = datetime.datetime(2024, 1, 1)


def main(state_code: StateCode, mode: str, file_tag: str | None) -> None:
    """Plants a deliberately bad watermark in staging for testing check_watermarks.py."""
    project_id = GCP_PROJECT_STAGING
    region_code = state_code.value
    ingest_instance = DirectIngestInstance.PRIMARY.value

    with local_project_id_override(project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

            if mode == "stale":
                # For stale, we need an existing tag so Big Query metadata exists
                # for it.
                if file_tag is None:
                    with SessionFactory.for_proxy(db_key) as session:
                        existing, _ = get_watermarks(
                            session, region_code, ingest_instance
                        )
                    if not existing:
                        print(
                            f"No existing watermarks found for {region_code}. "
                            f"Cannot pick a file tag automatically — pass --file-tag explicitly."
                        )
                        sys.exit(1)
                    file_tag = sorted(existing.keys())[0]
                    print(f"No --file-tag specified, using first tag: {file_tag}")
                watermark_datetime = _FUTURE_WATERMARK
            else:
                # For missing, the tag must not exist in Big Query metadata.
                file_tag = file_tag or _FAKE_MISSING_TAG
                watermark_datetime = _PAST_WATERMARK

            with SessionFactory.for_proxy(db_key, autocommit=False) as session:
                # Delete any existing fake data so re-runs are safe.
                session.execute(
                    text(
                        "DELETE FROM direct_ingest_dataflow_raw_table_upper_bounds"
                        " WHERE job_id = :job_id"
                    ),
                    {"job_id": _FAKE_JOB_ID},
                )
                session.execute(
                    text(
                        "DELETE FROM direct_ingest_dataflow_job WHERE job_id = :job_id"
                    ),
                    {"job_id": _FAKE_JOB_ID},
                )
                session.execute(
                    text(
                        "INSERT INTO direct_ingest_dataflow_job"
                        " (job_id, region_code, ingest_instance, completion_time, is_invalidated)"
                        " VALUES (:job_id, :region_code, :ingest_instance, NOW(), FALSE)"
                    ),
                    {
                        "job_id": _FAKE_JOB_ID,
                        "region_code": region_code.upper(),
                        "ingest_instance": ingest_instance.upper(),
                    },
                )
                session.execute(
                    text(
                        "INSERT INTO direct_ingest_dataflow_raw_table_upper_bounds"
                        " (region_code, job_id, raw_data_file_tag, watermark_datetime)"
                        " VALUES (:region_code, :job_id, :file_tag, :watermark_datetime)"
                    ),
                    {
                        "region_code": region_code.upper(),
                        "job_id": _FAKE_JOB_ID,
                        "file_tag": file_tag,
                        "watermark_datetime": watermark_datetime,
                    },
                )
                session.commit()

    expected_error = "STALE" if mode == "stale" else "MISSING"
    print(
        f"\nPlanted {mode} watermark for [{region_code}] tag [{file_tag}]:\n"
        f"  job_id:             {_FAKE_JOB_ID}\n"
        f"  watermark_datetime: {watermark_datetime}\n"
        f"\nRun check_watermarks.py to see the {expected_error} error:\n"
        f"  python -m recidiviz.tools.ingest.operations.check_watermarks \\\n"
        f"      --project-id={project_id} --state-code={region_code}\n"
        f"\nClean up with --fix:\n"
        f"  python -m recidiviz.tools.ingest.operations.check_watermarks \\\n"
        f"      --project-id={project_id} --state-code={region_code} --fix"
    )


def create_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        required=True,
        type=StateCode,
        metavar="STATE_CODE",
        help="state code to plant the bad watermark for (e.g. US_OZ)",
    )
    parser.add_argument(
        "--mode",
        default="stale",
        choices=["stale", "missing"],
        help=(
            "stale (default) plants a future watermark for an existing tag, missing "
            "plants a watermark for a fake tag with no BQ metadata rows"
        ),
    )
    parser.add_argument(
        "--file-tag",
        default=None,
        help=(
            "file tag to plant the bad watermark for (defaults to the first tag from "
            "the state's existing watermarks or ZZZZ_FAKE_MISSING_TAG in missing mode"
        ),
    )
    return parser


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    args = create_parser().parse_args()
    main(args.state_code, args.mode, args.file_tag)
