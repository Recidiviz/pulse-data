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
Supports UT CPA enablement by inserting intake records into the
recidiviz-prod Cloud SQL database in the recidiviz-rnd-planner GCP project.

Fetches eligible clients directly from BigQuery based on supervision period
criteria (CCC/CTC locations, started within the last 7 days, not in custody).

Also reads from the UT release list Google Sheet (requires --sheet-gid).
Offender numbers are looked up in BigQuery to resolve pseudo IDs.
All pseudo IDs are combined into a single INSERT.

python -m recidiviz.tools.cpa.ut_enablement --sheet-gid 1026617810

python -m recidiviz.tools.cpa.ut_enablement --sheet-gid 1026617810 --dry-run false
"""
import argparse
import csv
import io
import logging
import sys

import requests
from google.cloud.secretmanager_v1 import SecretManagerServiceClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.tools.postgres.cloudsql_proxy_control import CloudSQLProxyControl
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

GCP_PROJECT_ID = "recidiviz-rnd-planner"
DATABASE_CONNECTION_NAME = "recidiviz-rnd-planner:us-central1:recidiviz-prod"
DATABASE_PASSWORD_SECRET_NAME = "RECIDIVIZ_POSTGRES_PASSWORD_PROD"  # nosec B105
DEFAULT_DATABASE_NAME = "recidiviz"
DEFAULT_DATABASE_USER = "postgres"
PROXY_PORT = 5441
PROXY_HOST = "127.0.0.1"

BQ_PROJECT = "recidiviz-123"
CLIENT_BQ_TABLE = "recidiviz-123.reentry.client_materialized"
ASSESSMENT_CONFIG_ID = "e6112fb0-2043-46c1-b91f-1791f1ffcc9d"

# UT Release List Google Sheet
RELEASE_SHEET_ID = "1KhTh_VOVG4u6g6L2kMguD4-3te2CEB-xBr0fQ6Wh6V0"
EXCLUDED_SECTIONS = {"DISCHARGE", "DETAINER"}

ELIGIBLE_CLIENTS_QUERY = """
SELECT DISTINCT client.pseudonymized_id AS client_pseudonymized_id
FROM
  `recidiviz-123.normalized_state.state_supervision_period` supervision_period
LEFT JOIN
  `recidiviz-123.normalized_state.state_staff` staff
ON
  supervision_period.state_code = staff.state_code
  AND supervision_period.supervising_officer_staff_id = staff.staff_id
LEFT JOIN
  `recidiviz-123.reentry.client_materialized` client
ON
  supervision_period.person_id = client.person_id
WHERE
  supervision_period.state_code = 'US_UT'
  AND (supervision_site LIKE "%CCC%"
    OR supervision_site LIKE "%CTC%"
    OR supervision_site = 'FORTITUDE TREATMENT CNTR')
  AND (json_extract_scalar(supervision_period_metadata, '$.Location') LIKE "%CCC%"
    OR json_extract_scalar(supervision_period_metadata, '$.Location') LIKE "%CTC%"
    OR json_extract_scalar(supervision_period_metadata, '$.Location') = 'FORTITUDE TC'
    OR json_extract_scalar(supervision_period_metadata, '$.Location') = ""
    OR json_extract_scalar(supervision_period_metadata, '$.Location') IS NULL)
  AND termination_date IS NULL
  AND start_date BETWEEN DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 7 DAY)
    AND CURRENT_DATE('US/Eastern')
  AND supervision_level != 'IN_CUSTODY'
LIMIT 1000
"""

logger = logging.getLogger(__name__)

proxy = CloudSQLProxyControl(port=PROXY_PORT)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sheet-gid",
        type=str,
        required=True,
        help="The gid of the release list sheet tab (from URL gid=XXXXX).",
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def fetch_eligible_pseudo_ids_from_bq() -> list[str]:
    """Fetches eligible client pseudo IDs from BigQuery."""
    with local_project_id_override(BQ_PROJECT):
        bq_client = BigQueryClientImpl()
        query_job = bq_client.run_query_async(
            query_str=ELIGIBLE_CLIENTS_QUERY, use_query_cache=True
        )
        return [row["client_pseudonymized_id"] for row in query_job]


def read_offender_numbers_from_sheet(sheet_gid: str) -> list[str]:
    """Reads qualifying OFFENDER # values from the release list Google Sheet.

    Uses the CSV export URL to fetch data without OAuth (requires sheet to be
    shared with "anyone with the link can view").

    Skips rows in DISCHARGE or DETAINER sections, and excludes rows where
    RELEASE TO starts with REG or NUR.
    """
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{RELEASE_SHEET_ID}"
        f"/export?format=csv&gid={sheet_gid}"
    )

    response = requests.get(export_url, timeout=30)
    response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))

    offender_numbers = []
    in_excluded_section = False

    for row in reader:
        offender_num = row.get("OFFENDER #", "").strip()

        # Track when we enter/exit excluded sections
        if offender_num in EXCLUDED_SECTIONS:
            in_excluded_section = True
            continue

        # A new non-excluded section header resets the flag
        if offender_num and not offender_num.isdigit():
            in_excluded_section = False
            continue

        if in_excluded_section:
            continue

        # Skip empty rows
        if not offender_num:
            continue

        # Exclude clients released to REG or NUR
        release_to = row.get("RELEASE TO", "").strip().upper()
        if release_to.startswith("REG") or release_to.startswith("NUR"):
            continue

        offender_numbers.append(offender_num)

    return offender_numbers


def lookup_pseudo_ids_from_bq(offender_numbers: list[str]) -> list[str]:
    """Looks up pseudonymized_id values in BigQuery for the given offender numbers."""
    in_clause = ", ".join(f"'{num}'" for num in offender_numbers)
    bq_query = (
        f"SELECT pseudonymized_id FROM `{CLIENT_BQ_TABLE}` "
        f"WHERE state_code = 'US_UT' AND external_id IN ({in_clause})"
    )

    with local_project_id_override(BQ_PROJECT):
        bq_client = BigQueryClientImpl()
        query_job = bq_client.run_query_async(query_str=bq_query, use_query_cache=True)
        return [row["pseudonymized_id"] for row in query_job]


def collect_pseudo_ids(sheet_gid: str) -> list[str]:
    """Collects pseudo IDs from BigQuery and the release list Google Sheet.

    First fetches eligible clients from BigQuery based on supervision period
    criteria. Then reads the release list Google Sheet to add additional clients.
    """
    all_pseudo_ids: list[str] = []

    # Fetch eligible clients from BigQuery
    print("Fetching eligible clients from BigQuery...")
    bq_pseudo_ids = fetch_eligible_pseudo_ids_from_bq()
    print(f"Found {len(bq_pseudo_ids)} eligible client(s) from BQ.")
    all_pseudo_ids.extend(bq_pseudo_ids)

    # Read offender numbers from Google Sheet
    print(f"Reading release list from Google Sheet (gid={sheet_gid})...")
    offender_numbers = read_offender_numbers_from_sheet(sheet_gid)
    print(f"Found {len(offender_numbers)} qualifying offender numbers from sheet.")
    if offender_numbers:
        sheet_pseudo_ids = lookup_pseudo_ids_from_bq(offender_numbers)
        print(f"Resolved {len(sheet_pseudo_ids)} pseudo IDs from BQ.")
        all_pseudo_ids.extend(sheet_pseudo_ids)

    return all_pseudo_ids


def get_db_password() -> str:
    """Fetches the database password from Secret Manager."""
    client = SecretManagerServiceClient()
    secret_name = f"projects/{GCP_PROJECT_ID}/secrets/{DATABASE_PASSWORD_SECRET_NAME}/versions/latest"
    response = client.access_secret_version(name=secret_name)
    return response.payload.data.decode("UTF-8")


def build_query(all_pseudo_ids: list[str]) -> str:
    """Builds the INSERT query for the given pseudo IDs."""
    values = ",\n    ".join(f"('{pid}')" for pid in all_pseudo_ids)
    return f"""
INSERT INTO "public"."intake" (
  id,
  created_at,
  updated_at,
  client_id,
  status,
  current_section,
  internal_access,
  intake_type,
  client_pseudo_id,
  assessment_config_id
)
SELECT
  gen_random_uuid(),
  NOW(),
  NOW(),
  NULL,
  'created',
  NULL,
  TRUE,
  'conversation',
  v.pseudo_id,
  '{ASSESSMENT_CONFIG_ID}'
FROM (
  VALUES
    {values}
) AS v(pseudo_id)
WHERE NOT EXISTS (
  SELECT 1
  FROM "public"."intake" i
  WHERE i.client_pseudo_id = v.pseudo_id
    AND i.intake_type = 'conversation'
)
RETURNING *;
"""


def run_query(query: str, db_password: str) -> None:
    """Connects to the database through the proxy and runs the given query."""
    url = URL.create(
        drivername="postgresql",
        username=DEFAULT_DATABASE_USER,
        password=db_password,
        host=PROXY_HOST,
        port=PROXY_PORT,
        database=DEFAULT_DATABASE_NAME,
    )
    engine = create_engine(url)

    with Session(bind=engine) as session:
        result = session.execute(text(query))
        rows = result.fetchall()
        print(f"Inserted {len(rows)} row(s).")
        session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()

    pseudo_ids = collect_pseudo_ids(args.sheet_gid)

    if not pseudo_ids:
        print("No eligible clients found.")
        sys.exit(1)

    print(f"\nTotal: {len(pseudo_ids)} pseudo IDs collected.")
    built_query = build_query(pseudo_ids)

    if args.dry_run:
        print("[DRY RUN] Query that would be executed:")
        print(built_query)
    else:
        with proxy.connection_for_instance(connection_string=DATABASE_CONNECTION_NAME):
            run_query(query=built_query, db_password=get_db_password())
