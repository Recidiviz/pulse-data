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
Queries CPA survey results from the recidiviz-prod Cloud SQL database
and augments with client data from BigQuery. Writes results to a Google Sheet.

python -m recidiviz.tools.cpa.query_survey_results
"""
import json
import logging
from datetime import datetime
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.tools.cpa.utils import (
    BQ_PROJECT,
    CLIENT_BQ_TABLE,
    DATABASE_CONNECTION_STRING_SECRET_NAME,
    DATABASE_PASSWORD_SECRET_NAME,
    DATABASE_USERNAME_SECRET_NAME,
    PROXY_PORT,
    SERVICE_ACCOUNT_KEY_SECRET_NAME,
    create_db_engine,
    get_secret,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import CloudSQLProxyControl
from recidiviz.utils.metadata import local_project_id_override

# https://docs.google.com/spreadsheets/d/14DiK_Wb5GB2fCf9A6zdD0DNI5mMQgOpwS4aEuf5XsqM/
OUTPUT_SHEET_ID = "14DiK_Wb5GB2fCf9A6zdD0DNI5mMQgOpwS4aEuf5XsqM"

# default to not including the full action plan markdown
INCLUDE_MARKDOWN_RESULT = False

# source: https://www.notion.so/recidiviz/How-to-pull-intake-artifacts-from-DB-2727889f4d198083aa3bcdd60590783c
SURVEY_RESULTS_QUERY = f"""
SELECT
  plan.client_id,
  plan.client_pseudo_id,
  assessment.assessment_type,
  plan.created_at,
  plan.updated_at,
  plan.edited_manually,
  {"plangeneration.markdown_result," if INCLUDE_MARKDOWN_RESULT else ""}
  assessment.scores,
  planasset.file_blob AS intake_conversation_summary,
  client_address.state,
  intake_survey.difficulty_rating,
  intake_survey.questions_confusing,
  intake_survey.preferred_method,
  intake_survey.method_other,
  intake_survey.additional_feedback
FROM
  plan
LEFT JOIN
  plangeneration
ON
  plangeneration.plan_id = plan.id
LEFT JOIN
  planasset
ON
  planasset.plan_id = plan.id
  AND planasset.filename = 'summary.md'
LEFT JOIN
  assessment
ON
  assessment.client_pseudo_id = plan.client_pseudo_id
LEFT JOIN
  intake
ON
  intake.client_pseudo_id = plan.client_pseudo_id
LEFT JOIN
  client_address
ON
  client_address.intake_id = intake.id
LEFT JOIN
  intake_survey
ON
  intake_survey.intake_id = intake.id
"""

logger = logging.getLogger(__name__)

proxy = CloudSQLProxyControl(port=PROXY_PORT)


def fetch_survey_results(engine: Engine) -> list[dict]:
    """Fetches survey results from the PostgreSQL database."""
    with Session(bind=engine) as session:
        result = session.execute(text(SURVEY_RESULTS_QUERY))
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


def fetch_client_data_from_bq(
    pseudo_ids: list[str],
) -> dict[str, dict[str, str]]:
    """Fetches client data from BigQuery for the given pseudo IDs.

    Returns a dict mapping pseudo_id -> {"location": ..., "state_code": ...}.
    """
    if not pseudo_ids:
        return {}

    in_clause = ", ".join(f"'{pid}'" for pid in pseudo_ids)
    bq_query = f"""
        SELECT pseudonymized_id, location, state_code
        FROM `{CLIENT_BQ_TABLE}`
        WHERE pseudonymized_id IN ({in_clause})
    """

    with local_project_id_override(BQ_PROJECT):
        bq_client = BigQueryClientImpl()
        query_job = bq_client.run_query_async(query_str=bq_query, use_query_cache=True)
        return {
            row["pseudonymized_id"]: {
                "location": row["location"],
                "state_code": row["state_code"],
            }
            for row in query_job
        }


def get_sheets_service() -> Resource:
    """Creates a Google Sheets API service using service account credentials
    fetched from Secret Manager."""
    sa_key_json = json.loads(get_secret(SERVICE_ACCOUNT_KEY_SECRET_NAME))
    credentials = service_account.Credentials.from_service_account_info(
        sa_key_json, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=credentials)


def convert_value_for_sheet(value: Any) -> str:
    """Converts a value to a string suitable for Google Sheets."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value)


def write_results_to_new_tab(sheets_service: Resource, results: list[dict]) -> None:
    """Creates a new tab in the output sheet and writes results to it."""
    if not results:
        print("No results to write.")
        return

    tab_title = f"{datetime.now().strftime('%m-%d-%Y')}"

    # Add a new sheet (tab) to the existing spreadsheet
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=OUTPUT_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_title}}}]},
    ).execute()

    # Get column headers from first result
    headers = list(results[0].keys())

    # Convert all values to strings
    rows = [headers]
    for result in results:
        row = [convert_value_for_sheet(result.get(col)) for col in headers]
        rows.append(row)

    sheets_service.spreadsheets().values().update(
        spreadsheetId=OUTPUT_SHEET_ID,
        range=f"'{tab_title}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    print(f"Wrote {len(results)} rows to tab '{tab_title}'.")


def main() -> None:
    """Fetch CPA survey results from Postgres and augment with location and state data from BQ."""
    logging.basicConfig(level=logging.INFO)

    # Fetch survey results from PostgreSQL
    print("Fetching survey results from PostgreSQL...")
    connection_string = get_secret(DATABASE_CONNECTION_STRING_SECRET_NAME)
    with proxy.connection_for_instance(connection_string=connection_string):
        db_username = get_secret(DATABASE_USERNAME_SECRET_NAME)
        db_password = get_secret(DATABASE_PASSWORD_SECRET_NAME)
        db_engine = create_db_engine(db_password=db_password, db_username=db_username)
        survey_results = fetch_survey_results(db_engine)
        print(f"Found {len(survey_results)} survey result(s).")

    # Extract unique pseudo IDs and fetch client data from BigQuery
    unique_pseudo_ids = list({r["client_pseudo_id"] for r in survey_results})
    print(
        f"Fetching client data for {len(unique_pseudo_ids)} unique pseudo IDs from BigQuery..."
    )
    client_data_map = fetch_client_data_from_bq(unique_pseudo_ids)
    print(f"Found client data for {len(client_data_map)} client(s).")

    # Enrich survey results with client data
    for result in survey_results:
        client_data = client_data_map.get(result["client_pseudo_id"], {})
        result["location"] = client_data.get("location")
        result["state_code"] = client_data.get("state_code")

    # Write results to a new tab in the output sheet
    print("Writing results to new tab...")
    sheets_service = get_sheets_service()
    write_results_to_new_tab(sheets_service, survey_results)


if __name__ == "__main__":
    main()
