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
"""Class with methods for processing and storing roster data."""
from datetime import date
from typing import Dict, List

from google.cloud import bigquery

from recidiviz.admin_panel.line_staff_tools.constants import (
    CASE_TRIAGE_STATE_CODES,
    EMAIL_STATE_CODES,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import (
    PO_REPORT_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.case_triage.user_context import UserContext
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import local_project_id_override

_EXPECTED_ROSTER_KEYS = [
    "employee_name",
    "email_address",
    "job_title",
    "district",
    "external_id",
]


Roster = List[Dict[str, str]]


class RosterManager:
    """Handles validation and storage for uploaded rosters"""

    def __init__(self, state_code: StateCode, rows: Roster) -> None:
        self.state_code = state_code
        self.rows = rows

        if in_development():
            with local_project_id_override(GCP_PROJECT_STAGING):
                self.bq = BigQueryClientImpl()
        else:
            self.bq = BigQueryClientImpl()

    def validate_roster_upload(self) -> None:
        """Raises a ValueError if any validation tests fail, indicating the upload should be rejected."""
        if not self.rows:
            raise ValueError("No rows found in CSV file")

        test_row = self.rows[0]
        if len(test_row) != len(_EXPECTED_ROSTER_KEYS):
            raise ValueError("Invalid number of columns")
        for key in _EXPECTED_ROSTER_KEYS:
            if key not in test_row:
                raise ValueError(f'Missing column "{key}"')

        if self.state_code in EMAIL_STATE_CODES:
            # we will join on this district value, so make sure we can handle it
            dataset_ref = self.bq.dataset_ref_for_id(PO_REPORT_DATASET)
            query_job = self.bq.run_query_async(
                f"""
                SELECT DISTINCT district
                FROM `{self.bq.project_id}.{dataset_ref.dataset_id}.officer_supervision_district_association_materialized`
                WHERE state_code = '{self.state_code.value}'
                """
            )
            res = query_job.result()
            known_districts = [row[0] for row in res]
            unknown_districts = {
                row["district"]
                for row in self.rows
                if row["district"] not in known_districts
            }
            if unknown_districts:
                raise ValueError(
                    f"Unrecognized district names: {', '.join(unknown_districts)}. Supported district names are: {', '.join(known_districts)}"
                )

    def normalize_roster_values(self) -> None:
        """Mutates roster input to normalize values as needed."""

        for row in self.rows:
            # Enforce casing for columns where we have a preference.
            row["email_address"] = row["email_address"].lower()
            row["external_id"] = row["external_id"].upper()

    def grant_access(self) -> None:
        # Everyone on the roster gets access to the designated tools
        if self.state_code in CASE_TRIAGE_STATE_CODES:
            self._grant_case_triage_access()
        if self.state_code in EMAIL_STATE_CODES:
            self._grant_po_report_access()

    def _grant_case_triage_access(self) -> None:
        dataset_ref = self.bq.dataset_ref_for_id(STATIC_REFERENCE_TABLES_DATASET)

        query_job = self.bq.run_query_async(
            f"""
            SELECT email_address FROM `{self.bq.project_id}.{dataset_ref.dataset_id}.case_triage_users`
            WHERE state_code = '{self.state_code.value}'
            """
        )
        res = query_job.result()
        existing_user_emails = [row[0] for row in res]

        rows_to_insert = [
            {
                "state_code": self.state_code.value,
                "officer_external_id": row["external_id"],
                "email_address": row["email_address"],
                "segment_id": UserContext.segment_user_id_for_email(
                    row["email_address"]
                ),
                "received_access": date.today().strftime("%Y-%m-%d"),
            }
            for row in self.rows
            if row["email_address"] not in existing_user_emails
        ]
        if rows_to_insert:
            insert_job = self.bq.load_into_table_async(
                dataset_ref, "case_triage_users", rows_to_insert
            )
            insert_job.result()

    def _grant_po_report_access(self) -> None:
        dataset_ref = self.bq.dataset_ref_for_id(STATIC_REFERENCE_TABLES_DATASET)
        query_job = self.bq.run_query_async(
            f"""
            SELECT email_address FROM `{self.bq.project_id}.{dataset_ref.dataset_id}.po_report_recipients`
            WHERE state_code = '{self.state_code.value}'
            """
        )
        res = query_job.result()
        existing_user_emails = [row[0] for row in res]

        rows_to_insert = [
            {
                "state_code": self.state_code.value,
                "officer_external_id": row["external_id"],
                "email_address": row["email_address"],
                "district": row["district"],
                "received_access": date.today().strftime("%Y-%m-%d"),
            }
            for row in self.rows
            if row["email_address"] not in existing_user_emails
        ]
        if rows_to_insert:
            insert_job = self.bq.load_into_table_async(
                dataset_ref, "po_report_recipients", rows_to_insert
            )
            insert_job.result()

    def store_roster(self) -> None:
        dataset_ref = self.bq.dataset_ref_for_id(STATIC_REFERENCE_TABLES_DATASET)
        roster_table_name = f"{self.state_code.value.lower()}_roster"
        load_job = self.bq.load_into_table_async(
            dataset_ref,
            roster_table_name,
            self.rows,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job.result()
