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
"""Cloud Run job script for sending CPA UT AP&P notifications.

We will send an email to the supervisor of each client who has recently
started supervision.

Local Usage:
    python -m recidiviz.tools.cpa.ut_app_enablement \
        --project-id recidiviz-staging \
        --dry-run true

    python -m recidiviz.tools.cpa.ut_app_enablement \
        --project-id recidiviz-staging \
        --redirect-addresses you@recidiviz.org them@recidiviz.org \
        --dry-run false

    python -m recidiviz.tools.cpa.ut_app_enablement \
        --project-id recidiviz-staging \
        --as-of-date 2026-01-15 \
        --dry-run true

Remote Usage:
    Execute the Cloud Run Job in GCP (no --project-id needed).
"""
import argparse
import datetime
import json
import logging
import sys
from typing import Any

import google.auth
from sendgrid.helpers.mail import (  # type: ignore[attr-defined]
    ClickTracking,
    DynamicTemplateData,
    Email,
    Mail,
    SubscriptionTracking,
    To,
    TrackingSettings,
)

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_gcp,
)
from recidiviz.utils.google_drive import get_credentials, get_sheets_service
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.sendgrid_client_wrapper import SendGridClientWrapper
from recidiviz.utils.string import StrictStringFormatter

logger = logging.getLogger(__name__)

NEW_SUPERVISION_STARTS_QUERY_TEMPLATE = """
WITH agents AS (
  SELECT
    sei.external_id AS staff_external_id,
    s.email,
    s.full_name,
  FROM `observations__global_provisioned_user_span.global_provisioned_user_session_materialized` pus
  JOIN `us_ut_normalized_state.state_staff` s ON (LOWER(s.email) = LOWER(pus.email_address))
  JOIN `us_ut_normalized_state.state_staff_external_id` sei USING(staff_id)
  WHERE s.state_code = 'US_UT'
    AND is_provisioned_case_planning_assistant = 'true'
    AND end_date IS NULL
),

new_clients_with_officer AS (
  SELECT DISTINCT
    supervision_office_name_end AS supervision_office,
    person_id,
    supervising_officer_external_id_end AS staff_external_id
  FROM `sessions.compartment_sessions_materialized`
  JOIN agents ON (supervising_officer_external_id_end = staff_external_id)
  WHERE state_code = 'US_UT'
    AND compartment_level_1 = 'SUPERVISION'
    AND compartment_level_2 IN ('PROBATION', 'PAROLE')
    AND supervision_office_name_end != 'EXTERNAL_UNKNOWN'
    AND supervision_office_name_end IN (
      'SALT LAKE AP&P',
      'TOOELE AP&P',
      'CENTRAL VALLEY OFFICE'
    )
    AND start_date BETWEEN DATE_SUB(DATE '{as_of_date}', INTERVAL 5 DAY) AND DATE '{as_of_date}'
)

SELECT
    nc.person_id,
    cm.full_name as client_full_name,
    cm.stable_person_external_id as external_id,
    cm.pseudonymized_id,
    a.full_name AS supervisor_full_name,
    a.email as supervisor_email,
FROM new_clients_with_officer nc 
LEFT JOIN `reentry.client_materialized` cm
    ON nc.person_id = cm.person_id
LEFT JOIN agents a 
    ON nc.staff_external_id = a.staff_external_id
"""

FROM_EMAIL_ADDRESS = "no-reply@recidiviz.org"
FROM_EMAIL_NAME = "Recidiviz"
DYNAMIC_TEMPLATE_ID = "d-0abd50acea15478c9fa25da41152922c"
SUBJECT_PREFIX = "Action Required: "
SUBJECT_SUFFIX = " Ready for Intake Assessment"

RESULTS_SPREADSHEET_ID = "1iN00PSsPb_fHLP91daTQMY9yLvuWGv7JVmXdnJWlY7A"
SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_HEADERS = ["Client Name", "Supervisor Name", "Supervisor Email", "Status"]


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        type=str,
        required=False,
        choices=[GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING],
        default=None,
        help="GCP project ID. Required when running locally, must not be set in GCP.",
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    parser.add_argument(
        "--as-of-date",
        type=datetime.date.fromisoformat,
        default=None,
        help="Look for supervision starts in the 3 days up to and including this date "
        "(YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--redirect-addresses",
        type=str,
        nargs="+",
        default=None,
        help="If set, all emails will be sent to these addresses instead of the "
        "actual supervisor emails. Useful for testing.",
    )
    parser.add_argument(
        "--credentials-directory",
        type=str,
        default=None,
        help="Path to directory containing credentials.json (and cached token.pickle) "
        "for Google Sheets access. If omitted, results will not be written to the sheet.",
    )
    return parser


def write_results_to_sheet(
    results: list[dict[str, str]],
    credentials_directory: str | None = None,
) -> None:
    """Creates a new tab named with today's date and writes one row per email result."""
    if credentials_directory:
        creds = get_credentials(
            credentials_directory, readonly=False, scopes=SHEET_SCOPES
        )
    else:
        # Use Application Default Credentials (service account on Cloud Run)
        creds, _ = google.auth.default(scopes=SHEET_SCOPES)
    service = get_sheets_service(creds)
    tab_name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    service.spreadsheets().batchUpdate(
        spreadsheetId=RESULTS_SPREADSHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
    ).execute()

    rows = [SHEET_HEADERS] + [
        [r["client_name"], r["supervisor_name"], r["supervisor_email"], r["status"]]
        for r in results
    ]
    service.spreadsheets().values().update(
        spreadsheetId=RESULTS_SPREADSHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    logger.info("Wrote %d result row(s) to sheet tab '%s'.", len(results), tab_name)


def fetch_new_supervision_starts(
    as_of_date: datetime.date | None = None,
) -> list[dict[str, Any]]:
    """Fetches recent supervision starts from BigQuery."""
    date = as_of_date or datetime.date.today()
    bq_client = BigQueryClientImpl()
    query_job = bq_client.run_query_async(
        query_str=StrictStringFormatter().format(
            NEW_SUPERVISION_STARTS_QUERY_TEMPLATE, as_of_date=date
        ),
        use_query_cache=True,
    )
    # if query returns two rows for the same person, just pick one
    rows = {row["person_id"]: dict(row) for row in query_job}
    return list(rows.values())


def format_person_name(
    full_name_json: str | None, first_name_only: bool = False
) -> str:
    """Parses a JSON full_name field and returns a human-readable name.

    Expected format: {"given_names": "JOHN", "middle_names": "SMITH",
                      "name_suffix": "", "surname": "DOE"}
    Returns: "John Doe"
    """
    if not full_name_json:
        return "Unknown"
    parsed = json.loads(full_name_json)
    given = parsed.get("given_names", "").strip()
    surname = parsed.get("surname", "").strip()
    if first_name_only:
        return given.title()
    return f"{given.title()} {surname.title()}".strip()


def build_template_data(
    row: dict[str, Any], redirect_addresses: list[str] | None = None
) -> dict[str, str]:
    """Builds the dynamic template data dict for a single row."""
    client_name = format_person_name(row["client_full_name"])
    subject = (
        f"[{row['supervisor_email']}] " if redirect_addresses else ""
    ) + f"{SUBJECT_PREFIX}{client_name}{SUBJECT_SUFFIX}"
    return {
        "subject": subject,
        "client_name": client_name,
        "client_first_name": format_person_name(
            row["client_full_name"], first_name_only=True
        ),
        "supervisor_name": format_person_name(row["supervisor_full_name"]),
        "pseudo_id": row["pseudonymized_id"],
        "external_id": row["external_id"],
    }


def send_email(
    sendgrid_client: SendGridClientWrapper,
    to_email: str,
    template_data: dict[str, str],
    redirect_addresses: list[str] | None = None,
) -> bool:
    """Sends an email via SendGrid using a dynamic template."""
    actual_to = (
        [To(addr) for addr in redirect_addresses]
        if redirect_addresses
        else To(to_email)
    )

    message = Mail(
        from_email=Email(FROM_EMAIL_ADDRESS, FROM_EMAIL_NAME),
        to_emails=actual_to,  # type: ignore[arg-type]
    )
    message.template_id = DYNAMIC_TEMPLATE_ID  # type: ignore[attr-defined]
    message.dynamic_template_data = DynamicTemplateData(template_data)  # type: ignore[attr-defined]

    # Disable unsubscribes (not needed since this is not a marketing email)
    # Disable click tracking (because that messes up the https)
    message.tracking_settings = TrackingSettings(  # type: ignore[attr-defined]
        click_tracking=ClickTracking(enable=False, enable_text=False),
        subscription_tracking=SubscriptionTracking(enable=False),
    )

    try:
        response = sendgrid_client.client.send(message)
    except Exception:
        logger.exception("Error sending email to %s", to_email)
        return False

    logger.info(
        "Sent email to %s (status %s)",
        redirect_addresses if redirect_addresses else to_email,
        response.status_code,
    )
    return True


def fetch_already_notified_client_names(
    sendgrid_client: SendGridClientWrapper,
) -> set[str]:
    """Returns client names already notified via the UT app notification template in the past 7 days."""
    seven_days_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=7
    )
    cutoff = seven_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
    response = sendgrid_client.client.client.messages.get(  # type: ignore[attr-defined]
        query_params={
            "limit": 1000,
            "query": f'template_id="{DYNAMIC_TEMPLATE_ID}" AND last_event_time>TIMESTAMP "{cutoff}"',
        },
    )
    body = json.loads(response.body)
    client_names = set()
    for msg in body.get("messages", []):
        subject = msg.get("subject", "")
        # Strip optional redirect prefix e.g. "[supervisor@example.com] "
        if "]" in subject:
            subject = subject.split("] ", 1)[-1]
        if subject.startswith(SUBJECT_PREFIX) and subject.endswith(SUBJECT_SUFFIX):
            client_names.add(subject[len(SUBJECT_PREFIX) : -len(SUBJECT_SUFFIX)])
    return client_names


def run(
    *,
    dry_run: bool,
    redirect_addresses: list[str] | None = None,
    as_of_date: datetime.date | None = None,
    credentials_directory: str | None = None,
) -> None:
    """Main job logic."""
    rows = fetch_new_supervision_starts(as_of_date)
    logger.info("Found %d new supervision start(s).", len(rows))

    if redirect_addresses:
        logger.info("Redirecting all emails to %s", redirect_addresses)

    sendgrid_client = (
        None if dry_run else SendGridClientWrapper(key_type="ut_app_notifications")
    )

    already_sent: set[str] = set()
    if not dry_run:
        assert sendgrid_client is not None
        already_sent = fetch_already_notified_client_names(sendgrid_client)
        logger.info("Found %d already-notified client(s).", len(already_sent))

    results: list[dict[str, str]] = []

    for row in rows:
        supervisor_email = row["supervisor_email"] or ""
        template_data = build_template_data(row, redirect_addresses)
        client_name = template_data["client_name"]
        supervisor_name = template_data["supervisor_name"]
        logger.info("  %s -> %s", client_name, supervisor_email)

        if dry_run:
            logger.info("    template_data: %s", template_data)
            results.append(
                {
                    "client_name": client_name,
                    "supervisor_name": supervisor_name,
                    "supervisor_email": supervisor_email,
                    "status": "dry_run",
                }
            )
            continue

        if client_name in already_sent:
            logger.info("Skipping %s — already notified.", client_name)
            results.append(
                {
                    "client_name": client_name,
                    "supervisor_name": supervisor_name,
                    "supervisor_email": supervisor_email,
                    "status": "skipped_already_notified",
                }
            )
            continue

        if not supervisor_email:
            logger.warning("Skipping %s — no supervisor email.", client_name)
            results.append(
                {
                    "client_name": client_name,
                    "supervisor_name": supervisor_name,
                    "supervisor_email": supervisor_email,
                    "status": "skipped_no_email",
                }
            )
            continue

        assert sendgrid_client is not None
        success = send_email(
            sendgrid_client, supervisor_email, template_data, redirect_addresses
        )
        if not success:
            logger.error(
                "Failed to send email to %s for %s", supervisor_email, client_name
            )
            results.append(
                {
                    "client_name": client_name,
                    "supervisor_name": supervisor_name,
                    "supervisor_email": supervisor_email,
                    "status": "failed",
                }
            )
        else:
            results.append(
                {
                    "client_name": client_name,
                    "supervisor_name": supervisor_name,
                    "supervisor_email": supervisor_email,
                    "status": "sent",
                }
            )

    if dry_run:
        logger.info("[DRY RUN] No further action taken.")

    if results and (credentials_directory or in_gcp()):
        write_results_to_sheet(results, credentials_directory)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = create_parser().parse_args()

    if in_gcp():
        if args.project_id:
            raise ValueError(
                "Do not set --project-id when running in GCP; the project "
                "will be inferred from the execution environment."
            )
        run(
            dry_run=args.dry_run,
            redirect_addresses=args.redirect_addresses,
            as_of_date=args.as_of_date,
            credentials_directory=args.credentials_directory,
        )
    else:
        if args.project_id is None:
            raise ValueError("You must provide --project-id when running locally.")
        with local_project_id_override(args.project_id):
            run(
                dry_run=args.dry_run,
                redirect_addresses=args.redirect_addresses,
                as_of_date=args.as_of_date,
                credentials_directory=args.credentials_directory,
            )
