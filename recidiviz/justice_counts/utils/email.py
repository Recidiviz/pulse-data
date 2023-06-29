# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utility file for constructing an email to be sent via Sendgrid."""
import logging
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper
from recidiviz.utils.environment import in_gcp_staging


def send_confirmation_email(
    session: Session,
    success: bool,
    file_name: str,
    agency_id: str,
    metric_key_to_errors: Optional[
        Dict[Optional[str], List[JusticeCountsBulkUploadException]]
    ] = None,
    metric_definitions: Optional[List[MetricDefinition]] = None,
) -> None:
    """At the end of Automatic Bulk Upload, we send a confirmation email to users
    notifying that their upload has either succeeded or failed. We do so by using the
    SendGridClientWrapper.

    Emails are sent to all users associated with the given agency.
    """
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")

    user_emails = AgencyUserAccountAssociationInterface.get_user_emails_by_agency_id(
        session=session, agency_id=int(agency_id)
    )

    subject_str, html = _email_builder(
        success=success,
        file_name=file_name,
        metric_key_to_errors=metric_key_to_errors,
        metric_definitions=metric_definitions,
    )

    for user_email in user_emails:
        # TODO(#21717) Un-gate email notifications
        if "@recidiviz.org" in user_email and in_gcp_staging():
            try:
                send_grid_client.send_message(
                    to_email=user_email,
                    from_email="no-reply@justice-counts.org",
                    from_email_name="Justice Counts",
                    subject=subject_str,
                    html_content=html,
                )
            except Exception as e:
                logging.exception("Failed to send confirmation email: %s", e)


def _email_builder(
    success: bool,
    file_name: str,
    metric_key_to_errors: Optional[
        Dict[Optional[str], List[JusticeCountsBulkUploadException]]
    ] = None,
    metric_definitions: Optional[List[MetricDefinition]] = None,
) -> Tuple[str, str]:
    """This is a helper function that constructs a string used for the email notification
    html and subject. The html and subject are constructed based on if the Automated Bulk
    Upload has succeeded or failed. We also include any warnings/errors in the html.
    """
    if success:
        subject_str = "Publisher: Automated Bulk Upload Success"

        html = f"""<p>Congratulations! The following file has been uploaded to Publisher: {file_name}.</p>"""
    else:
        subject_str = "Publisher: Automated Bulk Upload Failure"
        html = f"""<p>An error was encountered while uploading the {file_name} file to Publisher.</p>"""
        html += """<p>The team will review the issue and reach out to you regarding next steps.</p>"""

    if metric_key_to_errors is not None and metric_definitions is not None:
        metric_key_to_display_name = {
            metric.key: metric.display_name for metric in metric_definitions
        }

        html += "<p>Please review the warnings below; if needed, resolve the warnings in your file and reupload.</p>"
        for metric_key, errors in metric_key_to_errors.items():
            if errors == []:
                continue

            if metric_key is not None:
                html += f"<p>{metric_key_to_display_name[metric_key]}</p>"
            else:
                html += "<p></p>"
            for error in errors:
                html += f"<p>{error}</p>"
            html += "<p></p>"

    html += """<p>If you have any questions regarding your upload, please email justice-counts-support@csg.org.</p>"""
    return subject_str, html
