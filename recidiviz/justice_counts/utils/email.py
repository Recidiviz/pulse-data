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
import os
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper
from recidiviz.utils.environment import in_gcp_staging


def send_confirmation_email(
    session: Session,
    success: bool,
    file_name: str,
    agency_id: str,
    spreadsheet_id: Optional[str] = None,
    metric_key_to_errors: Optional[
        Dict[Optional[str], List[JusticeCountsBulkUploadException]]
    ] = None,
) -> None:
    """At the end of Automatic Bulk Upload, we send a confirmation email to users
    notifying that their upload has either succeeded or failed. We do so by using the
    SendGridClientWrapper.

    Emails are sent to all users associated with the given agency (except for CSG users).
    """
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")

    user_emails = AgencyUserAccountAssociationInterface.get_user_emails_by_agency_id(
        session=session, agency_id=int(agency_id)
    )
    subject_str, html = _email_builder(
        success=success,
        file_name=file_name,
        metric_key_to_errors=metric_key_to_errors,
        agency_id=agency_id,
        spreadsheet_id=spreadsheet_id,
    )

    # Send confirmation email to all users that belong to the agency
    # except for CSG users
    for user_email in user_emails:
        if "@csg.org" not in user_email:
            try:
                send_grid_client.send_message(
                    to_email=user_email,
                    from_email="no-reply@justice-counts.org",
                    from_email_name="Justice Counts",
                    subject=subject_str,
                    html_content=html,
                    disable_link_click=True,
                )
            except Exception as e:
                logging.exception("Failed to send confirmation email: %s", e)


def _email_builder(
    success: bool,
    file_name: str,
    agency_id: str,
    spreadsheet_id: Optional[str] = None,
    metric_key_to_errors: Optional[
        Dict[Optional[str], List[JusticeCountsBulkUploadException]]
    ] = None,
) -> Tuple[str, str]:
    """This is a helper function that constructs a string used for the email notification
    html and subject. The html and subject are constructed based on if the Automated Bulk
    Upload has succeeded or failed. We also include links to standalone review and
    errors/warnings pages in the html.
    """
    _, filename = os.path.split(file_name)

    if in_gcp_staging():
        domain = "publisher-staging"
    else:
        domain = "publisher"

    if success:
        subject_str = "Publisher: Automated Bulk Upload Success"

        review_url = f"https://{domain}.justice-counts.org/agency/{agency_id}/upload/{spreadsheet_id}/review-metrics"
        html = f"""<p>Congratulations! The file <b>{filename}</b> has been uploaded to the Justice Counts Publisher. You can now <a href="{review_url}">review and publish your uploaded data</a> on Publisher.</p>"""
    else:
        subject_str = "Publisher: Automated Bulk Upload Failure"
        html = f"""<p>An error was encountered while uploading the {filename} file to Publisher.</p>"""

    if metric_key_to_errors is not None:
        errors_warnings_url = f"https://{domain}.justice-counts.org/agency/{agency_id}/upload/{spreadsheet_id}/errors-warnings"
        html += f"""<p>Your file contained some formatting and data issues. Please review the relevant <a href="{errors_warnings_url}">warnings</a> to understand if further action is required.</p>"""

    html += """<p>If you have any questions regarding your upload, please email justice-counts-support@csg.org.</p>"""
    return subject_str, html
