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
import calendar
import datetime
import itertools
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from google.cloud import storage
from jinja2 import Template
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.datapoints_for_metric import DatapointsForMetric
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import (
    REMINDER_EMAILS_BUCKET_PROD,
    REMINDER_EMAILS_BUCKET_STAGING,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper
from recidiviz.utils.environment import in_gcp_production, in_gcp_staging

UNSUBSCRIBE_GROUP_ID = 26272


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

    Emails are sent to subscribed users associated with the given agency (except for CSG users).
    """
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")

    subscribed_user_emails = (
        AgencyUserAccountAssociationInterface.get_subscribed_user_emails_by_agency_id(
            session=session, agency_id=int(agency_id)
        )
    )
    subject_str, html = _confirmation_email_builder(
        success=success,
        file_name=file_name,
        metric_key_to_errors=metric_key_to_errors,
        agency_id=agency_id,
        spreadsheet_id=spreadsheet_id,
    )

    # Send confirmation email to all users that belong to the agency
    # except for CSG users
    for user_email in subscribed_user_emails:
        if "@csg.org" not in user_email:
            try:
                send_grid_client.send_message(
                    to_email=user_email,
                    from_email="no-reply@justice-counts.org",
                    from_email_name="Justice Counts",
                    subject=subject_str,
                    html_content=html,
                    disable_link_click=True,
                    unsubscribe_group_id=UNSUBSCRIBE_GROUP_ID,
                )
            except Exception as e:
                logging.exception("Failed to send confirmation email: %s", e)


def _confirmation_email_builder(
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

    if metric_key_to_errors is not None and len(metric_key_to_errors) != 0:
        errors_warnings_url = f"https://{domain}.justice-counts.org/agency/{agency_id}/upload/{spreadsheet_id}/errors-warnings"
        html += f"""<p>Your file contained some formatting and data issues. Please review the relevant <a href="{errors_warnings_url}">warnings</a> to understand if further action is required.</p>"""

    html += """<p>If you have any questions regarding your upload, please email justice-counts-support@csg.org.</p>"""
    return subject_str, html


def send_reminder_emails(
    session: Session,
    agency_id: int,
) -> None:
    """Every month we send a reminder email to all users subscribed to their agency's
    emails to notify them that they have data missing from their most recent annual / fiscal-year
    reports.
    """
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")

    subscribed_user_emails = (
        AgencyUserAccountAssociationInterface.get_subscribed_user_emails_by_agency_id(
            session=session, agency_id=int(agency_id)
        )
    )

    if len(subscribed_user_emails) == 0:
        logging.debug("No users subscribed, no emails to send")
        return

    agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
    today = datetime.date.today()
    (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
        monthly_report_date_range,
    ) = get_missing_metrics(agency=agency, today=today, session=session)

    if (
        len(system_to_missing_monthly_metrics) == 0
        and len(date_range_to_system_to_missing_annual_metrics) == 0
    ):
        # Don't send reminder email if no metrics are missing
        logging.debug("No missing metrics, not sending email")
        return

    domain = "publisher-staging" if in_gcp_staging() is True else "publisher"
    html = _reminder_email_builder(
        agency=agency,
        system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
        domain=domain,
        monthly_report_date_range=monthly_report_date_range,
    )

    # Save email in GCP so that we have a copy to review every month
    date_str = f"{today.month}-{today.day}-{today.year}.html"
    file_path = f"{agency.name}/{date_str}"
    storage_client = storage.Client()
    if in_gcp_production():
        bucket = storage_client.bucket(REMINDER_EMAILS_BUCKET_PROD)
    else:
        bucket = storage_client.bucket(REMINDER_EMAILS_BUCKET_STAGING)

    blob = bucket.blob(file_path)
    blob.upload_from_string(html)

    # Send reminder email to all users that belong to the agency
    # except for CSG users
    for user_email in subscribed_user_emails:
        # TODO(#26282): Filter out Recidiviz Emails. For playtesting, only recidiviz email
        # addresses will get reminder emails
        if "@recidiviz.org" not in user_email:
            continue
        try:
            send_grid_client.send_message(
                to_email=user_email,
                from_email="no-reply@justice-counts.org",
                from_email_name="Justice Counts",
                subject=f"Reminder to Upload Metrics for {agency.name} in Publisher",
                html_content=html,
                disable_link_click=True,
            )
        except Exception as e:
            logging.exception("Failed to send reminder email to %s. %s", user_email, e)


def _reminder_email_builder(
    agency: schema.Agency,
    system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
    date_range_to_system_to_missing_annual_metrics: Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ],
    monthly_report_date_range: Tuple[datetime.date, datetime.date],
    domain: str,
) -> str:
    """This is a helper function that constructs a string used for the reminder email
    html. The html and subject are constructed based on what monthly and
    annual metrics the agency is missing.
    """

    data_entry_url = (
        f"https://{domain}.justice-counts.org/agency/{agency.id}/data-entry"
    )

    # Read the template file
    with open(
        "recidiviz/justice_counts/utils/email_templates/reminder_email.html.jinja2",
        mode="r",
        encoding="utf-8",
    ) as file:
        template_string = file.read()

        data = {
            "data_entry_url": data_entry_url,
            "system_to_missing_monthly_metrics": system_to_missing_monthly_metrics,
            "date_range_to_system_to_missing_annual_metrics": date_range_to_system_to_missing_annual_metrics,
            "monthly_report_date_range": monthly_report_date_range,
            "month_names": calendar.month_name[0:],
        }

        # Create a Jinja template
        template = Template(template_string)

        # Render the template with the data
        html = template.render(data)

        return html


# Helpers for Publisher Email Notifications


def get_missing_metrics(
    agency: schema.Agency, today: datetime.date, session: Session
) -> Tuple[
    Dict[schema.System, List[MetricDefinition]],
    Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ],
    Tuple[datetime.date, datetime.date],
]:
    """
    Retrieves the missing monthly and annual metrics for the agency. Returns a tuple:
    (system_to_missing_monthly_metrics, date_range_to_system_to_missing_annual_metrics).

    system_to_missing_monthly_metrics: missing monthly metrics grouped by system
    date_range_to_system_to_missing_annual_metrics: missing annual metrics, grouped by
    starting month of the report and system of the metric

    Args:
        agency (schema.Agency): The agency to fetch the missing metrics for.
        session (Session): The database session to use.
    """

    latest_monthly_report = ReportInterface.get_latest_monthly_report_by_agency_id(
        session=session, agency_id=agency.id
    )

    monthly_report_date_range = (
        (
            latest_monthly_report.date_range_start,
            latest_monthly_report.date_range_end,
        )
        if latest_monthly_report is not None
        else (
            (today - relativedelta(months=1)).replace(day=1),
            today.replace(day=1),
        )
    )

    latest_annual_reports = ReportInterface.get_latest_annual_reports_by_agency_id(
        session=session, agency_id=agency.id
    )

    metric_setting_datapoints = DatapointInterface.get_agency_datapoints(
        session=session, agency_id=agency.id
    )

    monthly_report_datapoints = (
        latest_monthly_report.datapoints if latest_monthly_report is not None else []
    )

    annual_reports_datapoints = list(
        itertools.chain(
            *[
                latest_annual_report.datapoints
                for latest_annual_report in latest_annual_reports
            ]
        )
    )

    metric_key_to_datapoints = DatapointInterface.build_metric_key_to_datapoints(
        datapoints=monthly_report_datapoints
        + annual_reports_datapoints
        + metric_setting_datapoints
    )

    (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
    ) = _get_missing_metrics_by_system(
        metric_key_to_datapoints=metric_key_to_datapoints,
    )

    return (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
        monthly_report_date_range,
    )


def _get_missing_metrics_by_system(
    metric_key_to_datapoints: Dict[str, DatapointsForMetric]
) -> Tuple[
    Dict[schema.System, List[MetricDefinition]],
    Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ],
]:
    """
    Provided the metric setting datapoints and the report datapoints of the most recent
    reports, this function returns the MetricDefinitions of the missing metrics
    grouped by system.

    Args:
        metric_key_to_datapoints (Dict[str, DatapointsForMetric]): A dict
        mapping metric definition key to DatapointsForMetric object for the corresponding
        metric

        agency (schema.Agency): The agency to fetch the missing metrics for.
    """

    system_to_missing_monthly_metrics: Dict[
        schema.System, List[MetricDefinition]
    ] = defaultdict(list)
    date_range_to_system_to_missing_annual_metrics: Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ] = defaultdict(lambda: defaultdict(list))

    for metric_key, datapoints_for_metric in metric_key_to_datapoints.items():
        # If the metric is not reported by this agency, continue
        if datapoints_for_metric.is_metric_enabled is not True:
            continue

        # If there is data reported for the metric, continue
        if (
            datapoints_for_metric.aggregated_value is not None
            or DatapointsForMetric.are_disaggregated_metrics_reported(
                datapoints_for_metric=datapoints_for_metric
            )
            is True
        ):
            continue

        metric_definition = METRIC_KEY_TO_METRIC[metric_key]

        reporting_frequency = (
            datapoints_for_metric.custom_reporting_frequency.frequency
            or metric_definition.reporting_frequency
        )

        if reporting_frequency == schema.ReportingFrequency.MONTHLY:
            system_to_missing_monthly_metrics[metric_definition.system].append(
                metric_definition
            )

        else:
            report_start_month = (
                datapoints_for_metric.custom_reporting_frequency.starting_month or 1
            )
            today = datetime.date.today()
            current_year = today.year
            current_month = today.month

            if current_month >= report_start_month:
                start_year = current_year - 1
                end_year = current_year
            else:
                # This will catch the edge case of fiscal-year reports with
                # end dates in the current year that have not passed yet.
                # For example, say its June 2023, but a fiscal year report is
                # reported from July in one year to July in the next. The most
                # recent fiscal year report will be July 2021 - July 2022 because
                # the report from July 2022 - July 2023 is still in progress.
                start_year = current_year - 2
                end_year = current_year - 1

            start_date = datetime.date(start_year, report_start_month, 1)
            end_date = datetime.date(end_year, report_start_month, 1)
            date_range = (start_date, end_date)
            date_range_to_system_to_missing_annual_metrics[date_range][
                metric_definition.system
            ].append(metric_definition)
    return (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
    )
