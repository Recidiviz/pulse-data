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
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from google.cloud import storage
from jinja2 import Template
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import (
    REMINDER_EMAILS_BUCKET_PROD,
    REMINDER_EMAILS_BUCKET_STAGING,
    UNSUBSCRIBE_GROUP_ID,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.environment import in_gcp_production, in_gcp_staging
from recidiviz.utils.sendgrid_client_wrapper import SendGridClientWrapper


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


def send_reminder_emails_for_superagency(
    session: Session,
    agency_id: int,
    dry_run: bool,
    logger: logging.Logger,
    today: datetime.date,
) -> None:
    """Every month we send a reminder email to all users subscribed to their agency's
    emails to notify them that they have data missing from their most recent annual / fiscal-year
    reports. For superagencies their reports will include data about their child agencies as well as
    their own superagency metrics.
    """

    subscribed_user_associations = AgencyUserAccountAssociationInterface.get_subscribed_user_associations_by_agency_id(
        session=session, agency_id=int(agency_id)
    )

    msg = "DRY_RUN " if dry_run is True else ""

    if len(subscribed_user_associations) == 0:
        msg += "No users subscribed, no emails to send"
        logger.info(msg)
        return

    agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
    child_agencies = AgencyInterface.get_child_agencies_for_agency(
        session=session, agency=agency
    )

    for user_association in subscribed_user_associations:
        (
            system_to_monthly_metric_to_num_child_agencies,
            date_range_to_system_to_annual_metric_to_num_child_agencies,
            monthly_report_date_range,
        ) = get_missing_metrics_for_superagencies(
            agencies=[agency] + child_agencies,
            today=today,
            session=session,
            days_after_time_period_to_send_email=(
                user_association.days_after_time_period_to_send_email
                if user_association.days_after_time_period_to_send_email is not None
                else 15
            ),
        )

        if (
            len(system_to_monthly_metric_to_num_child_agencies) == 0
            and len(date_range_to_system_to_annual_metric_to_num_child_agencies) == 0
        ):
            # Don't send reminder email if no metrics are missing
            msg += "No missing metrics, not sending email"
            logger.info(msg)
            return

        domain = "publisher-staging" if in_gcp_staging() is True else "publisher"
        html = _reminder_email_builder_superagency(
            agency=agency,
            system_to_monthly_metric_to_num_child_agencies=system_to_monthly_metric_to_num_child_agencies,
            date_range_to_system_to_annual_metric_to_num_child_agencies=date_range_to_system_to_annual_metric_to_num_child_agencies,
            domain=domain,
            monthly_report_date_range=monthly_report_date_range,
        )

        _save_reminder_email_to_GCP(
            agency=agency,
            html=html,
            today=today,
            logger=logger,
            dry_run=dry_run,
            msg=msg,
        )

        _send_reminder_email(
            agency=agency,
            html=html,
            logger=logger,
            dry_run=dry_run,
            subscribed_user_email=user_association.user_account.email,
        )


def get_missing_metrics_for_superagencies(
    agencies: List[schema.Agency],
    today: datetime.date,
    session: Session,
    days_after_time_period_to_send_email: int,
) -> Tuple[
    Dict[schema.System, Dict[str, int]],
    Dict[
        Tuple[datetime.date, datetime.date],
        Dict[schema.System, Dict[str, int]],
    ],
    Optional[Tuple[datetime.date, datetime.date]],
]:
    """
    Retrieves the missing monthly and annual metrics for a superagency along with the
    amount of child agencies missing the metrics. Returns a tuple:
    (system_to_monthly_metric_to_num_child_agencies,
    date_range_to_system_to_annual_metric_to_num_child_agencies, monthly_report_date_range ).

    Args:
        agency (schema.Agency): The agencies to fetch the missing metrics for. This list includes
        the superagency and its child agencies.

        session (Session): The database session to use.

        today: A datetime.date object representing today's date.
    """

    system_to_monthly_metric_to_num_child_agencies: Dict[
        schema.System, Dict[str, int]
    ] = defaultdict(lambda: defaultdict(int))
    date_range_to_system_to_annual_metric_to_num_child_agencies: Dict[
        Tuple[datetime.date, datetime.date],
        Dict[schema.System, Dict[str, int]],
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    monthly_report_date_range = None
    for agency in agencies:
        (
            system_to_missing_monthly_metrics,
            date_range_to_system_to_missing_annual_metrics,
            agency_monthly_report_date_range,
        ) = get_missing_metrics(
            agency=agency,
            today=today,
            session=session,
            days_after_time_period_to_send_email=days_after_time_period_to_send_email,
        )

        monthly_report_date_range = (
            agency_monthly_report_date_range
            if agency_monthly_report_date_range is not None
            else monthly_report_date_range
        )

        for (
            system,
            missing_monthly_metrics,
        ) in system_to_missing_monthly_metrics.items():
            for metric in missing_monthly_metrics:
                system_to_monthly_metric_to_num_child_agencies[system][
                    metric.display_name.title()
                ] += 1

        for (
            date_range,
            system_to_missing_annual_metrics,
        ) in date_range_to_system_to_missing_annual_metrics.items():
            for (
                system,
                missing_annual_metrics,
            ) in system_to_missing_annual_metrics.items():
                for metric in missing_annual_metrics:
                    date_range_to_system_to_annual_metric_to_num_child_agencies[
                        date_range
                    ][system][metric.display_name.title()] += 1
    return (
        system_to_monthly_metric_to_num_child_agencies,
        date_range_to_system_to_annual_metric_to_num_child_agencies,
        monthly_report_date_range,
    )


def send_reminder_emails(
    session: Session,
    agency_id: int,
    dry_run: bool,
    logger: logging.Logger,
    today: datetime.date,
) -> None:
    """Every month we send a reminder email to all users subscribed to their agency's
    emails to notify them that they have data missing from their most recent annual / fiscal-year
    reports.
    """
    subscribed_user_associations = AgencyUserAccountAssociationInterface.get_subscribed_user_associations_by_agency_id(
        session=session, agency_id=int(agency_id)
    )

    msg = "DRY_RUN " if dry_run is True else ""

    if len(subscribed_user_associations) == 0:
        msg += "No users subscribed, no emails to send"
        logger.info(msg)
        return

    agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)

    for user_association in subscribed_user_associations:
        (
            system_to_missing_monthly_metrics,
            date_range_to_system_to_missing_annual_metrics,
            monthly_report_date_range,
        ) = get_missing_metrics(
            agency=agency,
            today=today,
            session=session,
            days_after_time_period_to_send_email=(
                user_association.days_after_time_period_to_send_email
                if user_association.days_after_time_period_to_send_email is not None
                else 15
            ),
        )

        if (
            len(system_to_missing_monthly_metrics) == 0
            and len(date_range_to_system_to_missing_annual_metrics) == 0
        ):
            # Don't send reminder email if no metrics are missing
            msg += "No missing metrics, not sending email"
            logger.info(msg)
            return

        domain = "publisher-staging" if in_gcp_staging() is True else "publisher"
        html = _reminder_email_builder(
            agency=agency,
            system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
            date_range_to_system_to_missing_annual_metrics=date_range_to_system_to_missing_annual_metrics,
            domain=domain,
            monthly_report_date_range=monthly_report_date_range,
        )

        _save_reminder_email_to_GCP(
            agency=agency,
            html=html,
            today=today,
            logger=logger,
            dry_run=dry_run,
            msg=msg,
        )

        _send_reminder_email(
            agency=agency,
            html=html,
            logger=logger,
            dry_run=dry_run,
            subscribed_user_email=user_association.user_account.email,
        )


def _reminder_email_builder_superagency(
    agency: schema.Agency,
    system_to_monthly_metric_to_num_child_agencies: Dict[schema.System, Dict[str, int]],
    date_range_to_system_to_annual_metric_to_num_child_agencies: Dict[
        Tuple[datetime.date, datetime.date],
        Dict[schema.System, Dict[str, int]],
    ],
    domain: str,
    monthly_report_date_range: Optional[Tuple[datetime.date, datetime.date]] = None,
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
        "recidiviz/justice_counts/utils/email_templates/superagency_reminder_email.html.jinja2",
        mode="r",
        encoding="utf-8",
    ) as file:
        template_string = file.read()

        data = {
            "data_entry_url": data_entry_url,
            "system_to_monthly_metric_to_num_child_agencies": system_to_monthly_metric_to_num_child_agencies,
            "date_range_to_system_to_annual_metric_to_num_child_agencies": date_range_to_system_to_annual_metric_to_num_child_agencies,
            "monthly_report_date_range": monthly_report_date_range,
            "month_names": calendar.month_name[0:],
            "superagency_system": schema.System.SUPERAGENCY,
        }

        # Create a Jinja template
        template = Template(template_string)

        # Render the template with the data
        html = template.render(data)

        return html


def _reminder_email_builder(
    agency: schema.Agency,
    system_to_missing_monthly_metrics: Dict[schema.System, List[MetricDefinition]],
    date_range_to_system_to_missing_annual_metrics: Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ],
    domain: str,
    monthly_report_date_range: Optional[Tuple[datetime.date, datetime.date]] = None,
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
    agency: schema.Agency,
    today: datetime.date,
    session: Session,
    days_after_time_period_to_send_email: int,
) -> Tuple[
    Dict[schema.System, List[MetricDefinition]],
    Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ],
    Optional[Tuple[datetime.date, datetime.date]],
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
        session=session,
        agency_id=agency.id,
        days_after_time_period_to_send_email=days_after_time_period_to_send_email,
        today=today,
    )

    monthly_report_date_range = (
        (
            latest_monthly_report.date_range_start,
            latest_monthly_report.date_range_end,
        )
        if latest_monthly_report is not None
        else None
    )

    latest_annual_reports = ReportInterface.get_latest_annual_reports_by_agency_id(
        session=session,
        agency_id=agency.id,
        days_after_time_period_to_send_email=days_after_time_period_to_send_email,
        today=today,
    )

    (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
    ) = _get_missing_metrics_by_system(
        session=session,
        reports=(
            [latest_monthly_report] + latest_annual_reports
            if latest_monthly_report is not None
            else latest_annual_reports
        ),
        is_superagency=agency.is_superagency,
    )

    return (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
        monthly_report_date_range,
    )


def _get_missing_metrics_by_system(
    session: Session,
    reports: List[schema.Report],
    is_superagency: bool,
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

        agency (schema.Agency): The agency to fetch the missing metrics for.
    """

    system_to_missing_monthly_metrics: Dict[
        schema.System, List[MetricDefinition]
    ] = defaultdict(list)
    date_range_to_system_to_missing_annual_metrics: Dict[
        Tuple[datetime.date, datetime.date], Dict[schema.System, List[MetricDefinition]]
    ] = defaultdict(lambda: defaultdict(list))

    for report in reports:
        if report is None:
            # This assumes that the user has reports for the previous time periods.
            # This is a fine assumption to work off of since we automatically create
            # reports for agencies.
            continue

        reporting_frequency = (
            schema.ReportingFrequency.MONTHLY
            if abs((report.date_range_end - report.date_range_start).days) <= 31
            else schema.ReportingFrequency.ANNUAL
        )

        report_metrics = ReportInterface.get_metrics_by_report(
            session=session,
            report=report,
            is_superagency=is_superagency,
        )

        for report_metric in report_metrics:
            if report_metric.is_metric_enabled is not True:
                # If the metric is disabled, skip it.
                continue

            if report_metric.value is not None:
                # If there's a report metric value, skip the metric
                continue

            # Check each aggregated dimension
            for a in report_metric.aggregated_dimensions:
                if is_aggregated_dimension_data_reported(a):
                    # If data is reported for the breakdown, skip the metric.
                    continue

            if (
                report_metric.disaggregated_by_supervision_subsystems is True
                and report_metric.metric_definition.system == schema.System.SUPERVISION
            ):
                # If the metric is disaggregated by subsystem but is a supervision metric, skip it.
                continue

            if (
                report_metric.disaggregated_by_supervision_subsystems is False
                and report_metric.metric_definition.system
                in schema.System.supervision_subsystems()
            ):
                # If the metric is reported as the combined values of supervision subsystem,
                # but the metric is for a supervision subsystem, skip it.
                continue

            if reporting_frequency == schema.ReportingFrequency.MONTHLY:
                system_to_missing_monthly_metrics[
                    report_metric.metric_definition.system
                ].append(report_metric.metric_definition)
            else:
                date_range_to_system_to_missing_annual_metrics[
                    (report.date_range_start, report.date_range_end)
                ][report_metric.metric_definition.system].append(
                    report_metric.metric_definition
                )

    return (
        system_to_missing_monthly_metrics,
        date_range_to_system_to_missing_annual_metrics,
    )


def is_aggregated_dimension_data_reported(
    aggregated_dimension: MetricAggregatedDimensionData,
) -> bool:
    if (
        aggregated_dimension.dimension_to_value is not None
        and aggregated_dimension.dimension_to_enabled_status is not None
    ):
        for dimension in aggregated_dimension.dimension_to_value.keys():
            if (
                aggregated_dimension.dimension_to_value.get(dimension) is not None
                and aggregated_dimension.dimension_to_enabled_status.get(dimension)
                is True
            ):
                return True
    return False


def _save_reminder_email_to_GCP(
    html: str,
    today: datetime.date,
    agency: schema.Agency,
    logger: logging.Logger,
    msg: str,
    dry_run: bool,
) -> None:
    """Saves reminder email in GCP so that we have a copy to review every month"""

    date_str = (
        f"DRY-RUN-{today.month}-{today.day}-{today.year}.html"
        if dry_run is True
        else f"{today.month}-{today.day}-{today.year}.html"
    )
    file_path = f"{agency.name}/{date_str}"
    storage_client = storage.Client()
    if in_gcp_production():
        bucket = storage_client.bucket(REMINDER_EMAILS_BUCKET_PROD)
    else:
        bucket = storage_client.bucket(REMINDER_EMAILS_BUCKET_STAGING)

    blob = bucket.blob(file_path)
    blob.upload_from_string(html)
    msg += "Saving email to GCS"
    logger.info(msg)


def _send_reminder_email(
    subscribed_user_email: str,
    logger: logging.Logger,
    dry_run: bool,
    html: str,
    agency: schema.Agency,
) -> None:
    """Sends reminder email to all users that belong to an except for CSG users"""
    send_grid_client = SendGridClientWrapper(key_type="justice_counts")
    try:
        if dry_run is True:
            logger.info("DRY_RUN: Would send email to %s", subscribed_user_email)
            return

        logger.info("Sending email to %s", subscribed_user_email)
        send_grid_client.send_message(
            to_email=subscribed_user_email,
            from_email="no-reply@justice-counts.org",
            from_email_name="Justice Counts",
            subject=f"Reminder to Upload Metrics for {agency.name} in Publisher",
            html_content=html,
            disable_link_click=True,
        )
    except Exception as e:
        logger.exception(
            "Failed to send reminder email to %s. %s",
            subscribed_user_email,
            e,
        )
