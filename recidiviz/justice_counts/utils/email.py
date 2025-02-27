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
import itertools
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.datapoints_for_metric import DatapointsForMetric
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    get_supervision_subsystem_metric_definition,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper
from recidiviz.utils.environment import in_gcp_staging

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
    subject_str, html = _email_builder(
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

    if metric_key_to_errors is not None and len(metric_key_to_errors) != 0:
        errors_warnings_url = f"https://{domain}.justice-counts.org/agency/{agency_id}/upload/{spreadsheet_id}/errors-warnings"
        html += f"""<p>Your file contained some formatting and data issues. Please review the relevant <a href="{errors_warnings_url}">warnings</a> to understand if further action is required.</p>"""

    html += """<p>If you have any questions regarding your upload, please email justice-counts-support@csg.org.</p>"""
    return subject_str, html


# Helpers for Publisher Email Notifications


def get_missing_metrics(
    agency: schema.Agency, session: Session
) -> Tuple[
    Dict[schema.System, List[MetricDefinition]],
    Dict[schema.System, Dict[int, List[MetricDefinition]]],
]:
    """
    Retrieves the missing monthly and annual metrics for the agency. Returns a tuple:
    (system_to_missing_monthly_metrics, system_to_starting_month_to_missing_annual_metrics).

    system_to_missing_monthly_metrics: missing monthly metrics grouped by system
    system_to_starting_month_to_missing_annual_metrics: missing annual metrics, grouped by system
    and starting month of the report.

    Args:
        agency (schema.Agency): The agency to fetch the missing metrics for.
        session (Session): The database session to use.
    """

    latest_monthly_report = ReportInterface.get_latest_monthly_report_by_agency_id(
        session=session, agency_id=agency.id
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
        system_to_starting_month_to_missing_annual_metrics,
    ) = _get_missing_metrics_by_system(
        metric_key_to_datapoints=metric_key_to_datapoints, agency=agency
    )

    return (
        system_to_missing_monthly_metrics,
        system_to_starting_month_to_missing_annual_metrics,
    )


def _get_missing_metrics_by_system(
    metric_key_to_datapoints: Dict[str, DatapointsForMetric], agency: schema.Agency
) -> Tuple[
    Dict[schema.System, List[MetricDefinition]],
    Dict[schema.System, Dict[int, List[MetricDefinition]]],
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
    system_to_starting_month_to_missing_annual_metrics: Dict[
        schema.System, Dict[int, List[MetricDefinition]]
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
            if (
                datapoints_for_metric.disaggregated_by_supervision_subsystems
                is not True
            ):
                # If this metric is NOT disaggeregated by supervision subsystem,
                # add it's metric definition to system_to_missing_monthly_metrics
                # as is
                system_to_missing_monthly_metrics[metric_definition.system].append(
                    metric_definition
                )
            else:
                _add_disaggregated_supervision_metrics(
                    agency=agency,
                    metric_definition=metric_definition,
                    system_to_missing_monthly_metrics=system_to_missing_monthly_metrics,
                )
        else:
            starting_month = (
                datapoints_for_metric.custom_reporting_frequency.starting_month or 1
            )
            if (
                datapoints_for_metric.disaggregated_by_supervision_subsystems
                is not True
            ):
                # If this metric is NOT disaggeregated by supervision subsystem,
                # add it's metric definition to
                # system_to_starting_month_to_missing_annual_metrics as is
                system_to_starting_month_to_missing_annual_metrics[
                    metric_definition.system
                ][starting_month].append(metric_definition)
            else:
                _add_disaggregated_supervision_metrics(
                    agency=agency,
                    metric_definition=metric_definition,
                    system_to_starting_month_to_missing_annual_metrics=system_to_starting_month_to_missing_annual_metrics,
                    starting_month=starting_month,
                )

    return (
        system_to_missing_monthly_metrics,
        system_to_starting_month_to_missing_annual_metrics,
    )


def _add_disaggregated_supervision_metrics(
    agency: schema.Agency,
    metric_definition: MetricDefinition,
    system_to_missing_monthly_metrics: Optional[
        Dict[schema.System, List[MetricDefinition]]
    ] = None,
    system_to_starting_month_to_missing_annual_metrics: Optional[
        Dict[schema.System, Dict[int, List[MetricDefinition]]]
    ] = None,
    starting_month: Optional[int] = None,
) -> None:
    """
    The metric definition passed-in is disaggeregated by supervision subsystem.
    This function adds the metric definitions of the supervision
    subsystems that the agency reports for to the corresponding missing
    metric dictionary.

    Args:
        agency (schema.Agency): The current agency.

        metric_definition (MetricDefinition): Metric definition of a metric
        disaggregated by supervision subsystem

        system_to_missing_monthly_metrics(Optional[Dict[schema.System, List[MetricDefinition]]]):
        Missing metric dictionary for the monthly report

        system_to_starting_month_to_missing_annual_metrics (Optional[
        Dict[schema.System, Dict[int, List[MetricDefinition]]]): Missing metric
        dictionary for annual metrics

        starting_month (Optional[int]): Starting month of the annual report

    """

    # If this metric IS disaggeregated by supervision subsystem,
    # add the metric definitions of the supervision subsystems that
    # the agency reports for to system_to_missing_monthly_metrics
    for system in [
        system
        for system in agency.systems
        if schema.System[system] in schema.System.supervision_subsystems()
    ]:
        supervision_subsystem_metric_definition = (
            get_supervision_subsystem_metric_definition(
                subsystem=system,
                supervision_metric_definition=metric_definition,
            )
        )
        if system_to_missing_monthly_metrics is not None:
            system_to_missing_monthly_metrics[schema.System[system]].append(
                supervision_subsystem_metric_definition
            )
        if (
            starting_month is not None
            and system_to_starting_month_to_missing_annual_metrics is not None
        ):
            system_to_starting_month_to_missing_annual_metrics[schema.System[system]][
                starting_month
            ].append(supervision_subsystem_metric_definition)
