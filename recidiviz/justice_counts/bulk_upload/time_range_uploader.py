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
"""Functionality for upload of spreadsheet rows for a certain time range into the Justice Counts database."""

import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.datapoint import DatapointUniqueKey
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import BREAKDOWN
from recidiviz.persistence.database.schema.justice_counts import schema


class TimeRangeUploader:
    """Uploads rows from excel tables that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        agency: schema.Agency,
        metadata: BulkUploadMetadata,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
        metricfile: MetricFile,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
    ) -> None:
        self.time_range = time_range
        self.metadata = metadata
        self.existing_datapoints_dict = existing_datapoints_dict
        self.agency = agency
        self.rows_for_this_time_range = rows_for_this_time_range
        self.metricfile = metricfile

    def upload_time_range(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        time_range_to_year_month: Dict[
            Tuple[datetime.date, datetime.date], Tuple[int, int]
        ],
        metric_key: str,
        existing_report: Optional[List[schema.Report]] = None,
    ) -> Tuple[schema.Report, List[DatapointJson]]:
        """Uploads rows for a certain time period and saves them in the JC database."""
        if existing_report is not None:
            if len(existing_report) != 1:
                raise ValueError(
                    f"Found {len(existing_report)} reports with time range {self.time_range}."
                )
            report = existing_report[0]
        else:  # existing report is None, so create the report
            year, month = time_range_to_year_month[self.time_range]
            reporting_frequency = ReportInterface.infer_reporting_frequency(
                self.time_range[0], self.time_range[1]
            )
            report = ReportInterface.create_report(
                session=session,
                agency_id=self.agency.id,
                year=year,
                month=month,
                frequency=reporting_frequency.value,
                user_account_id=(
                    self.metadata.user_account.id
                    if self.metadata.user_account is not None
                    else None
                ),
            )

        report_metric = self._get_report_metric_for_time_range(
            metric_key=metric_key,
        )

        datapoint_json_list = ReportInterface.add_or_update_metric(
            session=session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report,
            report_metric=report_metric,
            user_account=self.metadata.user_account,
            uploaded_via_breakdown_sheet=self.metricfile.disaggregation is not None,
            existing_datapoints_dict=self.existing_datapoints_dict,
            agency=self.agency,
            upload_method=self.metadata.upload_method,
        )

        if existing_report is None:
            # If a report was created and no errors were thrown while adding data,
            # set it to draft mode.
            report.status = schema.ReportStatus.DRAFT

        return report, datapoint_json_list

    def _get_report_metric_for_time_range(
        self,
        metric_key: str,
    ) -> MetricInterface:
        """Given a a set of rows from the CSV that all correspond to a single
        time period, convert the data in these rows to a MetricInterface object.
        If the metric associated with this CSV has no disaggregations, there
        should only be a single row for a single time period, and it contains
        the aggregate metric value. If the metric does have a disaggregation,
        there will be several rows, one with the value for each category.
        """
        aggregate_value = None
        dimension_to_value: Optional[Dict[DimensionBase, Optional[float]]] = (
            {d: None for d in self.metricfile.disaggregation}  # type: ignore[attr-defined]
            if self.metricfile.disaggregation is not None
            else None
        )

        # If this file represents a metric without disaggregations,
        # there should only be one row for a given time period.
        if self.metricfile.disaggregation is None:
            if len(self.rows_for_this_time_range) != 1:
                description = (
                    "There should only be a single row containing data "
                    f"for the time period {self.time_range[0].month}/{self.time_range[0].year}."
                )

                raise JusticeCountsBulkUploadException(
                    title="Too Many Rows",
                    description=description,
                    message_type=BulkUploadMessageType.ERROR,
                )
            row = self.rows_for_this_time_range[0]
            aggregate_value = (
                round(row["value"], 2)
                if isinstance(row["value"], float)
                else row["value"]
            )
            self.metadata.agency_name_to_metric_key_to_timerange_to_total_value[
                self.agency.name
            ][self.metricfile.definition.key][self.time_range] = aggregate_value
        else:  # metricfile.disaggregation is not None
            if self.metricfile.disaggregation_column_name is None:
                raise ValueError(
                    "`disaggregation` is not None but `disaggregation_column_name` is None"
                )
            for row in self.rows_for_this_time_range:
                # If this file represents a metric with a disaggregation,
                # there will likely be more than one row for a given time range;
                # there will be one row for each dimension value. Each will have
                # a value (i.e. the number or count) and a disaggregation value
                # (i.e. the category the count refers to, e.g. Male or Female).

                value = (
                    round(row["value"], 2)
                    if isinstance(row["value"], float)
                    else row["value"]
                )

                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = (
                    row[self.metricfile.disaggregation_column_name]
                    if self.metadata.is_single_page_upload is False
                    else row[BREAKDOWN]
                )

                matching_disaggregation_member = self.metricfile.disaggregation(disaggregation_value)  # type: ignore
                dimension_to_value[matching_disaggregation_member] = value  # type: ignore[index]

            aggregate_value = round(
                sum(
                    val  # type: ignore[misc]
                    for val in dimension_to_value.values()  # type: ignore[union-attr]
                    if val is not None
                ),
                2,  # Round the sum because of floating point arithmetic errors.
            )

            # Check that the sum of the disaggregate values is equal to that of the
            # aggregate and surface warning if the difference > 1.
            previously_saved_aggregate_value = (
                self.metadata.agency_name_to_metric_key_to_timerange_to_total_value.get(
                    self.agency.name, {}
                )
                .get(self.metricfile.definition.key, {})
                .get(self.time_range)
            )
            # if previously_saved_aggregate_value is None, a Missing Total Warning will be thrown
            # (don't need a warning for each breakdown row)
            if (
                previously_saved_aggregate_value is not None
                and abs(previously_saved_aggregate_value - aggregate_value) > 1
            ):
                description = f"The sum of all values ({aggregate_value}) in the {self.metricfile.canonical_filename} sheet for {self.time_range[0].strftime('%m/%d/%Y')}-{self.time_range[1].strftime('%m/%d/%Y')} does not equal the total value provided in the aggregate sheet ({previously_saved_aggregate_value})."
                breakdown_total_warning = JusticeCountsBulkUploadException(
                    title="Breakdown Total Warning",
                    message_type=BulkUploadMessageType.WARNING,
                    description=description,
                )
                self.metadata.metric_key_to_errors[metric_key].append(
                    breakdown_total_warning
                )

        return MetricInterface(
            key=self.metricfile.definition.key,
            value=aggregate_value,
            aggregated_dimensions=(
                [MetricAggregatedDimensionData(dimension_to_value=dimension_to_value)]
                if dimension_to_value is not None
                else []
            ),
        )
