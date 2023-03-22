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

from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    fuzzy_match_against_options,
    get_column_value,
)
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
from recidiviz.persistence.database.schema.justice_counts import schema


class TimeRangeUploader:
    """Uploads rows from excel tables that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        agency_id: int,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
        user_account: schema.UserAccount,
        text_analyzer: TextAnalyzer,
        metricfile: MetricFile,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        metric_key_to_timerange_to_total_value: Dict[
            str, Dict[Tuple[datetime.date, datetime.date], Optional[int]]
        ],
    ) -> None:
        self.time_range = time_range
        self.user_account = user_account
        self.existing_datapoints_dict = existing_datapoints_dict
        self.agency_id = agency_id
        self.rows_for_this_time_range = rows_for_this_time_range
        self.text_analyzer = text_analyzer
        self.metricfile = metricfile
        self.metric_key_to_timerange_to_total_value = (
            metric_key_to_timerange_to_total_value
        )

    def upload_time_range(
        self,
        session: Session,
        time_range_to_year_month: Dict[
            Tuple[datetime.date, datetime.date], Tuple[int, int]
        ],
        reporting_frequency: schema.ReportingFrequency,
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
            report = ReportInterface.create_report(
                session=session,
                agency_id=self.agency_id,
                user_account_id=self.user_account.id,
                year=year,
                month=month,
                frequency=reporting_frequency.value,
            )

        report_metric = self._get_report_metric_for_time_range()

        datapoint_json_list = ReportInterface.add_or_update_metric(
            session=session,
            report=report,
            report_metric=report_metric,
            user_account=self.user_account,
            # TODO(#15499) Infer aggregate value only if total sheet was not provided.
            use_existing_aggregate_value=self.metricfile.disaggregation is not None,
            existing_datapoints_dict=self.existing_datapoints_dict,
        )
        return report, datapoint_json_list

    def _get_report_metric_for_time_range(
        self,
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
                    time_range=self.time_range,
                )
            row = self.rows_for_this_time_range[0]
            aggregate_value = get_column_value(
                row=row,
                column_name="value",
                column_type=float,
                time_range=self.time_range,
                analyzer=self.text_analyzer,
            )
            self.metric_key_to_timerange_to_total_value[self.metricfile.definition.key][
                self.time_range
            ] = aggregate_value
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
                value = get_column_value(
                    row=row,
                    column_name="value",
                    column_type=float,
                    analyzer=self.text_analyzer,
                )

                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = get_column_value(
                    row=row,
                    column_name=self.metricfile.disaggregation_column_name,
                    column_type=str,
                    analyzer=self.text_analyzer,
                )

                try:
                    matching_disaggregation_member = self.metricfile.disaggregation(disaggregation_value)  # type: ignore
                except ValueError:
                    # A ValueError will be thrown by the line above if the user-entered disaggregation
                    # value is not actually a member of the disaggreation enum. In that case, we fuzzy
                    # match against the enum members and try again.
                    disaggregation_options = [
                        member.value for member in self.metricfile.disaggregation  # type: ignore[attr-defined]
                    ]
                    disaggregation_value = fuzzy_match_against_options(
                        analyzer=self.text_analyzer,
                        text=disaggregation_value,
                        options=disaggregation_options,
                        category_name=self.metricfile.disaggregation_column_name.replace(
                            "_", " "
                        ).title(),
                        time_range=self.time_range,
                    )
                    matching_disaggregation_member = self.metricfile.disaggregation(
                        disaggregation_value
                    )  # type: ignore[call-arg]
                dimension_to_value[matching_disaggregation_member] = value  # type: ignore[index]

            aggregate_value = sum(
                val  # type: ignore[misc]
                for val in dimension_to_value.values()  # type: ignore[union-attr]
                if val is not None
            )

            # Check that the sum of the disaggregate values is equal to that of the aggregate
            previously_saved_aggregate_value = (
                self.metric_key_to_timerange_to_total_value.get(
                    self.metricfile.definition.key, {}
                ).get(self.time_range)
            )
            # if previously_saved_aggregate_value is None, a Missing Total Warning will be thrown
            # (don't need a warning for each breakdown row)
            if (
                previously_saved_aggregate_value is not None
                and previously_saved_aggregate_value != aggregate_value
            ):
                description = f"The sum of all values ({aggregate_value}) in the {self.metricfile.canonical_filename} sheet for {self.time_range[0].strftime('%m/%d/%Y')}-{self.time_range[1].strftime('%m/%d/%Y')} does not equal the total value provided in the aggregate sheet ({previously_saved_aggregate_value})."
                raise JusticeCountsBulkUploadException(
                    title="Breakdown Total Warning",
                    message_type=BulkUploadMessageType.WARNING,
                    description=description,
                )

        return MetricInterface(
            key=self.metricfile.definition.key,
            value=aggregate_value,
            contexts=[],
            aggregated_dimensions=[
                MetricAggregatedDimensionData(dimension_to_value=dimension_to_value)
            ]
            if dimension_to_value is not None
            else [],
        )
