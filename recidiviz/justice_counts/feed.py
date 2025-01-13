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
"""Interface for working with public Justice Counts data feeds."""

import calendar
import itertools
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd
from flask import Response, make_response
from sqlalchemy.orm import Session

from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_METRIC_KEY_AND_DIM_ID_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.datapoint_utils import (
    get_dimension_id,
    get_dimension_member,
    is_datapoint_deprecated,
)

from .utils.date_utils import convert_date_range_to_year_month


class FeedInterface:
    """Contains methods for working with public data feeds."""

    @staticmethod
    def get_csv_of_feed(
        session: Session,
        agency_id: int,
        include_unpublished_data: bool = False,
        metric: Optional[str] = None,
        system: Optional[str] = None,
    ) -> Response:
        """Returns an agency's  data in csv form. Used by both a public and protected endpoint.
        For the public endoint, only published data is returned. For the protected endpoint, all data
        is returned."""
        rows = []

        system_to_filename_to_rows = FeedInterface.get_feed_for_agency_id(
            session,
            agency_id=agency_id,
            include_unpublished_data=include_unpublished_data,
        )

        if not system_to_filename_to_rows:
            feed_response = make_response("")
            feed_response.headers["Content-type"] = "text/plain"
            return feed_response

        if system is None:
            # If the agency has only provided for one system,
            # no need to specify `system` parameter
            system = list(system_to_filename_to_rows.items())[0][0]

        # Invalid state: metric parameter is present, but not system
        # Since some metrics are present in multiple systems, we can't
        # figure out which data to render
        if metric and not system:
            raise JusticeCountsServerError(
                code="justice_counts_bad_request",
                description="If the `metric` parameter is specified and the agency is "
                "multi-system, then you must also provide the `system` parameter.",
            )

        # Valid state: both metric and system parameters are present
        # Return plaintext csv of metric rows
        if system and metric:
            if (
                system in system_to_filename_to_rows
                and metric in system_to_filename_to_rows[system]
            ):
                rows = system_to_filename_to_rows[system][metric]
            else:
                rows = []

        for row in rows:
            if "month" in row:
                month_number = int(row["month"])
                row["month"] = calendar.month_name[month_number]

        df = pd.DataFrame.from_dict(rows)
        csv = df.to_csv(index=False)

        feed_response = make_response(csv)
        feed_response.headers["Content-type"] = "text/plain"
        return feed_response

    @staticmethod
    def get_feed_for_agency_id(
        session: Session, agency_id: int, include_unpublished_data: bool = False
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Generates the public feed (according to the Technical Specification)
        for a given agency. The format returned from this method is a dictionary
        of system name (because an agency can belong to multiple systems) to
        another dictionary of file name to the list of rows (each containing a
        single datapoint) that make up that file, e.g.:
        {
            "law_enforcement": {
                "arrests": [
                    {
                        "year": 2020,
                        "month": 3,
                        "value": 13,
                    },
                    ...
                ],
                "calls_for_service_by_type": [
                    {
                        "year": 2020,
                        "month": 1,
                        "value": 302,
                        "call_type": "EMERGENCY",
                    },
                    ...
                ],
                ...
            }
        }
        """
        # 1. First fetch all datapoints for this agency
        # TODO(#14626): Combine with data viz functionality.
        reports = ReportInterface.get_reports_by_agency_id(
            session=session,
            agency_id=agency_id,
            include_datapoints=True,
            published_only=include_unpublished_data is False,
        )
        datapoints = [
            d
            for d in itertools.chain(*[report.datapoints for report in reports])
            if d.context_key is None and is_datapoint_deprecated(d) is False
        ]

        # 2. Group the datapoints with the same system, metric key, and dimension together
        # e.g. datapoints for aggregate arrests should be in one group, and datapoints
        # for arrests by offense type in another, and datapoints for arrests by gender
        # in yet another. Each one of these groups will become their own file.
        system_metric_key_and_dim_id_to_datapoints = defaultdict(list)
        for datapoint in datapoints:
            metric_key = datapoint.metric_definition_key
            system = METRIC_KEY_TO_METRIC[metric_key].system
            dimension_id = get_dimension_id(datapoint=datapoint)
            system_metric_key_and_dim_id_to_datapoints[
                (system, metric_key, dimension_id)
            ].append(datapoint)

        # 3. For each <system, metric key, dimension> tuple, create a "file".
        # (i.e. list of rows, each containing a single datapoint for a point in time)
        system_to_filename_to_rows: Dict[
            str, Dict[str, List[Dict[str, Any]]]
        ] = defaultdict(dict)
        for (
            system,
            metric_key,
            dimension_id,
        ), datapoints in system_metric_key_and_dim_id_to_datapoints.items():
            rows = []
            metricfile = SYSTEM_METRIC_KEY_AND_DIM_ID_TO_METRICFILE[
                (system, metric_key, dimension_id)
            ]

            # 4. Group the datapoints by time range (reverse chronological order)
            datapoints_sorted_by_time_range = sorted(
                datapoints, key=lambda x: (x.start_date, x.end_date), reverse=True
            )
            datapoints_by_time_range = {
                k: list(v)
                for k, v in itertools.groupby(
                    datapoints_sorted_by_time_range,
                    key=lambda x: (x.start_date, x.end_date),
                )
            }

            for (
                start_date,
                end_date,
            ), time_range_datapoints in datapoints_by_time_range.items():
                # 5. Create a row for each datapoint and add to the file.
                year, month = convert_date_range_to_year_month(
                    start_date=start_date, end_date=end_date
                )
                for datapoint in time_range_datapoints:
                    row: Dict[str, Any] = {}
                    row["year"] = year
                    if month is not None:
                        row["month"] = month

                    if metricfile.disaggregation:
                        if metricfile.disaggregation_column_name is None:
                            raise ValueError(
                                "metricfile.disaggregation_column_name must be not None "
                                "if metricfile.disaggregation is specified"
                            )
                        row[
                            metricfile.disaggregation_column_name
                        ] = get_dimension_member(datapoint=datapoint)

                    row["value"] = datapoint.value
                    rows.append(row)

            system_to_filename_to_rows[system.value][
                metricfile.canonical_filename
            ] = rows

        return system_to_filename_to_rows
