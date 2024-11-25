# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Utilities for generating and analyzing aggregated metrics in python notebooks
"""

import json
import os
from datetime import datetime
from typing import List, Optional

import pandas as pd

from recidiviz.aggregated_metrics.legacy.custom_aggregated_metrics_template import (
    get_legacy_custom_aggregated_metrics_query_template,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models import aggregated_metric_configurations as amc
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    EventMetricConditionsMixin,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string


def get_custom_aggregated_metrics(
    metrics: List[AggregatedMetric],
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    # TODO(#34770): Update these parameters to make it clear they're end date based
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
    rolling_period_unit: Optional[MetricTimePeriod] = None,
    rolling_period_length: Optional[int] = None,
    print_query_template: bool = False,
    project_id: str = "recidiviz-staging",
) -> pd.DataFrame:
    """Returns a dataframe consisting of all metrics for specified unit of analysis, population, and time periods"""
    # TODO(#29291): Migrate to use an optimized custom metrics template builder
    #  once it exists.
    query_template = get_legacy_custom_aggregated_metrics_query_template(
        metrics,
        unit_of_analysis_type,
        population_type,
        time_interval_unit,
        time_interval_length,
        min_date,
        max_date,
        rolling_period_unit,
        rolling_period_length,
        # strip the project id prefix from the query template, since this can not be read by pd.read_gbq
    ).replace("{project_id}.", "")
    if print_query_template:
        print(query_template)
    return pd.read_gbq(query_template, project_id=project_id, progress_bar_type="tqdm")


def get_event_attrs(ea: str, e: str) -> str:
    """Returns the value for the relevant event attribute"""

    try:
        event_attrs = json.loads(ea)
        if e in event_attrs:
            return event_attrs[e]
    except ValueError:
        pass  # If an error occurs, just continue to the default return statement

    return ""


def get_person_events(
    *,
    state_code: str,
    metrics: list[str],
    project_id: str = "recidiviz-staging",
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
    parse_attributes: bool = False,
    output_file_path: str = "",
    supervisor_id: str = "",
    officer_ids: Optional[list[str]] = None,
    print_query: bool = False,
) -> pd.DataFrame:
    """
    Returns a dataframe for all the person_events that contribute to a given metric in aggregated metrics, along with
    information on the attributes and officers associated with the event. Useful for data validation, particularly
    at the officer/supervisor level

    state_code: string to filter to relevant state
    metrics: list of event types we want to pull events for
    min_date: start of time range we want to pull events for
    max_date: end of time range we want to pull events for (inclusive)
    parse_attributes: boolean specifying whether the output df should parse `event_attributes`
    output_file_path: File path for output
    supervisor_id: If the events are being pulled for a set of officers associated with a supervisor, the ID can be
        included in the output name
    officer_ids: If the events are being pulled for a set of officers, provide their external ids

    Args:
        officer_ids (object):
    """

    if not metrics:
        raise ValueError("Must provide at least one metric - none provided.")
    # If filtering to specific officers, this formats the necessary filter for the query
    officer_ids_filter = ""
    if officer_ids:
        officer_ids_sql = list_to_query_string(officer_ids, quoted=True)
        officer_ids_filter = f"""
            AND officer_id IN ({officer_ids_sql})
        """

    min_date_str = f'"{min_date.strftime("%Y-%m-%d")}"'
    max_date_str = (
        f'"{max_date.strftime("%Y-%m-%d")}"'
        if max_date
        else 'CURRENT_DATE("US/Eastern")'
    )

    # Iterate through the provided list of metrics
    metric_dfs = []
    for metric_set_str in metrics:
        metric_set = getattr(amc, metric_set_str)
        # For a single metric, the output from  getattr should not be a list. But for a metric with some kind of
        # disaggregation (e.g. by violation type), the output will be a list
        if not isinstance(metric_set, list):
            metric_set = [metric_set]
        for metric in metric_set:
            if not isinstance(metric, EventMetricConditionsMixin):
                raise ValueError(
                    "Must be a metric related to events such as EventCountMetrics or AssignmentEvent Metric."
                )
            if not isinstance(metric, AggregatedMetric):
                raise ValueError("Must be an AggregatedMetric.")
            print(metric.name)

            # TODO(#29291): Update this query to use the observation-specific table for
            #  the metric.
            query = f"""
                    SELECT
                        e.state_code,
                        e.person_id,
                        pei.external_id,
                        e.event,
                        e.event_date,
                        e.event_attributes,
                        s.officer_id,
                        s.assignment_date,
                        s.end_date_exclusive,
                    FROM `observations__person_event.all_person_events_materialized` e
                    INNER JOIN `aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized` s
                        ON e.person_id = s.person_id
                        AND (e.event_date between s.assignment_date and COALESCE(s.end_date,'9999-01-01'))
                        AND e.state_code = '{state_code}' 
                    LEFT JOIN `normalized_state.state_person_external_id` pei
                        ON e.person_id = pei.person_id
                        AND e.state_code = pei.state_code
                    WHERE ({metric.get_observation_conditions_string(filter_by_observation_type=True, read_observation_attributes_from_json=True)})
                    {officer_ids_filter}
                    AND e.event_date BETWEEN {min_date_str} AND {max_date_str}

                """

            if print_query:
                print(query)

            metric_df = pd.read_gbq(
                query,
                project_id=project_id,
                progress_bar_type="tqdm_notebook",
            )
            metric_df["metric"] = metric.name

            keep_cols = [
                "person_id",
                "external_id",
                "event",
                "metric",
                "event_date",
                "officer_id",
            ]
            # Some wrangling if the output needs to separate out the event attributes
            if parse_attributes:
                try:
                    event_attributes_example = metric_df[
                        metric_df["event_attributes"] != "{}"
                    ].event_attributes.iloc[0]
                    event_attributes = json.loads(event_attributes_example).keys()
                    for e in event_attributes:
                        metric_df[e] = metric_df["event_attributes"].apply(
                            get_event_attrs, e=e
                        )
                except IndexError:
                    print("No attributes to parse")

            if not parse_attributes:
                metric_df = metric_df[keep_cols]

            metric_dfs.append(metric_df)

    # Concatenate the different dfs
    events = pd.concat(metric_dfs, ignore_index=True)

    # Tag duplicates that might occur across metrics (especially true for inferred starts)
    events["duplicated"] = events.duplicated(["external_id", "event_date"], keep=False)
    events = events.sort_values(by=["person_id", "metric"])

    if output_file_path != "":
        events.to_excel(
            os.path.join(output_file_path, f"metrics_events_{supervisor_id}.xlsx"),
            index=False,
        )

    return events
