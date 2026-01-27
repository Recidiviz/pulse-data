# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Builds a view that joins a collection of metric time periods with unit of analysis
to unit of observation periods assignment periods, returning one result row for every
metric time period where there is some overlap with an assignment span. If there are
multiple assignments that overlap with a metric period, multiple rows will be returned.
"""
import re
import textwrap
from enum import Enum

from tabulate import tabulate

from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_metric_assignment_sessions_materialized_table_address,
)
from recidiviz.aggregated_metrics.dataset_config import (
    UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.calculator.query.bq_utils import MAGIC_END_DATE, nonnull_end_date_clause
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.utils.string_formatting import fix_indent
from recidiviz.utils.types import assert_type


class MetricTimePeriodToAssignmentJoinType(Enum):
    """Describes the type of join logic we'll use to associate metric time periods
    with unit of assignment periods.
    """

    # If the PERIOD of assignment overlaps at all with the metric period, we count it.
    INTERSECTION = "INTERSECTION"

    # If the PERIOD of assignment overlaps at all with the metric period, we associate
    # the assignment with the metric period. Additionally, if the metric period overlaps
    # with the day *after* the assignments ends, we associate the assignment with the
    # metric period.
    INTERSECTION_EVENT_ATTRIBUTION = "INTERSECTION_EVENT_ATTRIBUTION"

    # If the ASSIGNMENT START DATE of assignment overlaps at all with the metric period,
    # we associate the assignment with the metric period.
    ASSIGNMENT = "ASSIGNMENT"


class AssignmentsByTimePeriodViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """Builds a view that joins a collection of metric time periods with unit of
    analysis to unit of observation periods assignment periods, returning one result row
    for every metric time period where there is some overlap with an assignment span. If
    there are multiple assignments that overlap with a metric period, multiple rows will
    be returned.
    """

    ASSIGNMENT_START_DATE_COLUMN_NAME = "assignment_start_date"
    ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME = "assignment_end_date_exclusive_nonnull"

    INTERSECTION_START_DATE_COLUMN_NAME = "intersection_start_date"
    INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME = (
        "intersection_end_date_exclusive_nonnull"
    )
    INTERSECTION_EVENT_ATTRIBUTION_START_DATE_COLUMN_NAME = (
        "intersection_event_attribution_start_date"
    )
    INTERSECTION_EVENT_ATTRIBUTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME = (
        "intersection_event_attribution_end_date_exclusive_nonnull"
    )
    ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME = (
        "assignment_is_first_day_in_population"
    )

    @classmethod
    def docstring_output_columns_to_descriptions(cls) -> dict[str, str]:
        """Returns a dictionary mapping a specific set of output columns that need more
        explanation in the docstring to their explanatory description.
        """
        return {
            cls.ASSIGNMENT_START_DATE_COLUMN_NAME: (
                "The start date (inclusive) of the full unit of observation to unit of "
                "analysis assignment period being associated with this metric time "
                "period."
            ),
            cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME: (
                "The end date (exclusive) of the full unit of observation to unit of "
                "analysis assignment period being associated with this metric time "
                "period. This field is always non-null. If the assignment is currently "
                f"valid (no end date), the end date has value {MAGIC_END_DATE}."
            ),
            MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN: (
                f"The start date (inclusive) for the metric time period. May come "
                f"before the {cls.ASSIGNMENT_START_DATE_COLUMN_NAME} if the assignment "
                f"starts in the middle of this metric period, or after if the "
                f"assignment spans multiple metric periods."
            ),
            MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN: (
                f"The end date (exclusive) for the metric time period. May come "
                f"after the {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME} if the "
                f"assignment ends in the middle of this metric period, or before if "
                f"the assignment spans multiple metric periods."
            ),
            cls.INTERSECTION_START_DATE_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the start date (inclusive) of the period of time "
                "where the assignment and metric periods overlap. This is the "
                "start date (inclusive) that should be used to determine if a span "
                "observation overlaps and should be counted when calculating a "
                "PeriodSpanAggregatedMetric."
            ),
            cls.INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the end date (exclusive) of the period of time where "
                "the assignment and metric periods overlap. This is the end date "
                "(exclusive) that should be used to determine if a span observation "
                "overlaps and should be counted when calculating a "
                "PeriodSpanAggregatedMetric. This field is always non-null."
            ),
            cls.INTERSECTION_EVENT_ATTRIBUTION_START_DATE_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the start date (inclusive) of the period of time "
                "where the assignment and metric periods overlap, with one day added "
                "past the assignment start if the assignment start date does not fall "
                "on the unit of observation's first day in the population. This is first date "
                "(inclusive) when an event observation would count towards this metric "
                "period when calculating a PeriodEventAggregatedMetric."
            ),
            cls.INTERSECTION_EVENT_ATTRIBUTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the end date (exclusive) of the period of time where "
                "the assignment and metric periods overlap, with one day added past "
                "the assignment end date (if that date still falls within the metric "
                "period). This is the day after the last date when an event "
                "observation would count towards this metric period when calculating a "
                "PeriodEventAggregatedMetric. This field is always non-null."
            ),
            cls.ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME: (
                f"A boolean column indicating whether `{cls.ASSIGNMENT_START_DATE_COLUMN_NAME}` is the "
                "first day in the unit of observation's population. This is used to "
                "determine whether the assignment should be counted on the start date "
                "or the day after the start date when calculating aggregated metrics."
            ),
        }

    def __init__(
        self,
        *,
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        time_period: MetricTimePeriodConfig,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> None:
        self.dataset_id = UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID
        self.view_id = self._build_view_id(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
            time_period=time_period,
        )
        self.population_type = population_type
        self.unit_of_observation_type = unit_of_observation_type
        self.unit_of_analysis_type = unit_of_analysis_type
        self.metric_time_period_to_assignment_join_type = (
            metric_time_period_to_assignment_join_type
        )
        self.time_period = time_period

        self.clustering_fields = MetricUnitOfObservation(
            type=self.unit_of_observation_type
        ).primary_key_columns_ordered

        self.view_query_template = self.build_assignments_by_time_period_query_template(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
            time_period=time_period,
        )
        self.projects_to_deploy = None
        self.materialized_address = self.build_materialized_address(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
            time_period=time_period,
        )

    @classmethod
    def build_materialized_address(
        cls,
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        time_period: MetricTimePeriodConfig,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> BigQueryAddress:
        return assert_type(
            cls._build_materialized_address(
                dataset_id=UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID,
                view_id=cls._build_view_id(
                    population_type=population_type,
                    unit_of_analysis_type=unit_of_analysis_type,
                    unit_of_observation_type=unit_of_observation_type,
                    metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
                    time_period=time_period,
                ),
                materialized_address_override=None,
                should_materialize=True,
            ),
            BigQueryAddress,
        )

    @staticmethod
    def _build_view_id(
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
        time_period: MetricTimePeriodConfig,
    ) -> str:
        population_name = population_type.population_name_short
        unit_of_observation_name = unit_of_observation_type.short_name
        unit_of_analysis_name = unit_of_analysis_type.short_name
        join_type = metric_time_period_to_assignment_join_type.value.lower()

        return (
            f"{population_name}"
            f"__{unit_of_observation_name}_to_{unit_of_analysis_name}"
            f"__by_{join_type}"
            f"__{assert_type(time_period.config_name, str)}"
        )

    def _reformat_multi_line_paragraph(self, s: str, new_width: int) -> str:
        """Takes a multi-line string that contains a single paragraph of text and
        formats it to have the given new width.
        """
        return textwrap.fill(re.sub(r"\s+", " ", s), width=new_width)

    @property
    def description(self) -> str:
        """Returns a description for this view"""
        unit_of_analysis = MetricUnitOfAnalysis.for_type(self.unit_of_analysis_type)
        unit_of_observation = MetricUnitOfObservation(
            type=self.unit_of_observation_type
        )

        output_columns = self.get_output_columns(
            unit_of_analysis,
            unit_of_observation,
            self.metric_time_period_to_assignment_join_type,
        )

        output_columns_to_document = [
            c
            for c in output_columns
            if c
            not in {
                *unit_of_analysis.primary_key_columns,
                *unit_of_observation.primary_key_columns,
                MetricTimePeriodConfig.METRIC_TIME_PERIOD_PERIOD_COLUMN,
            }
        ]
        columns_to_descriptions = self.docstring_output_columns_to_descriptions()

        if (
            self.metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION
        ):
            join_description = (
                "where there is some overlap with an assignment span (treating the end "
                "date of the assignment span as *inclusive*, not *exclusive*, and "
                "treating the start date as *exclusive* in cases where the unit of "
                "observation was not newly entering the population)"
            )
        elif (
            self.metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION
        ):
            join_description = "where there is some overlap with an assignment span"
        elif (
            self.metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.ASSIGNMENT
        ):
            join_description = "where an assignment span starts during that period"
        else:
            raise ValueError(
                f"Unexpected MetricTimePeriodToAssignmentJoinType "
                f"[{self.metric_time_period_to_assignment_join_type}]"
            )

        base_description = self._reformat_multi_line_paragraph(
            f"""Joins a collection of metric time periods with assignment periods that 
        associate [{self.unit_of_analysis_type.value}] unit of analysis to 
        [{self.unit_of_observation_type.value}] unit of observation assignment spans,
        returning one result row for every metric time period {join_description}. If 
        there are multiple assignments associated with a metric period, multiple 
        rows will be returned.""",
            new_width=100,
        )
        columns_str = tabulate(
            [[col, columns_to_descriptions[col]] for col in output_columns_to_document],
            headers=["Column", "Description"],
            tablefmt="github",
        )

        return f"{base_description}\n\nKey column descriptions:\n{columns_str}\n"

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            bq_description=self.description,
            view_query_template=self.view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            sandbox_context=sandbox_context,
        )

    @classmethod
    def additional_output_columns_for_join_type(
        cls,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> list[str]:
        """Returns the names of the columns output by a view builder with the given
        join type which are not present in all views.
        """
        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION
        ):
            return [
                cls.INTERSECTION_EVENT_ATTRIBUTION_START_DATE_COLUMN_NAME,
                cls.INTERSECTION_EVENT_ATTRIBUTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME,
                cls.ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME,
            ]

        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION
        ):
            return [
                cls.INTERSECTION_START_DATE_COLUMN_NAME,
                cls.INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME,
            ]

        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.ASSIGNMENT
        ):
            return []

        raise ValueError(
            f"Unexpected metric_time_period_to_assignment_join_type: "
            f"[{metric_time_period_to_assignment_join_type}]"
        )

    @classmethod
    def get_output_columns(
        cls,
        unit_of_analysis: MetricUnitOfAnalysis,
        unit_of_observation: MetricUnitOfObservation,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> list[str]:
        """Returns the names of the columns output by a view builder with the given
        unit of analysis, unit of observation, and join type.
        """
        columns = [*unit_of_observation.primary_key_columns_ordered]
        # There may be overlap between unit_of_analysis and unit_of_observation PKs -
        # only add non-overlapping columns.
        for column in unit_of_analysis.primary_key_columns:
            if column not in columns:
                columns.append(column)
        columns.extend(MetricTimePeriodConfig.query_output_columns())
        columns.extend(
            [
                cls.ASSIGNMENT_START_DATE_COLUMN_NAME,
                cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME,
            ],
        )
        columns.extend(
            cls.additional_output_columns_for_join_type(
                metric_time_period_to_assignment_join_type
            )
        )

        return columns

    @classmethod
    def get_date_output_column_clauses(cls, output_columns: list[str]) -> list[str]:
        """Returns all date output column clauses for the assignments by time period query."""
        output_column_clauses = []
        for output_column in output_columns:
            if output_column == cls.INTERSECTION_START_DATE_COLUMN_NAME:
                output_column_clauses.append(
                    f"{cls._intersection_start_date_clause()} AS "
                    f"{cls.INTERSECTION_START_DATE_COLUMN_NAME}"
                )
                continue
            if output_column == cls.INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME:
                output_column_clauses.append(
                    f"{cls._intersection_end_date_exclusive_nonnull_clause()} AS "
                    f"{cls.INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME}"
                )
                continue
            if (
                output_column
                == cls.INTERSECTION_EVENT_ATTRIBUTION_START_DATE_COLUMN_NAME
            ):
                output_column_clauses.append(
                    f"{cls._intersection_event_attribution_start_date_clause()} AS "
                    f"{cls.INTERSECTION_EVENT_ATTRIBUTION_START_DATE_COLUMN_NAME}"
                )
                continue
            if (
                output_column
                == cls.INTERSECTION_EVENT_ATTRIBUTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME
            ):
                output_column_clauses.append(
                    f"{cls._intersection_event_attribution_end_date_exclusive_nonnull_clause()} AS "
                    f"{cls.INTERSECTION_EVENT_ATTRIBUTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME}"
                )
                continue
            if output_column == cls.ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME:
                output_column_clauses.append(
                    f"{cls.ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME}"
                )
                continue
            output_column_clauses.append(output_column)

        return output_column_clauses

    @classmethod
    def _get_output_column_clauses(
        cls,
        unit_of_analysis: MetricUnitOfAnalysis,
        unit_of_observation: MetricUnitOfObservation,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> list[str]:
        """Returns output column clauses for the assignments by time period query."""
        output_columns = cls.get_output_columns(
            unit_of_analysis=unit_of_analysis,
            unit_of_observation=unit_of_observation,
            metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
        )
        return cls.get_date_output_column_clauses(output_columns)

    @classmethod
    def _intersection_start_date_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        intersection_start_date column.
        """
        return fix_indent(
            f"""
                GREATEST(
                    {cls.ASSIGNMENT_START_DATE_COLUMN_NAME},
                    {MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN}
                )
                """,
            indent_level=0,
        )

    @classmethod
    def _intersection_event_attribution_start_date_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        intersection_event_attribution_start_date column.
        """
        return fix_indent(
            f"""
                GREATEST(
                    IF(
                        {cls.ASSIGNMENT_IS_FIRST_DAY_IN_POPULATION_COLUMN_NAME},
                        {cls.ASSIGNMENT_START_DATE_COLUMN_NAME},
                        DATE_ADD(
                            {cls.ASSIGNMENT_START_DATE_COLUMN_NAME},
                            INTERVAL 1 DAY
                        )
                    ),
                    {MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN}
                )
                """,
            indent_level=0,
        )

    @classmethod
    def _intersection_event_attribution_end_date_exclusive_nonnull_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        intersection_event_attribution_end_date_exclusive_nonnull column.
        """
        return fix_indent(
            f"""
            LEAST(
                -- If an event observation occurs on the exclusive end date of an 
                -- assignment period we still want to count it. However, if the event 
                -- occurs on the exclusive end date of the metric period, we don't count
                -- it.
                {MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN},
                IF(
                    {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME} = '{MAGIC_END_DATE}',
                    {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME},
                    DATE_ADD(
                        {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME},
                        INTERVAL 1 DAY
                    )
                )
            )
            """,
            indent_level=0,
        )

    @classmethod
    def _intersection_end_date_exclusive_nonnull_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        intersection_end_date_exclusive_nonnull column.
        """
        return fix_indent(
            f"""
            LEAST(
                {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME},
                {MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN}
            )
            """,
            indent_level=0,
        )

    @classmethod
    def get_join_condition_clause(
        cls,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> str:
        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION
        ):
            return f"{cls._intersection_event_attribution_start_date_clause()} < {cls._intersection_event_attribution_end_date_exclusive_nonnull_clause()}"

        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.INTERSECTION
        ):
            return f"{cls._intersection_start_date_clause()} < {cls._intersection_end_date_exclusive_nonnull_clause()}"

        if (
            metric_time_period_to_assignment_join_type
            is MetricTimePeriodToAssignmentJoinType.ASSIGNMENT
        ):

            return (
                f"{MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN} <= {cls.ASSIGNMENT_START_DATE_COLUMN_NAME} "
                f"AND {MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN} > {cls.ASSIGNMENT_START_DATE_COLUMN_NAME}"
            )

        raise ValueError(
            f"Unexpected metric_time_period_to_assignment_join_type: [{metric_time_period_to_assignment_join_type}]"
        )

    @classmethod
    def build_assignments_by_time_period_query_template(
        cls,
        *,
        time_period: MetricTimePeriodConfig,
        population_type: MetricPopulationType,
        unit_of_observation_type: MetricUnitOfObservationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
    ) -> str:
        """Builds a query template (with project_id format argument) that joins a collection
        of metric time periods with assignment periods, returning one result row for every
        metric time period where there is some overlap with an assignment span. If there are
        multiple assignments that overlap with a metric period, multiple rows will be
        returned.
        """
        unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
        unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)
        assignment_sessions_address = (
            get_metric_assignment_sessions_materialized_table_address(
                population_type=population_type,
                unit_of_observation_type=unit_of_observation.type,
                unit_of_analysis_type=unit_of_analysis.type,
            )
        )

        columns = cls._get_output_column_clauses(
            unit_of_analysis,
            unit_of_observation,
            metric_time_period_to_assignment_join_type,
        )
        columns_str = ",\n".join(columns)
        join_clause = cls.get_join_condition_clause(
            metric_time_period_to_assignment_join_type
        )

        query_template = f"""
WITH
time_periods AS (
{fix_indent(time_period.build_query(), indent_level=4)}
),
assignment_sessions AS (
    SELECT
        * EXCEPT(assignment_date, end_date_exclusive),
        assignment_date AS {cls.ASSIGNMENT_START_DATE_COLUMN_NAME},
        {nonnull_end_date_clause("end_date_exclusive")} AS {cls.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME}
    FROM
        `{assignment_sessions_address.format_address_for_query_template()}`
)
SELECT
{fix_indent(columns_str, indent_level=4)}
FROM
    time_periods
JOIN
    assignment_sessions
ON
{fix_indent(join_clause, indent_level=4)}
"""
        return fix_indent(query_template, indent_level=0)
