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
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    MetricTimePeriodJoinType,
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


class AssignmentsByTimePeriodViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """Builds a view that joins a collection of metric time periods with unit of
    analysis to unit of observation periods assignment periods, returning one result row
    for every metric time period where there is some overlap with an assignment span. If
    there are multiple assignments that overlap with a metric period, multiple rows will
    be returned.
    """

    ASSIGNMENT_START_DATE_COLUMN_NAME = "assignment_start_date"
    ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME = "assignment_end_date_exclusive_nonnull"

    EVENT_APPLIES_TO_PERIOD_START_DATE_COLUMN_NAME = (
        "event_applies_to_period_start_date"
    )
    EVENT_APPLIES_TO_PERIOD_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME = (
        "event_applies_to_period_end_date_exclusive_nonnull"
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
            cls.EVENT_APPLIES_TO_PERIOD_START_DATE_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the first date (inclusive) when an event observation "
                "would count towards this metric period when calculating a "
                "PeriodEventAggregatedMetric."
            ),
            cls.EVENT_APPLIES_TO_PERIOD_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME: (
                "This column is pre-computed for use later in aggregated metrics "
                "queries. This is the day after the last date when an event "
                "observation would count towards this metric period when calculating a "
                "PeriodEventAggregatedMetric. This field is always non-null."
            ),
        }

    def __init__(
        self,
        *,
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        time_period: MetricTimePeriodConfig,
        metric_time_period_join_type: MetricTimePeriodJoinType,
    ) -> None:
        self.dataset_id = UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID
        self.view_id = self._build_view_id(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_join_type=metric_time_period_join_type,
            time_period=time_period,
        )
        self.population_type = population_type
        self.unit_of_observation_type = unit_of_observation_type
        self.unit_of_analysis_type = unit_of_analysis_type
        self.metric_time_period_join_type = metric_time_period_join_type
        self.time_period = time_period

        self.clustering_fields = MetricUnitOfObservation(
            type=self.unit_of_observation_type
        ).primary_key_columns_ordered

        self.view_query_template = self.build_assignments_by_time_period_query_template(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_join_type=metric_time_period_join_type,
            time_period=time_period,
        )
        self.projects_to_deploy = None
        self.materialized_address = self.build_materialized_address(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
            metric_time_period_join_type=metric_time_period_join_type,
            time_period=time_period,
        )

    @classmethod
    def build_materialized_address(
        cls,
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        time_period: MetricTimePeriodConfig,
        metric_time_period_join_type: MetricTimePeriodJoinType,
    ) -> BigQueryAddress:
        return assert_type(
            cls._build_materialized_address(
                dataset_id=UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID,
                view_id=cls._build_view_id(
                    population_type=population_type,
                    unit_of_analysis_type=unit_of_analysis_type,
                    unit_of_observation_type=unit_of_observation_type,
                    metric_time_period_join_type=metric_time_period_join_type,
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
        metric_time_period_join_type: MetricTimePeriodJoinType,
        time_period: MetricTimePeriodConfig,
    ) -> str:
        population_name = population_type.population_name_short
        unit_of_observation_name = unit_of_observation_type.short_name
        unit_of_analysis_name = unit_of_analysis_type.short_name
        join_type = metric_time_period_join_type.value.lower()

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
        base_description = self._reformat_multi_line_paragraph(
            f"""Joins a collection of [{self.time_period.period_name_type.value}] metric
        time periods with assignment periods that associate 
        [{self.unit_of_analysis_type.value}] unit of analysis to 
        [{self.unit_of_observation_type.value}] unit of observation assignment spans,
        returning one result row for every metric time period where there is some 
        overlap with an assignment span. If there are multiple assignments that overlap
        with a metric period, multiple rows will be returned.""",
            new_width=100,
        )
        columns_str = tabulate(
            [
                [col, descr]
                for col, descr in self.docstring_output_columns_to_descriptions().items()
            ],
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
            should_deploy_predicate=None,
        )

    @classmethod
    def _get_output_column_clauses(
        cls,
        unit_of_analysis: MetricUnitOfAnalysis,
        unit_of_observation: MetricUnitOfObservation,
    ) -> list[str]:
        """Returns output column clauses for the assignments by time period query."""
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

        columns.append(
            f"{cls._event_applies_to_period_start_date_clause()} AS {cls.EVENT_APPLIES_TO_PERIOD_START_DATE_COLUMN_NAME}"
        )
        columns.append(
            f"{cls._event_applies_to_period_end_date_exclusive_clause()} AS {cls.EVENT_APPLIES_TO_PERIOD_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME}"
        )

        # TODO(#35895): Add other useful calculated date fields if needed for PeriodSpanAggregatedMetric
        # TODO(#35897): Add other useful calculated date fields if needed for AssignmentEventAggregatedMetric
        # TODO(#35898): Add other useful calculated date fields if needed for AssignmentSpanAggregatedMetric

        return columns

    @classmethod
    def _event_applies_to_period_start_date_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        event_applies_to_period_start_date column.
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
    def _event_applies_to_period_end_date_exclusive_clause(cls) -> str:
        """Returns a SQL logic that gives us the value of the
        event_applies_to_period_end_date_exclusive_nonnull column.
        """
        return fix_indent(
            f"""
            LEAST(
                -- If an event observation occurs on the exclusive end date of an 
                -- assignment period we still want to count it. However, if the event 
                -- occurs on the exlcusive end date of the metric period, we don't count
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
    def _get_join_condition_clause(
        cls,
        metric_time_period_join_type: MetricTimePeriodJoinType,
    ) -> str:
        if metric_time_period_join_type is MetricTimePeriodJoinType.PERIOD:
            return f"{cls._event_applies_to_period_start_date_clause()} < {cls._event_applies_to_period_end_date_exclusive_clause()}"

        if metric_time_period_join_type is MetricTimePeriodJoinType.ASSIGNMENT:
            raise NotImplementedError(
                f"TODO(#35897), TODO(#35898): Build assignment periods join logic for Assignment* "
                f"type metrics ({metric_time_period_join_type})"
            )

        raise ValueError(
            f"Unexpected metric_time_period_join_type: [{metric_time_period_join_type}]"
        )

    @classmethod
    def build_assignments_by_time_period_query_template(
        cls,
        *,
        time_period: MetricTimePeriodConfig,
        population_type: MetricPopulationType,
        unit_of_observation_type: MetricUnitOfObservationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        metric_time_period_join_type: MetricTimePeriodJoinType,
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

        columns = cls._get_output_column_clauses(unit_of_analysis, unit_of_observation)
        columns_str = ",\n".join(columns)
        join_clause = cls._get_join_condition_clause(metric_time_period_join_type)

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
        `{{project_id}}.{assignment_sessions_address.to_str()}`
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
