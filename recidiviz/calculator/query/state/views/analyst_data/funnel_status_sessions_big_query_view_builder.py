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
"""View builder that can be used to represent spans of time over which a 
given unit of observation was in some status. The output of this view builder
illustrates the volume of units moving through different stages of a system.
"""
from typing import Literal

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.string_formatting import fix_indent


def _index_columns_join_clause(
    table_1_name: str, table_2_name: str, index_cols: list[str]
) -> str:
    return fix_indent(
        "\nAND ".join(
            [
                f"{table_1_name}.{col} = {table_2_name}.{col}"
                for col in sorted(index_cols)
            ]
        ),
        indent_level=8,
    )


@attr.define(frozen=True, kw_only=True)
class FunnelStatusSpanQueryBuilder:
    """Object for representing spans of time a set of statuses (as defined in
    status_cols_by_type) is true about a given unit of observation (as defined via
    index_cols).
    """

    # SQL query for pulling the spans. Must contain index_cols
    sql_source: str
    # The name of the start date column in sql_source
    start_date_col: str
    # The name of the end date exclusive column in sql_source
    end_date_exclusive_col: str
    # List of columns representing the level of granularity at which the
    # spans query in sql_source is constructed
    index_cols: list[str]
    # Map of output status attribute columns in the sql_source with their data type.
    status_cols_by_type: list[
        tuple[
            str,
            Literal[bigquery.enums.StandardSqlTypeNames.STRING]
            | Literal[bigquery.enums.StandardSqlTypeNames.BOOL]
            | Literal[bigquery.enums.StandardSqlTypeNames.INT64],
        ]
    ]

    # If True, build_query() will truncate each span that overlaps with provided reset
    # dates so that the end date is equal to the reset date.
    truncate_spans_at_reset_dates: bool = attr.ib(default=True)

    @property
    def status_cols(self) -> list[str]:
        return [cols_tuple[0] for cols_tuple in self.status_cols_by_type]

    def _query_template_join_clause(
        self, index_cols: list[str], funnel_reset_dates_sql_source: str
    ) -> str:
        if self.truncate_spans_at_reset_dates:
            status_cols_query_fragment = (
                " " + list_to_query_string(self.status_cols) + ", "
                if self.status_cols
                else ""
            )
            return f"""
    LEFT JOIN
        ({funnel_reset_dates_sql_source}) reset_dates
    ON
{_index_columns_join_clause("spans", "reset_dates", index_cols)}
        AND reset_dates.funnel_reset_date > spans.{self.start_date_col}
    -- Use the first funnel reset date following the span start as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {list_to_query_string(sorted(index_cols))},{status_cols_query_fragment}start_date 
        ORDER BY COALESCE(end_date_exclusive, "9999-01-01")
    ) = 1"""
        return ""

    def _query_template_end_date_clause(self) -> str:
        if self.truncate_spans_at_reset_dates:
            return f"""{revert_nonnull_end_date_clause(
            f'LEAST({nonnull_end_date_clause(f"spans.{self.end_date_exclusive_col}")}, {nonnull_end_date_clause("reset_dates.funnel_reset_date")})'
        )}"""
        return f"spans.{self.end_date_exclusive_col}"

    def build_query(
        self, index_cols: list[str], funnel_reset_dates_sql_source: str
    ) -> str:
        """Returns a query representing spans of time a set of statuses (as defined in
        status_cols_by_type) is true about a given unit of observation (as defined via
        index_cols).

        If truncate_spans_at_reset_dates is True, spans may be truncated so that they
        end on the reset dates specifiecd in the |funnel_reset_dates_sql_source|.

        Assumes that |funnel_reset_dates_sql_source| is a query that returns columns
        index_cols and a single funnel_reset_date column.
        """
        if len(set(self.status_cols)) != len(self.status_cols):
            raise ValueError(
                f"Status columns can not include duplicate names. Duplicate names "
                f"found in [{list_to_query_string(self.status_cols)}]"
            )

        status_columns_query_fragment = list_to_query_string(
            self.status_cols, table_prefix="spans"
        )

        return f"""
    SELECT
        -- Index columns
        {list_to_query_string(sorted(index_cols), table_prefix="spans")},
        spans.{self.start_date_col} AS start_date,
        {self._query_template_end_date_clause()} AS end_date_exclusive,
        -- Status columns
        {status_columns_query_fragment}
    FROM
        ({self.sql_source}) spans{self._query_template_join_clause(index_cols, funnel_reset_dates_sql_source)}"""


@attr.define(frozen=True, kw_only=True)
class FunnelStatusEventQueryBuilder:
    """Object for representing spans of time a set of statuses (as defined in
    status_cols_by_type) is true about a given unit of observation (as defined via
    index_cols), where those statuses change with point-in-time events.
    """

    # SQL query for pulling the span
    sql_source: str
    # The name of the event date column in sql_source
    event_date_col: str
    # List of columns representing the level of granularity at which the
    # spans query in sql_source is constructed
    index_cols: list[str]
    # Map of output status attribute columns in the sql_source with their data type.
    status_cols_by_type: list[
        tuple[
            str,
            Literal[bigquery.enums.StandardSqlTypeNames.STRING]
            | Literal[bigquery.enums.StandardSqlTypeNames.BOOL]
            | Literal[bigquery.enums.StandardSqlTypeNames.INT64],
        ]
    ]

    @property
    def status_cols(self) -> list[str]:
        return [cols_tuple[0] for cols_tuple in self.status_cols_by_type]

    def build_query(
        self, index_cols: list[str], funnel_reset_dates_sql_source: str
    ) -> str:
        """Returns a query representing spans of time a set of statuses (as defined in
        status_cols_by_type) is true about a given unit of observation (as defined via
        index_cols). All span start dates are derived from the event dates in
        |sql_source| and all end dates are derived from either a) the next following
        event or b) the funnel reset date, whichever comes first.
        """

        status_columns_query_fragment = list_to_query_string(
            self.status_cols, table_prefix="events"
        )
        status_cols_query_fragment_partition = (
            " " + list_to_query_string(self.status_cols) + ", "
            if self.status_cols
            else ""
        )

        return f"""
    SELECT
        -- Index columns
        {list_to_query_string(sorted(index_cols), table_prefix="events")},
        events.{self.event_date_col} AS start_date,
        reset_dates.funnel_reset_date AS end_date_exclusive,
        -- Status columns
        {status_columns_query_fragment}
    FROM
        ({self.sql_source}) events
    LEFT JOIN
        ({funnel_reset_dates_sql_source}) reset_dates
    ON
{_index_columns_join_clause("events", "reset_dates", index_cols)}
        AND reset_dates.funnel_reset_date > events.{self.event_date_col}
    -- Use the first funnel reset date following the event as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {list_to_query_string(sorted(index_cols))},{status_cols_query_fragment_partition}start_date 
        ORDER BY COALESCE(end_date_exclusive, "9999-01-01")
    ) = 1"""


class FunnelStatusSessionsViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that can be used to represent spans of time over which a
    given unit of observation was in some status. The output of this view builder
    illustrates the volume of units moving through different stages of a system.

    Example output columns:

    person_id | task_name | start_date | end_date_exclusive | eligible | viewed | form_started | form_submitted
    <---- index_cols ----><------ span dates --------------><------------- attribute columns ------------------>

    Attribute columns can be of type BOOL, STRING, or INT64. If there are overlaps in
    the input spans or events, the output will aggregate the statuses by taking a
    LOGICAL_OR over booleans, or a MAX over strings/integers.

    Attributes are capable of being overlapping, but are not guaranteed to be strict
    subsets of one another. For example, a person can be in both the "eligible" and
    "viewed" statuses at the same time, but it is not guaranteed that all "viewed"
    people are "eligible", even if this is the ordered sequence of input status events.
    """

    def __init__(
        self,
        # Name of the funnel status view
        view_id: str,
        # Description of the funnel status view
        description: str,
        # List of columns representing the level of granularity at which the
        # funnel should be constructed
        index_cols: list[str],
        # Source query for generating the set of dates that should reset all
        # funnel statuses to false (for booleans) or null (all other types).
        # Reset date column should be named `funnel_reset_date`.
        funnel_reset_dates_sql_source: str,
        # Builders for generating attribute spans that make up the funnel
        funnel_status_query_builders: list[
            FunnelStatusSpanQueryBuilder | FunnelStatusEventQueryBuilder
        ],
    ) -> None:

        for index, query_builder in enumerate(funnel_status_query_builders):
            if sorted(query_builder.index_cols) != sorted(index_cols):
                raise ValueError(
                    f"All funnel status queries must have the same index columns. Expected: [{list_to_query_string(index_cols, quoted=True)}], found: [{list_to_query_string(query_builder.index_cols, quoted=True)}] for item [{index}] in `funnel_status_query_builders`."
                )

        self.name = view_id
        self.description = description
        self.index_cols = index_cols
        self.funnel_reset_dates_sql_source = funnel_reset_dates_sql_source
        self.funnel_status_query_builders = funnel_status_query_builders

        super().__init__(
            dataset_id=ANALYST_VIEWS_DATASET,
            view_id=view_id,
            description=description,
            view_query_template=self._build_query_template(),
            should_materialize=True,
            clustering_fields=self.index_cols,
        )

    @property
    def span_funnel_status_queries(self) -> list[FunnelStatusSpanQueryBuilder]:
        return [
            b
            for b in self.funnel_status_query_builders
            if isinstance(b, FunnelStatusSpanQueryBuilder)
        ]

    @property
    def event_funnel_status_queries(self) -> list[FunnelStatusEventQueryBuilder]:
        """Returns the list of event funnel status queries."""
        return [
            b
            for b in self.funnel_status_query_builders
            if isinstance(b, FunnelStatusEventQueryBuilder)
        ]

    @property
    def output_columns(self) -> list[str]:
        """Returns the list of columns that will be output by this view."""
        return (
            self.index_cols
            + [
                "funnel_status_session_id",
                "date_gap_id",
                "start_date",
                "end_date_exclusive",
            ]
            + self.status_columns
        )

    def get_funnel_status_query_fragment_with_common_schema(
        self,
        funnel_status_query: FunnelStatusSpanQueryBuilder
        | FunnelStatusEventQueryBuilder,
    ) -> str:
        """
        Returns a query fragment that converts a funnel status query into the common
        schema required by the funnel status sessions view in order to properly union
        together all input funnel status queries. If a funnel status query contains a
        given status column, this function selects the column; otherwise, the column
        is filled with NULL. The output of this function is fed into
        create_sub_sessions_with_attributes to generate a query containing all status
        columns.
        """
        select_clauses: list[str] = []
        for status_col in self.status_columns:
            if status_col in funnel_status_query.status_cols:
                select_clauses.append(status_col)
            else:
                select_clauses.append(f"NULL AS {status_col}")
        select_columns_query_fragment = fix_indent(
            ",\n".join(select_clauses), indent_level=4
        )
        return f"""
SELECT
	{list_to_query_string(sorted(self.index_cols))},
    start_date,
    end_date_exclusive,
{select_columns_query_fragment}
FROM (
{funnel_status_query.build_query(index_cols=self.index_cols, funnel_reset_dates_sql_source=self.funnel_reset_dates_sql_source)}
)"""

    def _build_union_all_sessions_query_template(self) -> str:
        # Check for duplicate status names across input span/event queries
        if len(set(self.status_columns)) != len(self.status_columns):
            raise ValueError(
                f"Duplicate names found among input span and event funnel status queries: [{list_to_query_string(self.status_columns)}]"
            )

        subqueries = [
            self.get_funnel_status_query_fragment_with_common_schema(funnel_query)
            for funnel_query in self.span_funnel_status_queries
            + self.event_funnel_status_queries
        ]
        return fix_indent("\nUNION ALL\n".join(subqueries), indent_level=4)

    def _build_query_template(self) -> str:
        all_status_sessions_query_template = (
            self._build_union_all_sessions_query_template()
        )
        status_aggregations_query_fragment = fix_indent(
            ",\n".join(self.status_aggregation_query_fragments), indent_level=8
        )

        return f"""
WITH all_sessions AS (
    {all_status_sessions_query_template}
)
,
{create_sub_sessions_with_attributes(
    "all_sessions",
    index_columns=self.index_cols,
    end_date_field_name="end_date_exclusive"
)}
,
# Deduplicate across all sub sessions to generate a single span of time with boolean flags for each status.
sessions_dedup AS (
    SELECT
        {list_to_query_string(self.index_cols)},
        start_date,
        end_date_exclusive,
{status_aggregations_query_fragment},
    FROM
        sub_sessions_with_attributes
    GROUP BY
        {list_to_query_string(self.index_cols)},
        start_date,
        end_date_exclusive
)
{aggregate_adjacent_spans(
    table_name="sessions_dedup",
    index_columns=self.index_cols,
    attribute=self.status_columns,
    session_id_output_name="funnel_status_session_id",
    end_date_field_name="end_date_exclusive"
)}
"""

    @property
    def status_columns(
        self,
    ) -> list[str]:
        statuses = [
            status
            for query in self.funnel_status_query_builders
            for status in query.status_cols
        ]
        return statuses

    @property
    def status_aggregation_query_fragments(
        self,
    ) -> list[str]:
        all_status_aggregation_query_fragments = []
        for query in self.funnel_status_query_builders:
            for status, data_type in query.status_cols_by_type:
                if data_type == bigquery.StandardSqlTypeNames.BOOL:
                    all_status_aggregation_query_fragments.append(
                        f"COALESCE(LOGICAL_OR({status}), FALSE) AS {status}"
                    )
                if data_type in (
                    bigquery.StandardSqlTypeNames.STRING,
                    bigquery.StandardSqlTypeNames.INT64,
                ):
                    all_status_aggregation_query_fragments.append(
                        f"MAX({status}) AS {status}"
                    )
        return all_status_aggregation_query_fragments
