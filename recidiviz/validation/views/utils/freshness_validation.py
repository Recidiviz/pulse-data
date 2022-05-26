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
""" Helpers for builder freshness validation views """
from typing import List

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder

# We only expect new data for a given day to be processed by ~10 am Pacific so if it is
# any earlier pretend it is the prior day.
from recidiviz.validation.views.case_triage.utils import MAX_DAYS_STALE


def current_date_fragment(*, is_postgres: bool = False) -> str:
    if is_postgres:
        return "DATE(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles' - INTERVAL '10 hours')"

    return "EXTRACT(DATE FROM TIMESTAMP_SUB(CURRENT_DATETIME('America/Los_Angeles'), INTERVAL 10 HOUR))"


def current_date_sub(*, days: int, is_postgres: bool = False) -> str:
    # Preferring - operator as it is both BQ/Postgres compatible
    return f"{current_date_fragment(is_postgres=is_postgres)} - {days}"


def current_date_add(*, days: int, is_postgres: bool = False) -> str:
    # Preferring + operator as it is both BQ/Postgres compatible
    return f"{current_date_fragment(is_postgres=is_postgres)} + {days}"


# Farthest in the future that data can be before it is excluded from the validation.
# This prevents any data far in the future from making the validation always pass.
MAX_DAYS_IN_FUTURE = 100


@attr.s(auto_attribs=True)
class FreshnessValidationAssertion:
    """Asserts that the most recent data for a column is no more than X days stale"""

    region_code: str
    assertion_name: str
    description: str
    source_data_query: str
    allowed_days_stale: int = attr.attrib(default=MAX_DAYS_STALE)

    def to_sql_query(self) -> str:
        """This query yields an array of dates that we expected to have data but did not"""
        query = f"""
            /* {self.description} */
            SELECT
                '{self.region_code}' as region_code,
                '{self.assertion_name}' as assertion,
                GENERATE_DATE_ARRAY(
                    DATE_ADD(source_data.latest_date, INTERVAL 1 DAY),
                    {current_date_sub(days=self.allowed_days_stale)}
                ) as failed_dates
            FROM (
                {self.source_data_query}
            ) AS source_data
        """

        return query

    @classmethod
    def build_cloudsql_connection_source_data_query(
        cls,
        *,
        location: str,
        connection: str,
        table: str,
        date_column_clause: str,
        filter_clause: str = "TRUE",
    ) -> str:
        """Returns a query that select the maximum value of the `date_column_clause` from a CloudSQL connection"""
        return f"""
        SELECT
            *
        FROM EXTERNAL_QUERY(
            "projects/{{project_id}}/locations/{location}/connections/{connection}",
            '''
                SELECT
                   MAX({date_column_clause}) AS latest_date
               FROM {table}
               WHERE
                   {date_column_clause} < {current_date_add(days=MAX_DAYS_IN_FUTURE, is_postgres=True)}
                   AND {filter_clause}
           '''
       )
       """

    @classmethod
    def build_bq_source_data_query(
        cls,
        *,
        dataset: str,
        table: str,
        date_column_clause: str,
        filter_clause: str = "TRUE",
    ) -> str:
        """Returns a query that select the maximum value of the `date_column_clause` from `dataset.table`"""
        return f"""
            SELECT
                MAX({date_column_clause}) AS latest_date
            FROM `{{project_id}}.{dataset}.{table}`
            WHERE
                {date_column_clause} < {current_date_add(days=MAX_DAYS_IN_FUTURE)}
                AND {filter_clause}
        """


@attr.s(auto_attribs=True)
class FreshnessValidation:
    """Produces a BigQueryViewBuilder that validates the given freshness assertions."""

    dataset: str
    view_id: str
    description: str
    assertions: List[FreshnessValidationAssertion]

    def __attrs_post_init__(self) -> None:
        # Every single assertion should have a unique statement
        assertion_statements = [
            assertion.assertion_name for assertion in self.assertions
        ]
        assert len(set(assertion_statements)) == len(assertion_statements)

    def build_query_template(self) -> str:
        """This query yields all the dates that failed an assertion and the list of
        assertions that failed for the given date.
        """
        assertion_queries = "\n UNION ALL \n".join(
            [assertion.to_sql_query() for assertion in self.assertions]
        )
        # This pivots the data from a list of dates that failed for each assertion to a
        # list of assertions that failed for each date.
        return f"""
            /* {self.description} */
            WITH assertions AS ( {assertion_queries} )
            SELECT
                region_code,
                failed_date,
                ARRAY_AGG(assertion) as failed_assertions
            FROM assertions, UNNEST(failed_dates) failed_date
            GROUP BY region_code, failed_date
        """

    def to_big_query_view_builder(self) -> SimpleBigQueryViewBuilder:
        return SimpleBigQueryViewBuilder(
            dataset_id=self.dataset,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.build_query_template(),
        )
