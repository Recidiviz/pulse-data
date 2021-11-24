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
CURRENT_DATE_FRAGMENT = 'EXTRACT(DATE FROM TIMESTAMP_SUB(CURRENT_DATETIME("America/Los_Angeles"), INTERVAL 10 HOUR))'


def current_date_sub(*, days: int) -> str:
    return f"DATE_SUB({CURRENT_DATE_FRAGMENT}, INTERVAL {days} DAY)"


def current_date_add(*, days: int) -> str:
    return f"DATE_ADD({CURRENT_DATE_FRAGMENT}, INTERVAL {days} DAY)"


# Farthest in the future that data can be before it is excluded from the validation.
# This prevents any data far in the future from making the validation always pass.
MAX_DAYS_IN_FUTURE = 100


@attr.s(auto_attribs=True)
class FreshnessValidationAssertion:
    """Asserts that the most recent data for a column is no more than X days stale"""

    region_code: str
    assertion_name: str
    description: str
    dataset: str
    table: str
    date_column_clause: str
    allowed_days_stale: int

    # Filters the data that is used when checking the assertions.
    # E.g. `state_code = 'US_XX'` can be used if we want to check freshness of data for
    # a single state from a table that contains data for multiple states.
    filter_clause: str = attr.ib(default="TRUE")

    def to_sql_query(self) -> str:
        """This query yields an array of dates that we expected to have data but did not"""
        query = f"""
            /* {self.description} */   
            SELECT
                '{self.region_code}' as region_code,
                '{self.assertion_name}' as assertion,
                GENERATE_DATE_ARRAY(
                    DATE_ADD(MAX({self.date_column_clause}), INTERVAL 1 DAY),
                    {current_date_sub(days=self.allowed_days_stale)}
                ) as failed_dates
            FROM `{{project_id}}.{self.dataset}.{self.table}`
            WHERE
                {self.date_column_clause} < {current_date_add(days=MAX_DAYS_IN_FUTURE)}
                AND {self.filter_clause}
        """

        return query


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
