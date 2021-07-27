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
from typing import Any, List, Optional

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder


def current_date_sub(*, days: int) -> str:
    return f"DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"


def current_date_add(*, days: int) -> str:
    return f"DATE_ADD(CURRENT_DATE(), INTERVAL {days} DAY)"


@attr.s(auto_attribs=True)
class FreshnessValidationAssertion:
    region_code: str
    assertion: str
    description: str
    from_clause: str
    freshness_clause: Optional[str] = attr.ib(default=None)

    def to_sql_query(self) -> str:
        query = f"""
            /* {self.description} */   
            SELECT
                '{self.region_code}' as region_code,
                '{self.assertion}' as assertion,
                COUNT(*) > 0 as passed
            FROM {self.from_clause}
        """

        if self.freshness_clause:
            query += f"WHERE {self.freshness_clause}"

        return query

    @staticmethod
    def build_assertion(
        *, dataset: str, table: str, **kwargs: Any
    ) -> "FreshnessValidationAssertion":
        return FreshnessValidationAssertion(
            **kwargs, from_clause=f"`{{project_id}}.{dataset}.{table}`"
        )


@attr.s(auto_attribs=True)
class FreshnessValidation:
    dataset: str
    view_id: str
    description: str
    assertions: List[FreshnessValidationAssertion]

    def build_query_template(self) -> str:
        assertion_queries = "\n UNION ALL \n".join(
            [assertion.to_sql_query() for assertion in self.assertions]
        )
        return f"""
            /* {self.description} */
            WITH assertions AS ( {assertion_queries} )
            SELECT
                region_code,
                ARRAY_AGG(assertion) as failed_assertions
            FROM assertions
            WHERE passed = false
            GROUP BY region_code
        """

    def to_big_query_view_builder(self) -> SimpleBigQueryViewBuilder:
        return SimpleBigQueryViewBuilder(
            dataset_id=self.dataset,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.build_query_template(),
        )
