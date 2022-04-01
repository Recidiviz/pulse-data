# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Class that provides access to the results of the provided BigQuery query."""

from typing import Any, Callable, Dict, Iterator

from google.cloud.bigquery.job.query import QueryJob
from google.cloud.bigquery.table import Row

from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.utils.types import T


def no_op_value_converter(
    field_name: str,  # pylint: disable=unused-argument
    value: Any,
) -> Any:
    return value


FieldName = str
FieldValue = Any


class BigQueryResultsContentsHandle(ContentsHandle[Dict[FieldName, T]]):
    """Class that provides access to the results of the provided BigQuery query. If a
    |value_converter| is provided, all result values will be converted before return
    according to the converter function.
    """

    def __init__(
        self,
        query_job: QueryJob,
        value_converter: Callable[[FieldName, FieldValue], T] = no_op_value_converter,
    ):
        self.value_converter = value_converter
        self.query_job = query_job

    def get_contents_iterator(self) -> Iterator[Dict[str, T]]:
        """Returns an iterator over the results of this query, formatted as dictionaries
        of key-value pairs.
        """
        for row in self.query_job:
            if not isinstance(row, Row):
                raise ValueError(f"Found unexpected type for row: [{type(row)}].")
            yield {key: self.value_converter(key, row.get(key)) for key in row.keys()}
