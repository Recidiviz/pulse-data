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
"""A PTransform that loads the results of the provided query and returns each row
as the value in a tuple where the key is the value of |key_column_name| for that
row.
"""
from typing import Tuple

import apache_beam as beam

from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.persistence.entity.generate_primary_key import PrimaryKey
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    ConvertDictToKVTuple,
    ReadFromBigQuery,
)
from recidiviz.pipelines.utils.execution_utils import TableRow


class LoadQueryResultsKeyedByColumn(beam.PTransform):
    """A PTransform that loads the results of the provided query and returns each row
    as the value in a tuple where the key is the value of |key_column_name| for that
    row.
    """

    def __init__(
        self,
        *,
        key_column_name: str,
        query_name: str,
        query_provider: BigQueryQueryProvider,
    ) -> None:
        super().__init__()
        self.key_column_name = key_column_name
        self.query_name = query_name
        self.query = query_provider.get_query()

    def expand(
        self, input_or_inputs: beam.pvalue.PBegin
    ) -> beam.PCollection[Tuple[PrimaryKey, TableRow]]:
        reference_view_data = (
            input_or_inputs
            | f"Load [{self.query_name}] query results"
            >> ReadFromBigQuery(query=self.query)
            | f"Make [{self.query_name}] ({self.key_column_name}, row) tuples"
            >> beam.ParDo(ConvertDictToKVTuple(self.key_column_name))
        )

        return reference_view_data
