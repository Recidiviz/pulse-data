# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines a PTransform that clears the contents of the provided BQ table"""
import apache_beam as beam
from apache_beam.pvalue import PBegin, PDone

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class ClearBQTable(beam.PTransform):
    """PTransform that clears the contents of the provided BQ table"""

    def __init__(self, address: BigQueryAddress) -> None:
        super().__init__()
        self.address = address

    def expand(self, input_or_inputs: PBegin) -> PDone:
        return (
            input_or_inputs
            | beam.Create([])
            | WriteToBigQuery(
                output_dataset=self.address.dataset_id,
                output_table=self.address.table_id,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )
