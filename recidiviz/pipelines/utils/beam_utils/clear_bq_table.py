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

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl


def _delete_all_rows_from_table(address: ProjectSpecificBigQueryAddress) -> None:
    """Deletes all rows from a BQ table via a DELETE query.

    Uses BigQueryClient directly rather than WriteToBigQuery with
    WRITE_TRUNCATE, because Beam's FILE_LOADS method does not submit a load job
    (and therefore never applies WRITE_TRUNCATE) when zero elements flow to the
    table.
    """
    bq_client = BigQueryClientImpl(project_id=address.project_id)
    query_job = bq_client.run_query_async(
        query_str=f"DELETE FROM `{address.to_str()}` WHERE TRUE",
        use_query_cache=False,
    )
    query_job.result()


class ClearBQTable(beam.PTransform):
    """PTransform that clears the contents of the provided BQ table.

    Accepts either PBegin (runs immediately) or a PCollection (waits for all
    upstream elements before clearing).
    """

    def __init__(self, address: ProjectSpecificBigQueryAddress) -> None:
        super().__init__()
        self.address = address

    def expand(self, input_or_inputs: PBegin | beam.PCollection) -> PDone:
        if isinstance(input_or_inputs, PBegin):
            trigger = input_or_inputs | beam.Create([self.address])
        else:
            trigger = (
                input_or_inputs
                | beam.combiners.Count.Globally()
                | beam.Map(lambda _: self.address)
            )
        _ = trigger | beam.Map(_delete_all_rows_from_table)
        return PDone(input_or_inputs.pipeline)
