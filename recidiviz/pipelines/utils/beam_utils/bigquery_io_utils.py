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
"""Utils for beam calculations."""
# pylint: disable=abstract-method,redefined-builtin
from typing import Any, Dict, Generator, Optional, Tuple

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.pvalue import PBegin
from apache_beam.typehints import with_input_types, with_output_types


@with_input_types(beam.typehints.Dict[str, Any])
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ConvertDictToKVTuple(beam.DoFn):
    """Converts a dictionary into a key value tuple by extracting a value from the
    dictionary and setting it as the key.
    """

    def __init__(self, key_name: str) -> None:
        super().__init__()
        self.key_name = key_name

    # pylint: disable=arguments-differ
    def process(
        self, element: Dict[str, Any]
    ) -> Generator[Tuple[Any, Dict[str, Any]], None, None]:
        if self.key_name not in element:
            raise ValueError(
                f"Dictionary element [{element}] does not contain expected key "
                f"[{self.key_name}]."
            )

        yield element[self.key_name], element


class ReadFromBigQuery(beam.PTransform):
    """Reads query results from BigQuery."""

    def __init__(self, query: str):
        super().__init__()
        self._query = query

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[Dict[str, Any]]:
        return input_or_inputs | "Read from BigQuery" >> beam.io.ReadFromBigQuery(
            query=self._query, use_standard_sql=True, validate=True
        )


class WriteToBigQuery(beam.io.WriteToBigQuery):
    """Appends result rows to the given output BigQuery table."""

    def __init__(
        self,
        output_dataset: str,
        output_table: str,
        # Must be one of the values defined in beam.io.BigQueryDisposition
        write_disposition: str,
        schema: Optional[bigquery.TableSchema] = None,
    ):
        super().__init__(
            table=output_table,
            dataset=output_dataset,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=write_disposition,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            schema=schema,
        )
