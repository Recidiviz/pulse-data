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
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.pvalue import PBegin, PDone
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.pipelines.utils.beam_utils.clear_bq_table import (
    ClearBQTableWhenInputEmpty,
)


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


# TODO(apache/beam#20297): remove resource_labels kwarg once the labels applied to a
# dataflow job are automatically added to bq jobs.
class ReadFromBigQuery(beam.PTransform):
    """Reads query results from BigQuery."""

    def __init__(self, query: str, resource_labels: dict[str, str]):
        super().__init__()
        self._query = query
        self._resource_labels = resource_labels

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[Dict[str, Any]]:
        return input_or_inputs | "Read from BigQuery" >> beam.io.ReadFromBigQuery(
            query=self._query,
            use_standard_sql=True,
            validate=True,
            bigquery_job_labels=self._resource_labels,
        )


class WriteToBigQuery(beam.PTransform):
    """Writes rows to the given BigQuery table, clearing it first when a
    truncating write over empty input would otherwise leave a previous run's
    rows in place. Delegates the load to _WriteToBigQuerySink and the
    empty-input clear to ClearBQTableWhenInputEmpty."""

    def __init__(
        self,
        *,
        output_dataset: str,
        output_table: str,
        # Must be one of the values defined in beam.io.BigQueryDisposition
        write_disposition: str,
        schema: Optional[bigquery.TableSchema] = None,
    ):
        super().__init__()
        self.output_dataset = output_dataset
        self.output_table = output_table
        self.write_disposition = write_disposition
        self.schema = schema

    def expand(self, input_or_inputs: beam.PCollection[Dict[str, Any]]) -> PDone:
        if self.write_disposition == beam.io.BigQueryDisposition.WRITE_TRUNCATE:
            _ = (
                input_or_inputs
                | "Clear table when input empty"
                >> ClearBQTableWhenInputEmpty(
                    address=ProjectSpecificBigQueryAddress(
                        project_id=self._resolve_project_id(input_or_inputs),
                        dataset_id=self.output_dataset,
                        table_id=self.output_table,
                    )
                )
            )
        _ = input_or_inputs | "Write rows" >> _WriteToBigQuerySink(
            output_dataset=self.output_dataset,
            output_table=self.output_table,
            write_disposition=self.write_disposition,
            schema=self.schema,
        )
        return PDone(input_or_inputs.pipeline)

    def _resolve_project_id(self, input_or_inputs: beam.PCollection) -> str:
        """Returns the project the write targets, read from GoogleCloudOptions
        on the pipeline options the way Beam's own sink resolves an unqualified
        table's project."""
        project_id = input_or_inputs.pipeline.options.view_as(
            GoogleCloudOptions
        ).project
        if not project_id:
            raise ValueError(
                f"Cannot clear [{self.output_dataset}.{self.output_table}] before "
                f"a WRITE_TRUNCATE write: the pipeline options carry no project."
            )
        return project_id


class _WriteToBigQuerySink(beam.io.WriteToBigQuery):
    """Loads rows into the given BigQuery table via a FILE_LOADS job."""

    def __init__(
        self,
        *,
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
