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
from typing import Any, Dict, Iterable, Optional, Set, TypeVar

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.pipeline.utils.execution_utils import (
    select_all_by_person_query,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    json_serializable_metric_key,
)


@with_input_types(beam.typehints.Dict[str, Any], str)
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ConvertDictToKVTuple(beam.DoFn):
    """Converts a dictionary into a key value tuple by extracting a value from the dictionary and setting it as the
    key."""

    # pylint: disable=arguments-differ
    def process(self, element, key):
        if key not in element:
            raise ValueError(
                f"Dictionary element [{element}] does not contain expected key {key}."
            )

        key_value = element.get(key)

        if key_value:
            yield key_value, element

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(RecidivizMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class RecidivizMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    def process(self, element, *_args, **_kwargs):
        """The beam.io.WriteToBigQuery transform requires elements to be in dictionary form, where the values are in
        formats as required by BigQuery I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A RecidivizMetric

        Yields:
            A dictionary representation of the RecidivizMetric in the format Dict[str, Any] so that it can be written to
                BigQuery using beam.io.WriteToBigQuery.
        """
        element_dict = json_serializable_metric_key(element.__dict__)

        if isinstance(element, RecidivizMetric):
            yield beam.pvalue.TaggedOutput(element.metric_type.value, element_dict)
        else:
            raise ValueError(
                "Attempting to convert an object that is not a RecidivizMetric into a writable dict"
                "for BigQuery."
            )

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Any)
@with_output_types(beam.typehints.Dict[str, Any])
class ImportTable(beam.PTransform):
    """Reads in rows from the given dataset_id.table_id table in BigQuery. Returns each row as a dict."""

    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        state_code_filter: str,
        person_id_filter_set: Optional[Set[int]],
    ):
        super().__init__()
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.state_code_filter = state_code_filter
        self.person_id_filter_set = person_id_filter_set

    # pylint: disable=arguments-renamed
    def expand(self, pipeline: Pipeline):
        # Bring in the table from BigQuery
        table_query = select_all_by_person_query(
            self.dataset_id,
            self.table_id,
            self.state_code_filter,
            self.person_id_filter_set,
        )

        table_contents = (
            pipeline
            | f"Read {self.dataset_id}.{self.table_id} table from BigQuery"
            >> ReadFromBigQuery(query=table_query)
        )

        return table_contents


@with_input_types(beam.typehints.Any)
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ImportTableAsKVTuples(beam.PTransform):
    """Reads in rows from the given dataset_id.table_id table in BigQuery. Converts the output rows into key-value
    tuples, where the keys are the values for the self.table_key column in the table."""

    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        table_key: str,
        state_code_filter: str,
        person_id_filter_set: Optional[Set[int]],
    ):
        super().__init__()
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_key = table_key
        self.state_code_filter = state_code_filter
        self.person_id_filter_set = person_id_filter_set

    # pylint: disable=arguments-renamed
    def expand(self, pipeline: Pipeline):
        # Read in the table from BigQuery
        table_contents = (
            pipeline
            | f"Read {self.dataset_id}.{self.table_id} from BigQuery"
            >> ImportTable(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                state_code_filter=self.state_code_filter,
                person_id_filter_set=self.person_id_filter_set,
            )
        )

        # Convert the table rows into key-value tuples with the value for the self.table_key column as the key
        table_contents_as_kv = (
            table_contents
            | f"Convert {self.dataset_id}.{self.table_id} table to KV tuples"
            >> beam.ParDo(ConvertDictToKVTuple(), self.table_key)
        )

        return table_contents_as_kv


TypeToLift = TypeVar("TypeToLift")


@with_input_types(TypeToLift)
@with_output_types(TypeToLift)
class LiftToPCollectionElement(beam.DoFn):
    """Takes in the input and yields as an element in a PCollection. Does not manipulate
    the input in any way.

    Note: This is used when reading from BigQuery to avoid errors that we have
    encountered when passing output from BigQuery as a SideInput without yet
    processing it as an element in a PCollection.
    """

    # pylint: disable=arguments-differ
    def process(self, element: TypeToLift) -> Iterable[TypeToLift]:
        yield element

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


class ReadFromBigQuery(beam.PTransform):
    """Reads query results from BigQuery."""

    def __init__(self, query: str):
        super().__init__()
        self._query = query

    # pylint: disable=arguments-renamed
    def expand(self, pipeline: Pipeline):
        return (
            pipeline
            | "Read from BigQuery"
            >> beam.io.Read(
                beam.io.ReadFromBigQuery(
                    query=self._query, use_standard_sql=True, validate=True
                )
            )
            | "Process table rows as elements" >> beam.ParDo(LiftToPCollectionElement())
        )


class WriteAppendToBigQuery(beam.io.WriteToBigQuery):
    """Appends result rows to the given output BigQuery table."""

    def __init__(self, output_dataset: str, output_table: str):
        super().__init__(
            table=output_table,
            dataset=output_dataset,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        )
