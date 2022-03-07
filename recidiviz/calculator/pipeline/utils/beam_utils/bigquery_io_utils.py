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
import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar

import apache_beam as beam
from apache_beam.pvalue import PBegin
from apache_beam.typehints import with_input_types, with_output_types


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

    def expand(self, input_or_inputs: PBegin):
        return (
            input_or_inputs
            | "Read from BigQuery"
            >> beam.io.Read(
                beam.io.ReadFromBigQuery(
                    query=self._query, use_standard_sql=True, validate=True
                )
            )
            | "Process table rows as elements" >> beam.ParDo(LiftToPCollectionElement())
        )


class WriteToBigQuery(beam.io.WriteToBigQuery):
    """Appends result rows to the given output BigQuery table."""

    def __init__(
        self,
        output_dataset: str,
        output_table: str,
        write_disposition: beam.io.BigQueryDisposition,
    ):
        super().__init__(
            table=output_table,
            dataset=output_dataset,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=write_disposition,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        )


def json_serializable_dict(
    metric_key: Dict[str, Any],
    list_serializer: Optional[Callable[[str, List[Any]], str]] = None,
) -> Dict[str, Any]:
    """Converts a dictionary into a format that is JSON serializable.

    For values that are of type Enum, converts to their raw values. For values
    that are dates, converts to a string representation.

    If any of the fields are list types, must provide a |list_serializer| which will
    handle serializing list values to a serializable string value.
    """
    serializable_dict: Dict[str, Any] = {}

    for key, v in metric_key.items():
        if isinstance(v, Enum) and v is not None:
            serializable_dict[key] = v.value
        elif isinstance(v, datetime.date) and v is not None:
            serializable_dict[key] = v.strftime("%Y-%m-%d")
        elif isinstance(v, list):
            if not list_serializer:
                raise ValueError(
                    "Must provide list_serializer if there are list "
                    f"values in dict. Found list in key: [{key}]."
                )

            serializable_dict[key] = list_serializer(key, v)
        else:
            serializable_dict[key] = v

    return serializable_dict
