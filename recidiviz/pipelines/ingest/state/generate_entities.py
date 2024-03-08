# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A PTransform that generates entities from ingest view results."""
import datetime
from copy import deepcopy
from typing import Any, Dict, Tuple

import apache_beam as beam
from dateutil import parser
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.pipelines.ingest.state.constants import UpperBoundDate
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
)


def to_string_value_converter(
    field_name: str,
    value: Any,
) -> str:
    """Converts all values to strings."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int)):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()

    raise ValueError(
        f"Unexpected value type [{type(value)}] for field [{field_name}]: {value}"
    )


class GenerateEntities(beam.PTransform):
    """A PTransform that generates entities from ingest view results."""

    def __init__(
        self,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        ingest_view_manifest: IngestViewManifest,
    ):
        super().__init__()

        self._state_code = state_code
        self._ingest_instance = ingest_instance
        self._ingest_view_manifest = ingest_view_manifest
        self._parser_context = IngestViewContentsContextImpl(
            ingest_instance=self._ingest_instance,
        )

    def expand(
        self, input_or_inputs: beam.PCollection[Dict[str, Any]]
    ) -> beam.PCollection[Tuple[UpperBoundDate, RootEntity]]:
        return (
            input_or_inputs
            | f"Strip {self._ingest_view_manifest.ingest_view_name} rows of date metadata columns, returning in a tuple with the upper bound date."
            >> beam.Map(self.strip_off_dates)
            | f"Generate {self._ingest_view_manifest.ingest_view_name} entities."
            >> beam.MapTuple(self.generate_entity)
        )

    def strip_off_dates(
        self, element: Dict[str, Any]
    ) -> Tuple[UpperBoundDate, Dict[str, str]]:
        """Generates a tuple of (upperbound_date, row_without_dates) from a row in the ingest view results."""
        upperbound_date = parser.isoparse(
            element[UPPER_BOUND_DATETIME_COL_NAME]
        ).timestamp()

        row_without_date_metadata_cols = deepcopy(element)
        for column in ADDITIONAL_SCHEMA_COLUMNS:
            row_without_date_metadata_cols.pop(column.name)

        for key, value in row_without_date_metadata_cols.items():
            row_without_date_metadata_cols[key] = to_string_value_converter(key, value)

        return (upperbound_date, row_without_date_metadata_cols)

    def generate_entity(
        self, upperbound_date: UpperBoundDate, row: Dict[str, str]
    ) -> Tuple[UpperBoundDate, RootEntity]:
        entity = one(
            self._ingest_view_manifest.parse_contents(
                contents_iterator=iter([row]),
                context=self._parser_context,
            )
        )

        if not isinstance(entity, (StatePerson, StateStaff)):
            raise ValueError(f"Unexpected root entity type: {type(entity)}")

        return (upperbound_date, entity)
