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
from typing import Any, Dict, Tuple

import apache_beam as beam
from dateutil.parser import isoparse

from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.pipelines.ingest.state.constants import (
    INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES,
    UpperBoundDate,
)


def to_string_value_converter(field_name: str, value: Any) -> str:
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
    """
    A PTransform that generates tuples of (upper_bound_date, RootEntity)
    from ingest view results. The upper bound dates are needed to merge
    the root entities correctly downstream.
    """

    def __init__(
        self,
        ingest_view_manifest: IngestViewManifest,
        ingest_view_context: IngestViewContentsContext,
    ):
        super().__init__()

        self._ingest_view_manifest = ingest_view_manifest
        self._parser_context = ingest_view_context

    def expand(
        self, input_or_inputs: beam.PCollection[Dict[str, Any]]
    ) -> beam.PCollection[Tuple[UpperBoundDate, RootEntity]]:
        """Processes ingest_view_results into RootEntity objects"""
        return input_or_inputs | (
            f"Generate entity trees from {self._ingest_view_manifest.ingest_view_name} results rows, "
            "returning a tuple with the upper bound date and entity tree."
        ) >> beam.Map(self.generate_entity_tree_from_ingest_view_results)

    def generate_entity_tree_from_ingest_view_results(
        self, ingest_view_result_row: Dict[str, Any]
    ) -> Tuple[UpperBoundDate, RootEntity]:
        """
        Generates a tuple of (upperbound_date, RootEntity) from a row of ingest view results.
        """
        # Make sure to not modify the original element row!
        row_without_date_metadata_cols = {
            key: to_string_value_converter(key, value)
            for key, value in ingest_view_result_row.items()
            if key not in INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES
        }
        entity = self._ingest_view_manifest.parse_row_into_entity(
            row=row_without_date_metadata_cols,
            context=self._parser_context,
        )
        if not isinstance(entity, (StatePerson, StateStaff)):
            raise ValueError(f"Unexpected root entity type: {type(entity)}")
        return (
            isoparse(ingest_view_result_row[UPPER_BOUND_DATETIME_COL_NAME]).timestamp(),
            entity,
        )
