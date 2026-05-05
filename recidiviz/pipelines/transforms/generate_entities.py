# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
from typing import Any, Dict, Tuple, Type

import apache_beam as beam
from dateutil.parser import isoparse

from recidiviz.common.str_field_utils import to_string_value_converter
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

# TODO(#75853): Move these constants to a shared location outside of the ingest
# pipeline's state/ package.
from recidiviz.pipelines.ingest.state.constants import (
    INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES,
    UpperBoundDate,
)


class GenerateEntities(beam.PTransform):
    """A PTransform that generates tuples of (upper_bound_date, RootEntity)
    from ingest view results rows."""

    def __init__(
        self,
        ingest_view_manifest: IngestViewManifest,
        ingest_view_context: IngestViewContentsContext,
        expected_root_entity_types: Tuple[Type[RootEntity], ...],
    ) -> None:
        super().__init__()
        self._ingest_view_manifest = ingest_view_manifest
        self._parser_context = ingest_view_context
        self._expected_root_entity_types = expected_root_entity_types

    def expand(
        self, input_or_inputs: beam.PCollection[Dict[str, Any]]
    ) -> beam.PCollection[Tuple[UpperBoundDate, RootEntity]]:
        return input_or_inputs | (
            f"Generate entity trees from "
            f"{self._ingest_view_manifest.ingest_view_name} results rows, "
            "returning a tuple with the upper bound date and entity tree."
        ) >> beam.Map(self._generate_entity_from_ingest_view_results)

    def _generate_entity_from_ingest_view_results(
        self, ingest_view_result_row: Dict[str, Any]
    ) -> Tuple[UpperBoundDate, RootEntity]:
        """Generates a tuple of (UpperBoundDate, RootEntity) from a row of
        ingest view results."""
        row_without_metadata = {
            key: to_string_value_converter(key, value)
            for key, value in ingest_view_result_row.items()
            if key not in INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES
        }
        entity = self._ingest_view_manifest.parse_row_into_entity(
            row=row_without_metadata,
            context=self._parser_context,
        )
        if not isinstance(entity, self._expected_root_entity_types):
            raise ValueError(
                f"Expected one of "
                f"{[t.__name__ for t in self._expected_root_entity_types]} "
                f"but got: {type(entity)}"
            )
        return (
            isoparse(ingest_view_result_row[UPPER_BOUND_DATETIME_COL_NAME]).timestamp(),
            entity,
        )
