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
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, Tuple

import apache_beam as beam
from more_itertools import one

from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    to_string_value_converter,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.pipelines.ingest.state.constants import IngestViewName
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
    UPPER_BOUND_DATETIME_COL_NAME,
)


class GenerateEntities(beam.PTransform):
    """A PTransform that generates entities from ingest view results."""

    def __init__(
        self,
        state_code: str,
        ingest_instance: DirectIngestInstance,
        ingest_view_name: IngestViewName,
    ):
        super().__init__()

        self._state_code = state_code
        self._ingest_instance = ingest_instance
        self._ingest_view_name = ingest_view_name

    def expand(
        self, input_or_inputs: beam.PCollection[Dict[str, Any]]
    ) -> beam.PCollection[Tuple[datetime, RootEntity]]:
        return (
            input_or_inputs
            | f"Strip {self._ingest_view_name} rows of date metadata columns, returning in a tuple with the upper bound date."
            >> beam.Map(self.strip_off_dates)
            | f"Generate {self._ingest_view_name} entities."
            >> beam.MapTuple(self.generate_entity)
        )

    def strip_off_dates(
        self, element: Dict[str, Any]
    ) -> Tuple[datetime, Dict[str, str]]:
        """Generates a tuple of (upperbound_date, row_without_dates) from a row in the ingest view results."""
        upperbound_date = element[UPPER_BOUND_DATETIME_COL_NAME]

        row_without_date_metadata_cols = deepcopy(element)
        for column in ADDITIONAL_SCHEMA_COLUMNS:
            row_without_date_metadata_cols.pop(column.name)

        for key, value in row_without_date_metadata_cols.items():
            row_without_date_metadata_cols[key] = to_string_value_converter(key, value)

        return (upperbound_date, row_without_date_metadata_cols)

    def generate_entity(
        self, upperbound_date: datetime, row: Dict[str, str]
    ) -> Tuple[datetime, RootEntity]:
        region = direct_ingest_regions.get_direct_ingest_region(self._state_code)
        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=IngestViewResultsParserDelegateImpl(
                region=region,
                schema_type=SchemaType.STATE,
                ingest_instance=self._ingest_instance,
                results_update_datetime=upperbound_date,
            ),
        )

        entity = one(
            ingest_manifest_collector.manifest_parser.parse(
                ingest_view_name=self._ingest_view_name,
                contents_iterator=iter([row]),
            )
        )

        if not isinstance(entity, (StatePerson, StateStaff)):
            raise ValueError(f"Unexpected root entity type: {type(entity)}")

        return (upperbound_date, entity)