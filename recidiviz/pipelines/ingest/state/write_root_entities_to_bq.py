# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A PTransform that, given a collection of RootEntity objects, translates each to a
collection of BigQuery table rows and persists those rows to BigQuery.
"""

from types import ModuleType
from typing import Any, Iterable

import apache_beam as beam

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.pipelines.ingest.state.serialize_entities import SerializeEntities
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class WriteRootEntitiesToBQ(beam.PTransform):
    """A PTransform that, given a collection of RootEntity objects, translates each to a
    collection of BigQuery table rows and persists those rows to BigQuery.
    """

    def __init__(
        self,
        state_code: StateCode,
        output_dataset: str,
        output_table_ids: Iterable[str],
        entities_module: ModuleType,
    ) -> None:
        super().__init__()
        self.output_dataset = output_dataset
        self.output_table_ids = list(output_table_ids)
        self.state_code = state_code
        self.entities_module = entities_module
        self.entities_module_context = entities_module_context_for_module(
            entities_module
        )

    def expand(self, input_or_inputs: beam.PCollection[RootEntity]) -> None:
        final_entities: beam.PCollection[dict[str, Any]] = input_or_inputs | (
            "Serialize entities to table rows"
            >> beam.ParDo(
                SerializeEntities(
                    state_code=self.state_code,
                    entities_module_context=self.entities_module_context,
                )
            ).with_outputs(*self.output_table_ids)
        )

        for table_id in self.output_table_ids:
            _ = getattr(
                final_entities, table_id
            ) | f"Write {table_id} to BigQuery" >> WriteToBigQuery(
                output_dataset=self.output_dataset,
                output_table=table_id,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
