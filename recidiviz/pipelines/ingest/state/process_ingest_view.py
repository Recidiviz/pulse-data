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
"""Defines a PTransform that processes a single ingest view"""

from typing import Dict, Tuple

import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)
from recidiviz.pipelines.ingest.state.generate_entities import GenerateEntities
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class ProcessIngestView(beam.PTransform):
    """A PTransform that processes a single ingest view, persisting query output rows
    to the ingest view results dataset, and outputting Python entity trees that have
    been merged within the same update_datetime.

    If ingest_view_results_only is True, the raw ingest view results will be persisted
    to BQ and this transform will return an empty PCollection.
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        ingest_view_name: str,
        ingest_view_query_builder: DirectIngestViewQueryBuilder,
        raw_data_upper_bound_dates: Dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        ingest_view_manifest: IngestViewManifest,
        ingest_view_context: IngestViewContentsContext,
        output_dataset: str,
        ingest_view_results_only: bool,
        resource_labels: dict[str, str],
    ):
        super().__init__()
        self.state_code = state_code
        self.ingest_view_name = ingest_view_name
        self.ingest_view_query_builder = ingest_view_query_builder
        self.raw_data_upper_bound_dates = raw_data_upper_bound_dates
        self.raw_data_source_instance = raw_data_source_instance
        self.ingest_view_manifest = ingest_view_manifest
        self.ingest_view_context = ingest_view_context
        self.output_dataset = output_dataset
        self.ingest_view_results_only = ingest_view_results_only
        self.resource_labels = resource_labels

    def expand(
        self, input_or_inputs: PBegin
    ) -> beam.PCollection[
        Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
    ]:
        ingest_view_results = (
            input_or_inputs
            | f"Materialize {self.ingest_view_name} results"
            >> GenerateIngestViewResults(
                ingest_view_builder=self.ingest_view_query_builder,
                raw_data_tables_to_upperbound_dates=self.raw_data_upper_bound_dates,
                raw_data_source_instance=self.raw_data_source_instance,
                resource_labels=self.resource_labels,
            )
        )

        _ = (
            ingest_view_results
            | f"Write {self.ingest_view_name} results to table"
            >> WriteToBigQuery(
                output_dataset=self.output_dataset,
                output_table=self.ingest_view_name,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        if self.ingest_view_results_only:
            # Return an empty PCollection if we are only generating ingest view results
            return input_or_inputs | "Skip entity generation" >> beam.Create([])

        return (
            ingest_view_results
            | f"Generate {self.ingest_view_name} entities"
            >> GenerateEntities(
                ingest_view_manifest=self.ingest_view_manifest,
                ingest_view_context=self.ingest_view_context,
            )
            | f"Merge {self.ingest_view_name} entities"
            >> MergeIngestViewRootEntityTrees(
                ingest_view_name=self.ingest_view_name,
                state_code=self.state_code,
            )
        )
