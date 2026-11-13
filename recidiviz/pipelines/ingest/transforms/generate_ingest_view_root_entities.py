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
"""PTransform that generates root entity trees from a single ingest view."""
import apache_beam as beam
from apache_beam.pvalue import PBegin

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
from recidiviz.pipelines.ingest.transforms.generate_entities import GenerateEntities
from recidiviz.pipelines.ingest.transforms.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.types import UpperBoundDate
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class GenerateIngestViewRootEntities(beam.PTransform):
    """PTransform that materializes a single ingest view and parses its rows into
    root entity trees."""

    def __init__(
        self,
        *,
        ingest_view_name: str,
        ingest_view_query_builder: DirectIngestViewQueryBuilder,
        raw_data_upper_bound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        ingest_view_manifest: IngestViewManifest,
        ingest_view_context: IngestViewContentsContext,
        expected_root_entity_types: tuple[type[RootEntity], ...],
        output_dataset: str | None,
        ingest_view_results_only: bool,
        resource_labels: dict[str, str],
    ):
        super().__init__()
        if ingest_view_results_only and output_dataset is None:
            raise ValueError(
                f"`ingest_view_results_only=True` requires `output_dataset` to be "
                f"set (view [{ingest_view_name}])."
            )
        self.ingest_view_name = ingest_view_name
        self.ingest_view_query_builder = ingest_view_query_builder
        self.raw_data_upper_bound_dates = raw_data_upper_bound_dates
        self.raw_data_source_instance = raw_data_source_instance
        self.ingest_view_manifest = ingest_view_manifest
        self.ingest_view_context = ingest_view_context
        self.expected_root_entity_types = expected_root_entity_types
        self.output_dataset = output_dataset
        self.ingest_view_results_only = ingest_view_results_only
        self.resource_labels = resource_labels

    def expand(
        self, input_or_inputs: PBegin
    ) -> beam.PCollection[tuple[UpperBoundDate, RootEntity]]:
        """Materializes the view's query against raw data, optionally persists
        the raw query rows to ``output_dataset``, and parses rows into root
        entity trees tagged with the snapshot date they came from.

        When ``output_dataset is None`` the intermediate landing-table write is
        skipped. When ``ingest_view_results_only=True`` parsing is skipped and
        the returned collection is empty (the landing-table write still runs).

        Example element::

            (
                UpperBoundDate("2024-07-04T00:00:00.000000"),
                StatePerson(...),
            )
        """
        raw_data_tables_to_upperbound_dates = {
            # Narrow the pipeline's upper bound dates to only the tags this view
            # reads.
            file_tag: self.raw_data_upper_bound_dates[file_tag]
            for file_tag in self.ingest_view_query_builder.raw_data_table_dependency_file_tags
        }
        ingest_view_results = (
            input_or_inputs
            | f"Materialize {self.ingest_view_name} results"
            >> GenerateIngestViewResults(
                ingest_view_builder=self.ingest_view_query_builder,
                raw_data_tables_to_upperbound_dates=raw_data_tables_to_upperbound_dates,
                raw_data_source_instance=self.raw_data_source_instance,
                resource_labels=self.resource_labels,
            )
        )

        if self.output_dataset is not None:
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
            return input_or_inputs | "Skip entity generation" >> beam.Create([])

        return (
            ingest_view_results
            | f"Generate {self.ingest_view_name} entities"
            >> GenerateEntities(
                ingest_view_manifest=self.ingest_view_manifest,
                ingest_view_context=self.ingest_view_context,
                expected_root_entity_types=self.expected_root_entity_types,
            )
        )
