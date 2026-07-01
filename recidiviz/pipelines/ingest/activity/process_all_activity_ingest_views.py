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
"""PTransform that processes all ingest views for the activity ingest pipeline"""
from typing import Dict, Tuple

import apache_beam as beam
from apache_beam.pvalue import PBegin, PDone

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EnumMappingManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.monitoring.ingest_enum_counter import emit_enum_mapping_heartbeat
from recidiviz.monitoring.providers import get_global_meter_provider
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity.entities import StatePerson, StateStaff
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.pipelines.ingest.activity.exemptions import (
    INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS,
)
from recidiviz.pipelines.ingest.activity.pipeline_parameters import (
    IngestPipelineParameters,
)
from recidiviz.pipelines.ingest.raw_data_upper_bound_dates import (
    validate_and_backfill_raw_data_upper_bound_dates,
)
from recidiviz.pipelines.ingest.transforms.process_ingest_view import ProcessIngestView
from recidiviz.pipelines.ingest.types import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)
from recidiviz.pipelines.utils.beam_utils.clear_bq_table import ClearBQTable


class ProcessAllActivityIngestViews(beam.PTransform):
    """PTransform that processes all ingest views for the activity ingest pipeline."""

    def __init__(self, pipeline_parameters: IngestPipelineParameters) -> None:
        super().__init__()
        self.pipeline_parameters = pipeline_parameters

    def expand(
        self, input_or_inputs: PBegin
    ) -> beam.PCollection[
        Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
    ]:
        """Processes all ingest views for the activity ingest pipeline, outputting
        Python entity trees that have been merged within the same ingest view and
        date.

        If the parameters specify that we should produce ingest view results only,
        those results will be persisted and this transform will return an empty
        PCollection.
        """
        raw_data_source_instance = DirectIngestInstance(
            self.pipeline_parameters.raw_data_source_instance
        )
        state_code = StateCode(self.pipeline_parameters.state_code.upper())
        raw_data_upper_bound_dates = self.pipeline_parameters.raw_data_upper_bound_dates
        ingest_view_context = IngestViewContentsContext.build_for_project(
            project_id=self.pipeline_parameters.project,
            is_sandbox=self.pipeline_parameters.is_sandbox_pipeline,
            state_code=state_code,
        )

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )

        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=ActivityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.ACTIVITY,
        )
        all_launchable_views = ingest_manifest_collector.launchable_ingest_views(
            ingest_view_context
        )
        view_collector = DirectIngestViewQueryBuilderCollector(
            region=region,
            ingest_pipeline_type=IngestPipelineType.ACTIVITY,
            expected_ingest_views=all_launchable_views,
        )

        ingest_views_to_run = self.pipeline_parameters.launchable_ingest_views_to_run(
            all_launchable_views
        )

        for ingest_view_name in ingest_views_to_run:
            if ingest_view_name not in all_launchable_views:
                raise ValueError(
                    f"Found invalid ingest view for {state_code}: {ingest_view_name}"
                )

        raw_data_upper_bound_dates = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=state_code,
            region=region,
            view_collector=view_collector,
            view_names=ingest_views_to_run,
            raw_data_upper_bound_dates=raw_data_upper_bound_dates,
        )

        if set(ingest_views_to_run) != set(all_launchable_views):
            _ = (
                input_or_inputs
                | "Clear skipped ingest view results"
                >> _ClearAllSkippedIngestViews(
                    project_id=self.pipeline_parameters.project,
                    all_launchable_views=all_launchable_views,
                    ingest_views_to_run=ingest_views_to_run,
                    ingest_view_output_dataset=self.pipeline_parameters.ingest_view_results_output,
                )
            )

        merged_root_entities_with_dates_per_ingest_view: Dict[
            IngestViewName,
            beam.PCollection[Tuple[ExternalIdKey, Tuple[UpperBoundDate, RootEntity]]],
        ] = {}
        for ingest_view_name in ingest_views_to_run:
            ingest_view_query_builder = view_collector.get_query_builder_by_view_name(
                ingest_view_name
            )
            raw_data_tables_to_upperbound_dates = {
                # Filter down to only the tags referenced by this view.
                file_tag: raw_data_upper_bound_dates[file_tag]
                for file_tag in ingest_view_query_builder.raw_data_table_dependency_file_tags
            }

            root_entities_with_dates = input_or_inputs | ingest_view_name >> ProcessIngestView(
                state_code=state_code,
                ingest_view_name=ingest_view_name,
                ingest_view_query_builder=ingest_view_query_builder,
                raw_data_upper_bound_dates=raw_data_tables_to_upperbound_dates,
                raw_data_source_instance=raw_data_source_instance,
                ingest_view_manifest=ingest_manifest_collector.ingest_view_to_manifest[
                    ingest_view_name
                ],
                ingest_view_context=ingest_view_context,
                expected_root_entity_types=(StatePerson, StateStaff),
                entities_module=state_entities,
                output_dataset=self.pipeline_parameters.ingest_view_results_output,
                ingest_view_results_only=self.pipeline_parameters.ingest_view_results_only,
                should_throw_on_conflicts=(
                    ingest_view_name
                    not in INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS.get(state_code, {})
                ),
                resource_labels=self.pipeline_parameters.resource_labels,
            )
            merged_root_entities_with_dates_per_ingest_view[
                ingest_view_name
            ] = root_entities_with_dates

        merged_root_entities_with_dates: beam.PCollection[
            Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
        ] = (merged_root_entities_with_dates_per_ingest_view.values() | beam.Flatten())

        # Emit counter.add(0) heartbeats for all enum fields so that the alert
        # policy sees rate=0 for fields with no unmapped values (rather than
        # absent data, which would leave existing alerts open). Chained after
        # the merged_root_entities_with_dates result so it runs at the very end
        # with a beam.combiners.Count.Globally that makes sure the
        # merged_root_entities_with_dates is completely hydrated before we fire
        # the heartbeats. Otherwise a zero-count flush could prematurely
        # resolve an active alert.
        if not self.pipeline_parameters.is_sandbox_pipeline:
            manifests_for_views_to_run = {
                name: ingest_manifest_collector.ingest_view_to_manifest[name]
                for name in ingest_views_to_run
            }
            _ = (
                merged_root_entities_with_dates
                | "Wait for all parsing to complete" >> beam.combiners.Count.Globally()
                | "Emit enum mapping heartbeats"
                >> beam.Map(
                    lambda _, sc=state_code.value, m=manifests_for_views_to_run: _emit_heartbeats_and_flush(
                        sc, m
                    ),
                )
            )

        return merged_root_entities_with_dates


def _emit_heartbeats_and_flush(
    state_code: str,
    manifests_for_views_to_run: dict[str, IngestViewManifest],
) -> None:
    """Emits counter.add(0) heartbeats for all enum fields, then flushes the
    meter provider to ensure metrics are exported before the worker shuts down."""
    for ingest_view_name, manifest in manifests_for_views_to_run.items():
        for entity_node in manifest.output.all_nodes_referenced_with_type(
            EntityTreeManifest
        ):
            for field_name, field_manifest in entity_node.field_manifests.items():
                for enum_manifest in field_manifest.all_nodes_referenced_with_type(
                    EnumMappingManifest
                ):
                    emit_enum_mapping_heartbeat(
                        state_code=state_code,
                        enum_cls=enum_manifest.result_type,
                        field_name=field_name,
                        ingest_view_name=ingest_view_name,
                    )
    get_global_meter_provider().force_flush()


class _ClearAllSkippedIngestViews(beam.PTransform):
    """PTransform that clears all the results of ingest views that would be run with a
    full run of this pipeline (i.e. the table in the us_xx_ingest_view_results dataset
    exists, but which will not be run as part of this pipeline run).
    """

    def __init__(
        self,
        project_id: str,
        all_launchable_views: list[str],
        ingest_views_to_run: list[str],
        ingest_view_output_dataset: str,
    ):
        super().__init__()
        self.project_id = project_id
        self.all_launchable_views = all_launchable_views
        self.ingest_views_to_run = ingest_views_to_run
        self.ingest_view_output_dataset = ingest_view_output_dataset

    def expand(self, input_or_inputs: PBegin) -> PDone:
        for ingest_view_name in self.all_launchable_views:
            if ingest_view_name not in self.ingest_views_to_run:
                _ = input_or_inputs | f"Clear {ingest_view_name}" >> ClearBQTable(
                    address=ProjectSpecificBigQueryAddress(
                        project_id=self.project_id,
                        dataset_id=self.ingest_view_output_dataset,
                        table_id=ingest_view_name,
                    ),
                )
        return PDone(input_or_inputs.pipeline)
