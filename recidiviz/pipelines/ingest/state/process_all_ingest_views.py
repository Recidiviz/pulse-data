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
"""Defines a PTransform that processes all ingest views for an ingest pipeline"""
from typing import Dict, Tuple

import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)
from recidiviz.pipelines.ingest.state.process_ingest_view import ProcessIngestView


class ProcessAllIngestViews(beam.PTransform):
    """A PTransform that processes all ingest views for an ingest pipeline, outputting
    Python entity trees that have been merged within the same ingest view and date.

    If the parameters specify that we should produce ingest view results only, those
    results will be persisted and this transform will return an empty PCollection.
    """

    def __init__(self, pipeline_parameters: IngestPipelineParameters):
        super().__init__()
        self.pipeline_parameters = pipeline_parameters

    def expand(
        self, input_or_inputs: PBegin
    ) -> beam.PCollection[
        Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
    ]:
        raw_data_source_instance = DirectIngestInstance(
            self.pipeline_parameters.raw_data_source_instance
        )
        state_code = StateCode(self.pipeline_parameters.state_code.upper())
        raw_data_upper_bound_dates = self.pipeline_parameters.raw_data_upper_bound_dates
        ingest_view_context = IngestViewContentsContext.build_for_project(
            project_id=self.pipeline_parameters.project
        )

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )

        all_raw_file_tags = DirectIngestRegionRawFileConfig(
            state_code.value, region_module=region.region_module
        ).raw_file_tags

        if unexpected_file_tags := set(raw_data_upper_bound_dates) - all_raw_file_tags:
            raise ValueError(
                f"Found unexpected file tags in raw_data_upper_bound_dates. These are "
                f"not valid raw file tags for [{state_code.value}]: "
                f"[{unexpected_file_tags}]. "
            )

        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )
        all_launchable_views = ingest_manifest_collector.launchable_ingest_views(
            ingest_view_context
        )
        view_collector = DirectIngestViewQueryBuilderCollector(
            region, all_launchable_views
        )

        ingest_views_to_run = self.pipeline_parameters.launchable_ingest_views_to_run(
            all_launchable_views
        )

        for ingest_view_name in ingest_views_to_run:
            if ingest_view_name not in all_launchable_views:
                raise ValueError(
                    f"Found invalid ingest view for {state_code}: {ingest_view_name}"
                )

            ingest_view_query_builder = view_collector.get_query_builder_by_view_name(
                ingest_view_name
            )

            raw_files_with_data = set(raw_data_upper_bound_dates)
            if dependencies_missing_data := (
                ingest_view_query_builder.raw_data_table_dependency_file_tags
                - raw_files_with_data
            ):
                raise ValueError(
                    f"Found dependency table(s) of ingest view [{ingest_view_name}] with no "
                    f"data: {dependencies_missing_data}"
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
                output_dataset=self.pipeline_parameters.ingest_view_results_output,
                ingest_view_results_only=self.pipeline_parameters.ingest_view_results_only,
                resource_labels=self.pipeline_parameters.resource_labels,
            )
            merged_root_entities_with_dates_per_ingest_view[
                ingest_view_name
            ] = root_entities_with_dates

        merged_root_entities_with_dates: beam.PCollection[
            Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
        ] = (merged_root_entities_with_dates_per_ingest_view.values() | beam.Flatten())
        return merged_root_entities_with_dates
