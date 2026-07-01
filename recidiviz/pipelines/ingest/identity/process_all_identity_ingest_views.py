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
"""PTransform that processes all ingest views for the identity ingest pipeline."""
import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.entity.identity import identity_fragment_entities
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)
from recidiviz.pipelines.ingest.identity.types import SourcedIdentityFragment
from recidiviz.pipelines.ingest.raw_data_upper_bound_dates import (
    validate_and_backfill_raw_data_upper_bound_dates,
)
from recidiviz.pipelines.ingest.transforms.process_ingest_view import ProcessIngestView
from recidiviz.pipelines.ingest.types import ExternalIdKey, IngestViewName


class ProcessAllIdentityIngestViews(beam.PTransform):
    """PTransform that processes all ingest views for the identity ingest pipeline."""

    def __init__(self, pipeline_parameters: IdentityIngestPipelineParameters) -> None:
        super().__init__()
        self.pipeline_parameters = pipeline_parameters

    def expand(
        self, input_or_inputs: PBegin
    ) -> beam.PCollection[tuple[ExternalIdKey, SourcedIdentityFragment]]:
        """Discovers launchable identity views for the pipeline's tenant,
        materializes each view against raw data, parses rows into
        `IdentityFragment` trees, merges fragments sharing an external ID key
        and date within the same view, and flattens the per-view results into
        a single PCollection.

        Example output element::

            (
                ExternalIdKey(external_id="123", id_type="US_XX_DOC"),
                (
                    UpperBoundDate("2024-07-04T00:00:00.000000"),
                    IngestViewName("state_person"),
                    IdentityFragment(...),
                ),
            )

        Returns an empty PCollection when the tenant has no launchable identity
        views in the current project.
        """
        tenant = Tenant(self.pipeline_parameters.tenant)
        # v1 only supports tenants that are state codes; non-state tenants are
        # rejected by `IdentityIngestPipelineParameters.raw_data_input_dataset`.
        state_code = tenant.to_state_code()
        raw_data_source_instance = DirectIngestInstance(
            self.pipeline_parameters.raw_data_source_instance
        )
        raw_data_upper_bound_dates = self.pipeline_parameters.raw_data_upper_bound_dates

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )
        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=IdentityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        )
        ingest_view_context = IngestViewContentsContext.build_for_project(
            project_id=self.pipeline_parameters.project,
            is_sandbox=self.pipeline_parameters.is_sandbox_pipeline,
            state_code=state_code,
        )
        identity_view_names = ingest_manifest_collector.launchable_ingest_views(
            ingest_view_context
        )

        if not identity_view_names:
            return input_or_inputs | "No identity views to process" >> beam.Create([])

        view_collector = DirectIngestViewQueryBuilderCollector(
            region=region,
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
            expected_ingest_views=identity_view_names,
        )

        raw_data_upper_bound_dates = validate_and_backfill_raw_data_upper_bound_dates(
            state_code=state_code,
            region=region,
            view_collector=view_collector,
            view_names=identity_view_names,
            raw_data_upper_bound_dates=raw_data_upper_bound_dates,
        )

        merged_fragments_per_view: dict[
            IngestViewName,
            beam.PCollection[tuple[ExternalIdKey, SourcedIdentityFragment]],
        ] = {}
        for view_name in identity_view_names:
            view_query_builder = view_collector.get_query_builder_by_view_name(
                view_name
            )
            raw_data_tables_to_upperbound_dates = {
                file_tag: raw_data_upper_bound_dates[file_tag]
                for file_tag in view_query_builder.raw_data_table_dependency_file_tags
            }
            merged_fragments_per_view[
                view_name
            ] = input_or_inputs | view_name >> ProcessIngestView(
                state_code=state_code,
                ingest_view_name=view_name,
                ingest_view_query_builder=view_query_builder,
                raw_data_upper_bound_dates=raw_data_tables_to_upperbound_dates,
                raw_data_source_instance=raw_data_source_instance,
                ingest_view_manifest=ingest_manifest_collector.ingest_view_to_manifest[
                    view_name
                ],
                ingest_view_context=ingest_view_context,
                expected_root_entity_types=(IdentityFragment,),
                entities_module=identity_fragment_entities,
                output_dataset=None,
                ingest_view_results_only=False,
                should_throw_on_conflicts=True,
                resource_labels=self.pipeline_parameters.resource_labels,
            )

        return (
            merged_fragments_per_view.values()
            | "Flatten merged fragments" >> beam.Flatten()
        )
