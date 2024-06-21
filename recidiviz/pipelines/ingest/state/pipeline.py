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
"""The ingest pipeline. See recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py for details
on how to launch a local run."""
from typing import Any, Dict, Set, Tuple, Type

import apache_beam as beam
from apache_beam import Pipeline

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
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
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entities_by_association_table,
    is_association_table,
)
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.generate_primary_key import (
    PrimaryKey,
    generate_primary_key,
)
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.state.associate_with_primary_keys import (
    MERGED_ROOT_ENTITIES_WITH_DATES,
    PRIMARY_KEYS,
    AssociateRootEntitiesWithPrimaryKeys,
)
from recidiviz.pipelines.ingest.state.cluster_root_external_ids import (
    ClusterRootExternalIds,
)
from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)
from recidiviz.pipelines.ingest.state.generate_entities import GenerateEntities
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import string_representation
from recidiviz.pipelines.ingest.state.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.pipelines.ingest.state.merge_root_entities_across_dates import (
    MergeRootEntitiesAcrossDates,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.pipelines.ingest.state.serialize_entities import SerializeEntities
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


def get_pipeline_output_tables(expected_output_entities: Set[str]) -> Set[str]:
    """Returns the set of tables that the pipeline will output to."""
    expected_output_tables: Set[str] = set()
    for table in get_all_table_classes_in_schema(SchemaType.STATE):
        if is_association_table(table.name):
            parent_member, child_member = get_database_entities_by_association_table(
                state_schema, table.name
            )
            if (
                parent_member.get_entity_name() in expected_output_entities
                and child_member.get_entity_name() in expected_output_entities
            ):
                expected_output_tables.add(table.name)
        if table.name in expected_output_entities:
            expected_output_tables.add(table.name)

    return expected_output_tables


class StateIngestPipeline(BasePipeline[IngestPipelineParameters]):
    """Defines the ingest pipeline that reads from raw data, creates ingest view results
    and then creates state entities based on those ingest view results."""

    @classmethod
    def parameters_type(cls) -> Type[IngestPipelineParameters]:
        return IngestPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "INGEST"

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        return {}

    def run_pipeline(self, p: Pipeline) -> None:
        field_index = CoreEntityFieldIndex()
        raw_data_source_instance = DirectIngestInstance(
            self.pipeline_parameters.raw_data_source_instance
        )
        state_code = StateCode(self.pipeline_parameters.state_code.upper())
        raw_data_upper_bound_dates = self.pipeline_parameters.raw_data_upper_bound_dates

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
        all_launchable_views = ingest_manifest_collector.launchable_ingest_views()
        view_collector = DirectIngestViewQueryBuilderCollector(
            region, all_launchable_views
        )

        ingest_views_to_run = (
            self.pipeline_parameters.ingest_views_to_run.split(" ")
            if self.pipeline_parameters.ingest_views_to_run
            else all_launchable_views
        )

        for ingest_view in ingest_views_to_run:
            if ingest_view not in all_launchable_views:
                raise ValueError(
                    f"Found invalid ingest view for {state_code}: {ingest_view}"
                )

            query_builder = view_collector.get_query_builder_by_view_name(ingest_view)

            raw_files_with_data = set(raw_data_upper_bound_dates)
            if dependencies_missing_data := (
                query_builder.raw_data_table_dependency_file_tags - raw_files_with_data
            ):
                raise ValueError(
                    f"Found dependency table(s) of ingest view [{ingest_view}] with no "
                    f"data: {dependencies_missing_data}"
                )

        expected_output_entities = {
            entity_cls.get_entity_name()
            for ingest_view in ingest_views_to_run
            for entity_cls in ingest_manifest_collector.ingest_view_to_manifest[
                ingest_view
            ].hydrated_entity_classes()
        }

        if not expected_output_entities and not ingest_views_to_run:
            return
        if not expected_output_entities:
            raise ValueError("Pipeline has no expected output")

        merged_root_entities_with_dates_per_ingest_view: Dict[
            IngestViewName,
            beam.PCollection[Tuple[ExternalIdKey, Tuple[UpperBoundDate, RootEntity]]],
        ] = {}
        for ingest_view in ingest_views_to_run:
            query_builder = view_collector.get_query_builder_by_view_name(ingest_view)

            ingest_view_results: beam.PCollection[
                Dict[str, Any]
            ] = p | f"Materialize {ingest_view} results" >> GenerateIngestViewResults(
                project_id=self.pipeline_parameters.project,
                state_code=state_code,
                ingest_view_name=ingest_view,
                raw_data_tables_to_upperbound_dates={
                    # Filter down to only the tags referenced by this view.
                    file_tag: raw_data_upper_bound_dates[file_tag]
                    for file_tag in query_builder.raw_data_table_dependency_file_tags
                },
                raw_data_source_instance=raw_data_source_instance,
            )

            _ = (
                ingest_view_results
                | f"Write {ingest_view} results to table."
                >> WriteToBigQuery(
                    output_dataset=self.pipeline_parameters.ingest_view_results_output,
                    output_table=ingest_view,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                )
            )
            if self.pipeline_parameters.ingest_view_results_only:
                continue

            merged_root_entities_with_dates_per_ingest_view[ingest_view] = (
                ingest_view_results
                | f"Generate {ingest_view} entities."
                >> GenerateEntities(
                    state_code=state_code,
                    ingest_view_manifest=ingest_manifest_collector.ingest_view_to_manifest[
                        ingest_view
                    ],
                )
                | f"Merge {ingest_view} entities using IngestViewTreeMerger within same date and external ID."
                >> MergeIngestViewRootEntityTrees(
                    ingest_view_name=ingest_view,
                    state_code=state_code,
                    field_index=field_index,
                )
            )

        if self.pipeline_parameters.ingest_view_results_only:
            return

        merged_root_entities_with_dates: beam.PCollection[
            Tuple[ExternalIdKey, Tuple[UpperBoundDate, IngestViewName, RootEntity]]
        ] = (merged_root_entities_with_dates_per_ingest_view.values() | beam.Flatten())

        # Silence `No value for argument 'pcoll' in function call (no-value-for-parameter)`
        # pylint: disable=E1120
        all_root_entities: beam.PCollection[RootEntity] = (
            merged_root_entities_with_dates
            | "Remove all of the external ids" >> beam.Values()
            | "Remove all of the dates and ingest view names"
            >> beam.Map(lambda item: item[2])
        )

        root_entity_external_ids_to_primary_keys: beam.PCollection[
            Tuple[ExternalIdKey, PrimaryKey]
        ] = (
            all_root_entities
            | beam.ParDo(GetRootExternalIdClusterEdges())
            | ClusterRootExternalIds()
            | "Generate primary keys for root entities"
            >> beam.MapTuple(
                lambda root_external_id, cluster: (
                    root_external_id,
                    generate_primary_key(
                        string_representation(cluster),
                        state_code=state_code,
                    ),
                )
            )
        )

        output_state_tables = get_pipeline_output_tables(expected_output_entities)
        final_entities: beam.PCollection[Dict[str, Any]] = (
            {
                PRIMARY_KEYS: root_entity_external_ids_to_primary_keys,
                MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
            }
            | AssociateRootEntitiesWithPrimaryKeys()
            | MergeRootEntitiesAcrossDates(
                state_code=state_code, field_index=field_index
            )
            | RunValidations(
                expected_output_entities=expected_output_entities,
                field_index=field_index,
                state_code=state_code,
            )
            | beam.ParDo(
                SerializeEntities(state_code=state_code, field_index=field_index)
            ).with_outputs(*output_state_tables)
        )

        for table_name in output_state_tables:
            _ = getattr(
                final_entities, table_name
            ) | f"Write {table_name} to BigQuery" >> WriteToBigQuery(
                output_dataset=self.pipeline_parameters.pre_normalization_output,
                output_table=table_name,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )

        if self.pipeline_parameters.run_normalization:
            # TODO(#29517): Actually add normalization logic here!
            raise NotImplementedError(
                "Normalization logic in ingest pipelines not yet implemented"
            )
