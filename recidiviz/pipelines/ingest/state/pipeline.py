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
from datetime import datetime
from typing import Any, Dict, Tuple, Type

import apache_beam as beam
from apache_beam import Pipeline

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.pipeline_parameters import (
    IngestPipelineParameters,
    MaterializationMethod,
)
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
    PrimaryKey,
)
from recidiviz.pipelines.ingest.state.generate_entities import GenerateEntities
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import generate_primary_key
from recidiviz.pipelines.ingest.state.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class StateIngestPipeline(BasePipeline[IngestPipelineParameters]):
    """Defines the ingest pipeline that reads from raw data, creates ingest view results
    and then creates state entities based on those ingest view results."""

    @classmethod
    def parameters_type(cls) -> Type[IngestPipelineParameters]:
        return IngestPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "INGEST"

    # TODO(#20928) Replace with actual pipeline logic.
    def run_pipeline(self, p: Pipeline) -> None:
        ingest_instance = DirectIngestInstance(self.pipeline_parameters.ingest_instance)
        materialization_method = MaterializationMethod(
            self.pipeline_parameters.materialization_method
        )
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=self.pipeline_parameters.state_code
        )
        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=IngestViewResultsParserDelegateImpl(
                region=region,
                schema_type=SchemaType.STATE,
                ingest_instance=ingest_instance,
                results_update_datetime=datetime.now(),
            ),
        )
        view_collector = DirectIngestViewQueryBuilderCollector(
            region,
            ingest_manifest_collector.launchable_ingest_views(),
        )
        merged_root_entities_with_dates_per_ingest_view: Dict[
            IngestViewName,
            beam.PCollection[Tuple[ExternalIdKey, Tuple[datetime, RootEntity]]],
        ] = {}
        for ingest_view in ingest_manifest_collector.launchable_ingest_views():
            raw_data_tables = [
                raw_data_dependency.raw_file_config.file_tag
                for raw_data_dependency in view_collector.get_query_builder_by_view_name(
                    ingest_view
                ).raw_table_dependency_configs
            ]

            ingest_view_results: beam.PCollection[
                Dict[str, Any]
            ] = p | f"Materialize {ingest_view} results" >> GenerateIngestViewResults(
                project_id=self.pipeline_parameters.project,
                state_code=self.pipeline_parameters.state_code,
                ingest_view_name=ingest_view,
                raw_data_tables=raw_data_tables,
                ingest_instance=ingest_instance,
                materialization_method=materialization_method,
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

            merged_root_entities_with_dates_per_ingest_view[ingest_view] = (
                ingest_view_results
                | f"Generate {ingest_view} entities."
                >> GenerateEntities(
                    state_code=self.pipeline_parameters.state_code,
                    ingest_instance=ingest_instance,
                    ingest_view_name=ingest_view,
                )
                | f"Merge {ingest_view} entities using IngestViewTreeMerger within same date and external ID."
                >> MergeIngestViewRootEntityTrees(ingest_view_name=ingest_view)
            )

        merged_root_entities_with_dates: beam.PCollection[
            Tuple[ExternalIdKey, Tuple[datetime, RootEntity]]
        ] = (merged_root_entities_with_dates_per_ingest_view.values() | beam.Flatten())

        # pylint: disable=no-value-for-parameter
        all_root_entities: beam.PCollection[RootEntity] = (
            merged_root_entities_with_dates
            | "Remove all of the external ids" >> beam.Values()
            | "Remove all of the dates" >> beam.Values()
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
                        cluster,
                        state_code=StateCode(self.pipeline_parameters.state_code),
                    ),
                )
            )
        )

        _ = {
            PRIMARY_KEYS: root_entity_external_ids_to_primary_keys,
            MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | AssociateRootEntitiesWithPrimaryKeys()
