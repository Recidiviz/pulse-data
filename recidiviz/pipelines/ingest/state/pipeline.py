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
from typing import Dict, Tuple, Type

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.pvalue import PDone

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
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
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.generate_primary_key import (
    PrimaryKey,
    generate_primary_key,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
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
from recidiviz.pipelines.ingest.state.constants import ExternalIdKey
from recidiviz.pipelines.ingest.state.expected_output_helpers import (
    get_entity_table_ids_to_clear,
    get_expected_output_normalized_entity_classes,
    get_expected_output_pre_normalization_entity_classes,
    get_expected_output_table_ids,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import string_representation
from recidiviz.pipelines.ingest.state.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.pipelines.ingest.state.merge_root_entities_across_dates import (
    MergeRootEntitiesAcrossDates,
)
from recidiviz.pipelines.ingest.state.normalize_root_entities import (
    NormalizeRootEntities,
)
from recidiviz.pipelines.ingest.state.process_all_ingest_views import (
    ProcessAllIngestViews,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.pipelines.ingest.state.write_root_entities_to_bq import (
    WriteRootEntitiesToBQ,
)
from recidiviz.pipelines.utils.beam_utils.clear_bq_table import ClearBQTable
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_normalization_delegate,
)


class _ClearNoOutputEntityTables(beam.PTransform):
    """PTransform that clears entity tables the pipeline does NOT expect to
    produce data for, ensuring stale rows from prior runs are removed.

    Accepts a PCollection as input so that clearing runs only after upstream
    entity computation is complete (i.e. at the same time as the associated
    write steps).
    """

    def __init__(
        self, table_ids_to_clear: set[str], output_dataset: str, project_id: str
    ) -> None:
        super().__init__()
        self.table_ids_to_clear = table_ids_to_clear
        self.output_dataset = output_dataset
        self.project_id = project_id

    def expand(self, input_or_inputs: beam.PCollection) -> PDone:
        for table_id in sorted(self.table_ids_to_clear):
            _ = input_or_inputs | f"Clear {table_id}" >> ClearBQTable(
                address=ProjectSpecificBigQueryAddress(
                    project_id=self.project_id,
                    dataset_id=self.output_dataset,
                    table_id=table_id,
                ),
            )
        return PDone(input_or_inputs.pipeline)


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
        state_code = StateCode(self.pipeline_parameters.state_code.upper())
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )
        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )
        ingest_view_context = IngestViewContentsContext.build_for_project(
            project_id=self.pipeline_parameters.project
        )
        all_launchable_views = ingest_manifest_collector.launchable_ingest_views(
            ingest_view_context
        )
        ingest_views_to_run = self.pipeline_parameters.launchable_ingest_views_to_run(
            all_launchable_views
        )

        expected_output_entity_classes = (
            get_expected_output_pre_normalization_entity_classes(
                ingest_manifest_collector, ingest_views_to_run
            )
        )

        if not expected_output_entity_classes and not ingest_views_to_run:
            return
        if not expected_output_entity_classes:
            raise ValueError("Pipeline has no expected output")

        merged_root_entities_with_dates = (
            p
            | "Process ingest views"
            >> ProcessAllIngestViews(pipeline_parameters=self.pipeline_parameters)
        )

        if self.pipeline_parameters.ingest_view_results_only:
            return

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

        expected_pre_norm_table_ids = get_expected_output_table_ids(
            expected_output_entity_classes, state_entities
        )
        pre_norm_table_ids_to_clear = get_entity_table_ids_to_clear(
            expected_output_entity_classes, state_entities
        )

        pre_normalization_root_entities: beam.PCollection[RootEntity] = (
            {
                PRIMARY_KEYS: root_entity_external_ids_to_primary_keys,
                MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
            }
            | AssociateRootEntitiesWithPrimaryKeys()
            | MergeRootEntitiesAcrossDates(state_code=state_code)
            | RunValidations(
                expected_output_entity_classes=expected_output_entity_classes,
                state_code=state_code,
                entities_module=state_entities,
            )
        )

        _ = (
            pre_normalization_root_entities
            | f"Write pre-normalization entities to {self.pipeline_parameters.pre_normalization_output}"
            >> WriteRootEntitiesToBQ(
                state_code=state_code,
                entities_module=state_entities,
                output_dataset=self.pipeline_parameters.pre_normalization_output,
                output_table_ids=expected_pre_norm_table_ids,
            )
        )

        if pre_norm_table_ids_to_clear:
            _ = (
                pre_normalization_root_entities
                | "Clear no output pre-normalization entity tables"
                >> _ClearNoOutputEntityTables(
                    table_ids_to_clear=pre_norm_table_ids_to_clear,
                    output_dataset=self.pipeline_parameters.pre_normalization_output,
                    project_id=self.pipeline_parameters.project,
                )
            )

        if self.pipeline_parameters.pre_normalization_only:
            return

        expected_output_normalized_entity_classes = (
            get_expected_output_normalized_entity_classes(
                expected_output_entity_classes,
                delegate=get_state_specific_normalization_delegate(state_code.value),
            )
        )
        expected_norm_table_ids = get_expected_output_table_ids(
            expected_output_normalized_entity_classes, normalized_entities
        )
        norm_table_ids_to_clear = get_entity_table_ids_to_clear(
            expected_output_normalized_entity_classes, normalized_entities
        )

        normalized_root_entities: beam.PCollection[RootEntity] = (
            pre_normalization_root_entities
            | "Normalize root entities"
            >> NormalizeRootEntities(
                state_code=state_code,
                expected_output_entity_classes=expected_output_normalized_entity_classes,
            )
        )

        _ = (
            normalized_root_entities
            | f"Write normalized entities to {self.pipeline_parameters.normalized_output}"
            >> WriteRootEntitiesToBQ(
                state_code=state_code,
                entities_module=normalized_entities,
                output_dataset=self.pipeline_parameters.normalized_output,
                output_table_ids=expected_norm_table_ids,
            )
        )

        if norm_table_ids_to_clear:
            _ = (
                normalized_root_entities
                | "Clear no output normalized entity tables"
                >> _ClearNoOutputEntityTables(
                    table_ids_to_clear=norm_table_ids_to_clear,
                    output_dataset=self.pipeline_parameters.normalized_output,
                    project_id=self.pipeline_parameters.project,
                )
            )
