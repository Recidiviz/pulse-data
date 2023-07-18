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
from typing import Type

import apache_beam as beam
from apache_beam import Pipeline

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
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.utils.ingest_view_query_helpers import (
    generate_date_bound_tuples_query,
    get_ingest_view_date_diff_query,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    ReadFromBigQuery,
    WriteToBigQuery,
)


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
    # TODO(#22141) Adjust implementation to handle memory allotment errors from bigger states.
    def run_pipeline(self, p: Pipeline) -> None:
        ingest_instance = DirectIngestInstance(self.pipeline_parameters.ingest_instance)
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
        for ingest_view in ingest_manifest_collector.launchable_ingest_views():
            raw_data_tables = [
                raw_data_dependency.raw_file_config.file_tag
                for raw_data_dependency in view_collector.get_query_builder_by_view_name(
                    ingest_view
                ).raw_table_dependency_configs
            ]
            ingest_view_results = (
                p
                | f"Read {ingest_view} date pairs based on raw data tables."
                >> ReadFromBigQuery(
                    query=generate_date_bound_tuples_query(
                        project_id=self.pipeline_parameters.project,
                        state_code=self.pipeline_parameters.state_code,
                        raw_data_tables=raw_data_tables,
                    ),
                )
                | f"Generate date diff queries for {ingest_view} based on date pairs."
                >> beam.Map(
                    get_ingest_view_date_diff_query,
                    project_id=self.pipeline_parameters.project,
                    state_code=self.pipeline_parameters.state_code,
                    ingest_view_name=ingest_view,
                    ingest_instance=ingest_instance,
                )
                | f"Read {ingest_view} date diff queries based on date pairs."
                >> beam.io.ReadAllFromBigQuery()
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
