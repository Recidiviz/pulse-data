# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""The us_id case note extracted entities supplemental dataset calculation pipeline. See
recidiviz/tools/run_sandbox_calculation_pipeline.py for details on how to launch a
local run."""
import datetime
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Tuple, Type

import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.calculator.pipeline.base_pipeline import PipelineConfig
from recidiviz.calculator.pipeline.pipeline_type import (
    US_ID_CASE_NOTE_EXTRACTED_ENTITIES_PIPELINE_NAME,
)
from recidiviz.calculator.pipeline.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.supplemental.us_id_case_note_extracted_entities.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    WriteToBigQuery,
    json_serializable_dict,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.query.state.views.reference.us_id_case_update_info import (
    US_ID_CASE_UPDATE_INFO_VIEW_NAME,
)
from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration


class UsIdCaseNoteExtractedEntitiesPipelineRunDelegate(
    SupplementalDatasetPipelineRunDelegate
):
    """Defines the entity extraction pipeline that processes all US_ID case notes with
    appropriate fuzzy matched entities."""

    @classmethod
    def table_id(cls) -> str:
        return "us_id_case_note_matched_entities"

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        fields_from_case_updates: Dict[str, Type] = {
            "person_id": int,
            "person_external_id": str,
            "state_code": str,
            "agnt_case_updt_id": str,
            "create_dt": datetime.date,
            "create_by_usr_id": str,
            "agnt_note_title": str,
        }
        default_entity_mapping_types: Dict[str, Type] = {
            entity: bool for entity in cls.default_entity_mapping().keys()
        }
        return {**fields_from_case_updates, **default_entity_mapping_types}

    @property
    def text_analyzer(self) -> TextAnalyzer:
        return TextAnalyzer(
            TextMatchingConfiguration(
                stop_words_to_remove={"in", "out"}, text_entities=list(UsIdTextEntity)
            )
        )

    def extract_text_entities(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
    ) -> List[Dict[str, Any]]:
        """Runs the entities through extraction."""
        _person_id, table_data = element

        rows: Iterable[TableRow] = table_data.pop(US_ID_CASE_UPDATE_INFO_VIEW_NAME)
        if table_data:
            raise ValueError(
                f"Found unexpected keys in tables map: {table_data.keys()}"
            )
        final_rows: List[TableRow] = []

        for row in rows:
            entity_mapping = deepcopy(self.default_entity_mapping())
            agent_note_title = row["agnt_note_title"]
            if agent_note_title:
                matched_entities = self.text_analyzer.extract_entities(agent_note_title)
                for entity in matched_entities:
                    entity_mapping_key = entity.name.lower()
                    if (
                        entity == UsIdTextEntity.REVOCATION_INCLUDE
                        or (
                            entity == UsIdTextEntity.REVOCATION
                            and UsIdTextEntity.REVOCATION_INCLUDE in matched_entities
                        )
                        or (
                            entity == UsIdTextEntity.TREATMENT_COMPLETE
                            and UsIdTextEntity.ANY_TREATMENT not in matched_entities
                        )
                    ):
                        continue
                    entity_mapping[entity_mapping_key] = True
            final_row: Dict[str, Any] = {**row, **entity_mapping}
            final_row["create_dt"] = final_row["create_dt"].strftime("%Y-%m-%d")
            final_rows.append(json_serializable_dict(final_row))

        return final_rows

    @classmethod
    def default_entity_mapping(cls) -> Dict[str, bool]:
        return {
            entity.name.lower(): False
            for entity in UsIdTextEntity
            if entity != UsIdTextEntity.REVOCATION_INCLUDE
        }

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=US_ID_CASE_NOTE_EXTRACTED_ENTITIES_PIPELINE_NAME,
            required_entities=[],
            required_reference_tables=[US_ID_CASE_UPDATE_INFO_VIEW_NAME],
            required_state_based_reference_tables=[],
            state_specific_required_delegates=[],
            state_specific_required_reference_tables={},
        )

    def run_data_transforms(
        self, p: PBegin, pipeline_data: beam.Pipeline
    ) -> beam.Pipeline:
        extracted_entities = pipeline_data | "Extract text entities" >> beam.FlatMap(
            self.extract_text_entities
        )

        return extracted_entities

    def write_output(self, pipeline: beam.Pipeline) -> None:
        _ = (
            pipeline
            | f"Write extracted text entities to {self.pipeline_job_args.output_dataset}.{self.table_id()}"
            >> WriteToBigQuery(
                output_table=self.table_id(),
                output_dataset=self.pipeline_job_args.output_dataset,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )
