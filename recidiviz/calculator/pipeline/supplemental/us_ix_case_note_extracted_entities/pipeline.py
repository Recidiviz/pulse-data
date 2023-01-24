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
"""The us_ix case note extracted entities supplemental dataset calculation pipeline. See
recidiviz/tools/run_sandbox_calculation_pipeline.py for details on how to launch a
local run."""
import datetime
import re
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Tuple, Type

import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.calculator.pipeline.base_pipeline import PipelineConfig
from recidiviz.calculator.pipeline.pipeline_type import (
    US_IX_CASE_NOTE_EXTRACTED_ENTITIES_PIPELINE_NAME,
)
from recidiviz.calculator.pipeline.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.supplemental.us_ix_case_note_extracted_entities.us_ix_note_content_text_analysis_configuration import (
    NOTE_CONTENT_TEXT_ANALYZER,
    UsIxNoteContentTextEntity,
)
from recidiviz.calculator.pipeline.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration import (
    NOTE_TITLE_TEXT_ANALYZER,
    UsIxNoteTitleTextEntity,
)
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    WriteToBigQuery,
    json_serializable_dict,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.query.state.views.reference.us_ix_case_update_info import (
    US_IX_CASE_UPDATE_INFO_VIEW_NAME,
)
from recidiviz.common.text_analysis import TextAnalyzer


# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
class UsIxCaseNoteExtractedEntitiesPipelineRunDelegate(
    SupplementalDatasetPipelineRunDelegate
):
    """Defines the entity extraction pipeline that processes all US_IX case notes with
    fuzzy matched entities."""

    @classmethod
    def table_id(cls) -> str:
        return "us_ix_case_note_matched_entities"

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        fields_from_case_updates: Dict[str, Type[Any]] = {
            "person_id": int,
            "person_external_id": str,
            "state_code": str,
            "OffenderNoteId": str,
            "NoteDate": datetime.date,
            "StaffId": str,
            "Details": str,
        }
        default_entity_mapping_types: Dict[str, Type] = {
            entity: bool for entity in cls.default_entity_mapping().keys()
        }
        return {**fields_from_case_updates, **default_entity_mapping_types}

    @property
    def note_title_text_analyzer(self) -> TextAnalyzer:
        return NOTE_TITLE_TEXT_ANALYZER

    @property
    def note_content_text_analyzer(self) -> TextAnalyzer:
        return NOTE_CONTENT_TEXT_ANALYZER

    def extract_text_entities(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
    ) -> List[Dict[str, Any]]:
        """Runs the entities through extraction."""
        _person_id, table_data = element

        rows: Iterable[TableRow] = table_data.pop(US_IX_CASE_UPDATE_INFO_VIEW_NAME)
        if table_data:
            raise ValueError(
                f"Found unexpected keys in tables map: {table_data.keys()}"
            )
        final_rows: List[TableRow] = []

        for row in rows:
            entity_mapping = deepcopy(self.default_entity_mapping())
            note = row["Details"]
            if note:
                # Old notes that were converted to Atlas are in this JSON-like form
                # {note_title: xxxx} {note: yyyy}
                # We pull the title out of these and run them through the matcher we
                # used for the old ID system.
                if match := re.match(
                    r"\{note_title: (?P<note_title>.*)\} \{note: (?P<note>.*)\}", note
                ):
                    matched_entities = self.note_title_text_analyzer.extract_entities(
                        match.group("note_title")
                    )
                    for entity in matched_entities:
                        entity_mapping_key = entity.name.lower()
                        if (
                            entity == UsIxNoteTitleTextEntity.REVOCATION_INCLUDE
                            or (
                                entity == UsIxNoteTitleTextEntity.REVOCATION
                                and UsIxNoteTitleTextEntity.REVOCATION_INCLUDE
                                in matched_entities
                            )
                            or (
                                entity == UsIxNoteTitleTextEntity.TREATMENT_COMPLETE
                                and UsIxNoteTitleTextEntity.ANY_TREATMENT
                                not in matched_entities
                            )
                        ):
                            continue
                        entity_mapping[entity_mapping_key] = True

                # Notes entered directly into Atlas do not have this form or a title at
                # all. We just have the note body, so we run that through a separate
                # analyzer.
                else:
                    matched_entities = self.note_content_text_analyzer.extract_entities(
                        note
                    )
                    for entity in matched_entities:
                        entity_mapping_key = entity.name.lower()
                        if (
                            entity == UsIxNoteContentTextEntity.REVOCATION_INCLUDE
                            or (
                                entity == UsIxNoteContentTextEntity.REVOCATION
                                and UsIxNoteContentTextEntity.REVOCATION_INCLUDE
                                in matched_entities
                            )
                            or (
                                entity == UsIxNoteContentTextEntity.TREATMENT_COMPLETE
                                and UsIxNoteContentTextEntity.ANY_TREATMENT
                                not in matched_entities
                            )
                        ):
                            continue
                        entity_mapping[entity_mapping_key] = True

            final_row: Dict[str, Any] = {**row, **entity_mapping}
            if final_row["NoteDate"]:
                final_row["NoteDate"] = datetime.datetime.strptime(
                    final_row["NoteDate"], "%Y-%m-%d %H:%M:%S"
                ).date()
            final_rows.append(json_serializable_dict(final_row))

        return final_rows

    @classmethod
    def default_entity_mapping(cls) -> Dict[str, bool]:
        return {
            entity.name.lower(): False
            for entity in UsIxNoteTitleTextEntity
            if entity != UsIxNoteTitleTextEntity.REVOCATION_INCLUDE
        } | {
            entity.name.lower(): False
            for entity in UsIxNoteContentTextEntity
            if entity != UsIxNoteContentTextEntity.REVOCATION_INCLUDE
        }

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=US_IX_CASE_NOTE_EXTRACTED_ENTITIES_PIPELINE_NAME,
            required_entities=[],
            required_reference_tables=[US_IX_CASE_UPDATE_INFO_VIEW_NAME],
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
