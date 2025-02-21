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
recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py for details on how to launch a
local run."""
import datetime
import re
from copy import deepcopy
from typing import Dict, Type

import apache_beam as beam
from apache_beam import Pipeline
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.persistence.entity.serialization import json_serializable_dict
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS,
    US_IX_CASE_NOTE_DEFAULT_ENTITY_MAPPING,
    US_IX_CASE_NOTE_MATCHED_ENTITIES_TABLE_NAME,
)
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_case_update_info_query_provider import (
    US_IX_CASE_UPDATE_INFO_QUERY_NAME,
    get_us_ix_case_update_info_query_provider,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_content_text_analysis_configuration import (
    UsIxNoteContentTextEntity,
    get_note_content_text_analyzer,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration import (
    UsIxNoteTitleTextEntity,
    get_note_title_text_analyzer,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    ReadFromBigQuery,
    WriteToBigQuery,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.utils import metadata


# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
class UsIxCaseNoteExtractedEntitiesPipeline(SupplementalDatasetPipeline):
    """Defines the entity extraction pipeline that processes all US_IX case notes with
    fuzzy matched entities."""

    @classmethod
    def pipeline_name(cls) -> str:
        return "US_IX_CASE_NOTE_EXTRACTED_ENTITIES_SUPPLEMENTAL"

    @classmethod
    def input_reference_query_providers(
        cls,
        state_code: StateCode,
        address_overrides: BigQueryAddressOverrides | None,
    ) -> Dict[str, BigQueryQueryProvider]:
        return {
            US_IX_CASE_UPDATE_INFO_QUERY_NAME: get_us_ix_case_update_info_query_provider(
                project_id=metadata.project_id(), address_overrides=address_overrides
            )
        }

    @classmethod
    def table_id(cls) -> str:
        return US_IX_CASE_NOTE_MATCHED_ENTITIES_TABLE_NAME

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        return DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS[
            US_IX_CASE_NOTE_MATCHED_ENTITIES_TABLE_NAME
        ]

    @property
    def note_title_text_analyzer(self) -> TextAnalyzer:
        return get_note_title_text_analyzer()

    @property
    def note_content_text_analyzer(self) -> TextAnalyzer:
        return get_note_content_text_analyzer()

    def extract_text_entities(self, row: TableRow) -> TableRow:
        """Runs the entities through extraction."""

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

        final_row: TableRow = {**row, **entity_mapping}
        if final_row["NoteDate"]:
            final_row["NoteDate"] = datetime.datetime.strptime(
                final_row["NoteDate"], "%Y-%m-%d %H:%M:%S"
            ).date()

        return json_serializable_dict(final_row)

    @classmethod
    def default_entity_mapping(cls) -> Dict[str, bool]:
        return US_IX_CASE_NOTE_DEFAULT_ENTITY_MAPPING

    def run_pipeline(self, p: Pipeline) -> None:
        state_code = StateCode(self.pipeline_parameters.state_code)
        query_name, query_provider = one(
            self.all_input_reference_query_providers(
                state_code=state_code,
                address_overrides=self.pipeline_parameters.input_dataset_overrides,
            ).items()
        )
        _ = (
            p
            | f"Load [{query_name}] query results"
            >> ReadFromBigQuery(
                query=query_provider.get_query(),
                resource_labels=self.pipeline_parameters.resource_labels,
            )
            | "Extract text entities" >> beam.Map(self.extract_text_entities)
            | f"Write extracted text entities to {self.pipeline_parameters.output}.{self.table_id()}"
            >> WriteToBigQuery(
                output_table=self.table_id(),
                output_dataset=self.pipeline_parameters.output,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )
