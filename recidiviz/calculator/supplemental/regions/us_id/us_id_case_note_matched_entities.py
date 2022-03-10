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
"""Contains logic to generate the us_id_case_note_matched_entities BigQuery table."""
from copy import deepcopy
from typing import Any, Dict

from google.cloud import bigquery

from recidiviz.calculator.query.state.views.reference.us_id_case_update_info import (
    US_ID_CASE_UPDATE_INFO_VIEW_BUILDER,
)
from recidiviz.calculator.supplemental.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)
from recidiviz.calculator.supplemental.supplemental_dataset import (
    SupplementalDatasetTable,
)
from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration

# TODO(#11312): Remove once dataflow pipeline is deployed.
US_ID_CASE_NOTE_MATCHED_ENTITIES_TABLE = "us_id_case_note_matched_entities"
TEXT_ANALYZER = TextAnalyzer(
    TextMatchingConfiguration(
        stop_words_to_remove={"in", "out"},
        text_entities=list(UsIdTextEntity),
    )
)
DEFAULT_ENTITY_MAPPING = {
    entity.name.lower(): False
    for entity in UsIdTextEntity
    if entity != UsIdTextEntity.REVOCATION_INCLUDE
}


class UsIdCaseNoteMatchedEntities(SupplementalDatasetTable):
    """Generates the BigQuery table us_id_case_note_matched_entities based on
    results of text analysis."""

    def __init__(self) -> None:
        super().__init__(
            destination_table_id=US_ID_CASE_NOTE_MATCHED_ENTITIES_TABLE,
            dependent_views=[US_ID_CASE_UPDATE_INFO_VIEW_BUILDER],
        )

    def query_from_dependent_views(self, project_id: str) -> str:
        """Generates the query from the case update info reference view for US_ID."""
        materialized_reference_view = (
            US_ID_CASE_UPDATE_INFO_VIEW_BUILDER.materialized_address
        )
        if not materialized_reference_view:
            raise ValueError(
                f"Missing materialized address for reference view {US_ID_CASE_UPDATE_INFO_VIEW_BUILDER.view_id}"
            )

        return f"SELECT * FROM `{project_id}.{materialized_reference_view.dataset_id}.{materialized_reference_view.table_id}`"

    def process_row(
        self,
        row: bigquery.table.Row,
    ) -> Dict[str, Any]:
        """Extracts text entities for each row queried, generating a new appended row with
        boolean flags indicating which entities were matched to."""
        row_dict = dict(row)
        entity_mapping = deepcopy(DEFAULT_ENTITY_MAPPING)
        agent_note_title = row_dict["agnt_note_title"]
        if agent_note_title:
            matched_entities = TEXT_ANALYZER.extract_entities(agent_note_title)
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
        final_row: Dict[str, Any] = {**row_dict, **entity_mapping}
        final_row["create_dt"] = final_row["create_dt"].strftime("%Y-%m-%d %H:%M:%S")
        return final_row
