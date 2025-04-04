# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View of all US_ME case notes to be imported to cloud storage."""

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import CASE_NOTES_PROTOTYPE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_NOTES_DATA_STORE_VIEW_NAME = "case_notes_data_store"

CASE_NOTES_DATA_STORE_VIEW_DESCRIPTION = """All case notes formatted as JSON. This allows for structured import to cloud storage per
    https://cloud.google.com/generative-ai-app-builder/docs/prepare-data#bigquery-structured.
    The jsonData field is a JSON string with the following fields:
        state_code, external_id, note_id, note_body, note_title, note_date, note_type, note_mode
    """

CASE_NOTES_DATA_STORE_QUERY_TEMPLATE = """
    SELECT
        id,
        TO_JSON_STRING(STRUCT(state_code, external_id, note_id, note_body, note_title, note_date, note_type, note_mode))
            as jsonData,
        FROM `{project_id}.{case_notes_prototype_dataset}.case_notes_materialized`
"""

CASE_NOTES_DATA_STORE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_NOTES_PROTOTYPE_DATASET,
    view_id=CASE_NOTES_DATA_STORE_VIEW_NAME,
    view_query_template=CASE_NOTES_DATA_STORE_QUERY_TEMPLATE,
    description=CASE_NOTES_DATA_STORE_VIEW_DESCRIPTION,
    case_notes_prototype_dataset=CASE_NOTES_PROTOTYPE_DATASET,
    should_materialize=True,
    clustering_fields=["id"],
    materialized_table_schema=[
        bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="jsonData", field_type="STRING", mode="REQUIRED"),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_NOTES_DATA_STORE_VIEW_BUILDER.build_and_print()
