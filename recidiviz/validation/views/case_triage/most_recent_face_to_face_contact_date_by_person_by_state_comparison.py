# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view which provides a comparison of the most recent face-to-face contact date from
the state table vs. the etl_clients tables"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_VIEW_NAME = (
    "most_recent_face_to_face_contact_date_by_person_by_state_comparison"
)

MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_DESCRIPTION = """ Comparison of the most recent face-to-face contact date within etl clients to the state tables."""

# TODO(#8579): Remove the group by clauses once confirmed that there is one row per person.
# TODO(#8646): Update the view to import ingest view and compare values against ingest view.
MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_QUERY_TEMPLATE = """
WITH
  etl_clients AS (
  SELECT
    state_code AS region_code,
    person_external_id,
    supervising_officer_external_id AS officer_external_id,
    MAX(most_recent_face_to_face_date) AS most_recent_etl_face_to_face_contact_date
  FROM
    `{project_id}.{case_triage_dataset}.etl_clients_materialized`
  GROUP BY
    state_code,
    person_external_id,
    supervising_officer_external_id ),
  state_contacts AS (
  SELECT
    state_code AS region_code,
    person_id,
    MAX(contact_date) AS most_recent_state_face_to_face_contact_date
  FROM
    `{project_id}.{state_dataset}.state_supervision_contact`
  WHERE
    contact_date IS NOT NULL
    AND status = 'COMPLETED'
    AND contact_type = 'FACE_TO_FACE'
  GROUP BY
    state_code,
    person_id),
  person_to_external_ids AS (
  SELECT
    state_code AS region_code,
    person_id,
    external_id AS person_external_id
  FROM
    `{project_id}.{state_dataset}.state_person_external_id`)
SELECT
  region_code,
  person_external_id,
  officer_external_id,
  most_recent_etl_face_to_face_contact_date,
  most_recent_state_face_to_face_contact_date,
FROM
  etl_clients
JOIN (person_to_external_ids
  JOIN
    state_contacts
  USING
    (region_code,
      person_id))
USING
  (region_code,
    person_external_id)
"""

MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_VIEW_NAME,
    view_query_template=MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_QUERY_TEMPLATE,
    description=MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_DESCRIPTION,
    state_dataset=STATE_BASE_DATASET,
    case_triage_dataset=VIEWS_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MOST_RECENT_FACE_TO_FACE_CONTACT_DATE_BY_PERSON_BY_STATE_COMPARISON_VIEW_BUILDER.build_and_print()
