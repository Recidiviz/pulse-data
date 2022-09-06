# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Connects sentence IDs to judicial districts corresponding to the charges and court
cases on the sentence."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME = (
    "sentence_judicial_district_association"
)

SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION = """Connects sentence IDs to
judicial districts corresponding to the charges and court cases on the sentence."""

SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision AS (
      SELECT
        ss.state_code,
        ss.person_id,
        ss.external_id,
        'SUPERVISION' as sentence_type,
        ss.supervision_sentence_id as sentence_id,
        ss.start_date,
        COALESCE(ss.completion_date, ss.projected_completion_date) as completion_date,
        charge.county_code,
        charge.date_charged,
        charge.offense_date,
        court_case.date_convicted,
        court_case.judicial_district_code,
        IFNULL(is_controlling, false) AS is_controlling
      FROM
        `{project_id}.{base_dataset}.state_supervision_sentence` ss
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge_supervision_sentence_association`  
      USING (supervision_sentence_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge` charge
      USING (charge_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_court_case` court_case
      USING (court_case_id)
    ), incarceration AS (
      SELECT
        inc.state_code,
        inc.person_id,
        inc.external_id,
        'INCARCERATION' as sentence_type,
        inc.incarceration_sentence_id as sentence_id,
        inc.start_date,
        COALESCE(inc.completion_date, inc.projected_max_release_date) as completion_date,
        charge.county_code,
        charge.date_charged,
        charge.offense_date,
        court_case.date_convicted,
        court_case.judicial_district_code,
        IFNULL(is_controlling, false) AS is_controlling
      FROM
        `{project_id}.{base_dataset}.state_incarceration_sentence` inc
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge_incarceration_sentence_association`  
      USING (incarceration_sentence_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge` charge
      USING (charge_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_court_case` court_case
      USING (court_case_id)
    ), sentences AS (
      SELECT * FROM supervision
      UNION ALL
      SELECT * FROM incarceration
    )
    
    SELECT
      state_code,
      person_id,
      external_id,
      sentence_type,
      sentence_id,
      start_date,
      completion_date,
      offense_date,
      date_charged,
      date_convicted,
      -- Trims the leading and trailing whitespaces from the judicial_district_code --
      TRIM(COALESCE(sentences.judicial_district_code, county_judicial_district.judicial_district_code)) as judicial_district_code,
      is_controlling
    FROM
      sentences
    -- Join on the county_code from the charge to get the judicial district of the sentence
    LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_county_codes` county_judicial_district
    USING (state_code, county_code)
    """

SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
    view_query_template=SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE,
    description=SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER.build_and_print()
