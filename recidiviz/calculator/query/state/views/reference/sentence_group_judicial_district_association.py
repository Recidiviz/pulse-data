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
"""Connects sentence_group_ids to judicial districts corresponding to the charges and court cases on the sentence
group's sentences."""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME = (
    "sentence_group_judicial_district_association"
)

SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION = """Connects sentence_group_ids to judicial districts corresponding to the charges and court cases on the sentence
     group's sentences."""

SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision AS (
      SELECT
        sg.state_code,
        sg.person_id,
        sg.sentence_group_id,
        charge.county_code,
        charge.date_charged,
        charge.offense_date,
        court_case.date_convicted,
        judicial_district_code,
        IFNULL(is_controlling, false) AS is_controlling
      FROM
        `{project_id}.{base_dataset}.state_sentence_group` sg
      LEFT JOIN
        `{project_id}.{base_dataset}.state_supervision_sentence` 
      USING (sentence_group_id)
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
        sg.state_code,
        sg.person_id,
        sg.sentence_group_id,
        charge.county_code,
        charge.date_charged,
        charge.offense_date,
        court_case.date_convicted,
        judicial_district_code,
        IFNULL(is_controlling, false) AS is_controlling
      FROM
        `{project_id}.{base_dataset}.state_sentence_group` sg
      LEFT JOIN
        `{project_id}.{base_dataset}.state_incarceration_sentence` 
      USING (sentence_group_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge_incarceration_sentence_association`  
      USING (incarceration_sentence_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_charge` charge
      USING (charge_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_court_case` court_case
      USING (court_case_id)
    ), sentence_groups AS (
      SELECT * FROM supervision
      UNION ALL
      SELECT * FROM incarceration
    )
    
    SELECT
      state_code,
      person_id,
      sentence_group_id,
      offense_date,
      date_charged,
      date_convicted,
      -- Trims the leading and trailing whitespaces from the judicial_district_code --
      TRIM(COALESCE(sentence_groups.judicial_district_code, county_judicial_district.judicial_district_code)) as judicial_district_code,
      is_controlling
    FROM
      sentence_groups
    -- Join on the county_code from the charge to get the judicial district of the sentence
    LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_county_codes` county_judicial_district
    USING (state_code, county_code)
    """

SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
    view_query_template=SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE,
    description=SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER.build_and_print()
