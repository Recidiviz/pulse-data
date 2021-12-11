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
"""Maps incarceration_period_ids to the area of jurisdictional coverage of the court
that sentenced the person to the incarceration."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME = (
    "incarceration_period_judicial_district_association"
)

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION = """Maps incarceration_period_ids to the area of jurisdictional coverage of the
    court that sentenced the person to the incarceration. If there are multiple 
    non-null judicial districts associated with a supervision period,
    prioritizes ones associated with controlling charges on the sentence. Uses
    US_ND-specific external_id matching logic to match incarceration periods to 
    related sentences."""


INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH ips_to_all_sentences AS (
      -- Incarceration periods with all sentences --
      SELECT
        period.state_code,
        period.person_id,
        period.external_id as period_external_id,
        incarceration_period_id,
        sent.external_id as sentence_external_id,
        offense_date,
        date_charged,
        date_convicted,
        judicial_district_code,
        is_controlling
      FROM 
         `{project_id}.{base_dataset}.state_incarceration_period` period
      LEFT JOIN
         # We need a set external_id to match to IPs
        (SELECT * FROM `{project_id}.{reference_views_dataset}.sentence_judicial_district_association` WHERE sentence_type = 'INCARCERATION') sent
      USING (state_code, person_id)
    ), overlapping_sentences AS (
        SELECT
            *
        FROM 
            ips_to_all_sentences 
        -- FOR USE IN US_ND ONLY --
        -- Connect IPs to ISs through the external_id --
        WHERE SPLIT(period_external_id, '-')[SAFE_OFFSET(0)] = SPLIT(sentence_external_id, '-')[SAFE_OFFSET(0)]
    ), ranked_judicial_districts AS (
      SELECT
        * ,
        ROW_NUMBER() OVER (PARTITION BY person_id, incarceration_period_id ORDER BY judicial_district_code IS NULL, is_controlling DESC) AS ranking
      FROM
        overlapping_sentences
    )

    SELECT
      state_code,
      person_id,
      incarceration_period_id,
      judicial_district_code
    FROM
      ranked_judicial_districts
    WHERE ranking = 1
    -- THIS VIEW CAN ONLY BE USED IN US_ND
    AND state_code = 'US_ND'
    -- This will limit the size of the output, improving Dataflow job speeds
    AND judicial_district_code IS NOT NULL
    """

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
    view_query_template=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE,
    description=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER.build_and_print()
