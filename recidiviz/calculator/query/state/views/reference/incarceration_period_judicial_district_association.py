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
"""Maps incarceration_period_ids to the area of jurisdictional coverage of the court that sentenced the person to
     the incarceration."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME = 'incarceration_period_judicial_district_association'

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION = \
    """Maps incarceration_period_ids to the area of jurisdictional coverage of the court that sentenced the person to
     the incarceration. If there are multiple non-null judicial districts associated with a supervision period,
    prioritizes ones associated with controlling charges on the sentence."""

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH ips_to_sentence_groups AS (
      -- Sentence groups from incarceration sentences --
      SELECT
        ip.state_code,
        ip.person_id,
        incarceration_period_id,
        sentence_group_id
      FROM
        `{project_id}.{base_dataset}.state_incarceration_period` ip
      LEFT JOIN
        `{project_id}.{base_dataset}.state_incarceration_sentence_incarceration_period_association`
      USING (incarceration_period_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_incarceration_sentence` inc
      USING (incarceration_sentence_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_sentence_group`
      USING (sentence_group_id)
      
      UNION ALL
      
      -- Sentence groups from supervision sentences --
      SELECT
        ip.state_code,
        ip.person_id,
        incarceration_period_id,
        sentence_group_id
      FROM
        `{project_id}.{base_dataset}.state_incarceration_period` ip
      LEFT JOIN
        `{project_id}.{base_dataset}.state_supervision_sentence_incarceration_period_association`
      USING (incarceration_period_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_supervision_sentence` sup
      USING (supervision_sentence_id)
      LEFT JOIN
        `{project_id}.{base_dataset}.state_sentence_group`
      USING (sentence_group_id)
    ), ranked_judicial_districts AS (
      SELECT
        * EXCEPT(sentence_group_id),
        ROW_NUMBER() OVER (PARTITION BY person_id, incarceration_period_id ORDER BY judicial_district_code IS NULL, is_controlling DESC) AS ranking
      FROM
        ips_to_sentence_groups
      LEFT JOIN
        `{project_id}.{reference_dataset}.sentence_group_judicial_district_association`
      USING (state_code, person_id, sentence_group_id)
    )
    
    SELECT
      state_code,
      person_id,
      incarceration_period_id,
      judicial_district_code
    FROM
      ranked_judicial_districts
    WHERE ranking = 1
    -- This will limit the size of the output, improving Dataflow job speeds
    AND judicial_district_code IS NOT NULL
    """

INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_TABLES_DATASET,
    view_id=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
    view_query_template=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_QUERY_TEMPLATE,
    description=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER.build_and_print()
