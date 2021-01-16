# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Officer supervision district association by month."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_NAME = 'officer_supervision_district_association'

OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_DESCRIPTION = """
 Officer supervision district association.
 Identifies the district in which a given parole officer has the largest number of cases.
 """

OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH all_officers_to_person_count_in_district AS (
        SELECT 
                state_code, year, month, SPLIT(district, '|')[OFFSET(0)] as district_name,
                COUNT(DISTINCT person_id) AS person_count,
                officer_external_id
              FROM `{project_id}.{reference_views_dataset}.event_based_supervision_populations`
              WHERE district != 'ALL'
                -- Only the following supervision types should be included in the PO report --
                AND supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
                -- Only the following states are supported for the PO report --
                AND state_code = 'US_ID'
                AND officer_external_id IS NOT NULL
                AND {thirty_six_month_filter}
             GROUP BY state_code, year, month, officer_external_id, district_name),
    filtered_officers_to_person_count AS (
        SELECT  
          *,
          ROW_NUMBER() OVER (PARTITION BY state_code, year, month, officer_external_id ORDER BY
          person_count DESC, district_name) as district_inclusion_priority
    FROM all_officers_to_person_count_in_district)
    SELECT state_code, year, month, officer_external_id, district_name AS district
    FROM filtered_officers_to_person_count
    WHERE filtered_officers_to_person_count.district_inclusion_priority = 1
    """

OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_NAME,
    should_materialize=True,
    view_query_template=OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_QUERY_TEMPLATE,
    description=OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    thirty_six_month_filter=bq_utils.thirty_six_month_filter()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER.build_and_print()
