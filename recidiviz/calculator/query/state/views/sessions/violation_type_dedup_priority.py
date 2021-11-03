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
"""Dedup priority for violation types - replicates state specific logic
used in calculating dataflow supervision violation type"""
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_NAME = "violation_type_dedup_priority"

VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_DESCRIPTION = """Dedup priority for violation types - replicates state specific logic
    used in calculating dataflow supervision violation type"""

VIOLATION_TYPE_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    # TODO(#7912): implement full violation type enum coverage
    /*{description}*/
   WITH CTE as (
        SELECT
            'US_MO' AS state_code,
            SPLIT(violation_type,'-')[OFFSET(0)] AS violation_type,
            SPLIT(violation_type,'-')[OFFSET(1)] AS violation_subtype_temp,
            ROW_NUMBER() OVER() AS priority
        FROM UNNEST([
            'FELONY-FELONY',
            'MISDEMEANOR-MISDEMEANOR',
            'TECHNICAL-LAW_CITATION',
            'ABSCONDED-ABSCONDED',
            'MUNICIPAL-MUNICIPAL',
            'ESCAPED-ESCAPED',
            'TECHNICAL-SUBSTANCE_ABUSE',
            'TECHNICAL-TECHNICAL'
        ]) AS violation_type
            
        UNION ALL
    
        SELECT
            'US_PA' AS state_code,
            SPLIT(violation_type,'-')[OFFSET(0)] AS violation_type,
            SPLIT(violation_type,'-')[OFFSET(1)] AS violation_subtype_temp,
            ROW_NUMBER() OVER() AS priority
        FROM UNNEST([
            'LAW-LAW',
            'TECHNICAL-HIGH_TECH',
            'ABSCONDED-ABSCONDED',
            'TECHNICAL-SUBSTANCE_ABUSE',
            'TECHNICAL-ELEC_MONITORING',
            'TECHNICAL-MED_TECH',
            'TECHNICAL-LOW_TECH'
        ]) AS violation_type
        WITH OFFSET AS priority
    
        UNION ALL
    
        SELECT state_code, 
            SPLIT(violation_type,'-')[OFFSET(0)] AS violation_type,
            SPLIT(violation_type,'-')[OFFSET(1)] AS violation_subtype_temp,
            priority
        FROM (
            SELECT violation_type,
                    state_code,
            ROW_NUMBER() OVER(partition by state_code) AS priority
            FROM UNNEST(['US_ID','US_ND']) state_code CROSS JOIN 
                UNNEST([
                'FELONY-FELONY',
                'MISDEMEANOR-MISDEMEANOR',
                'LAW-LAW',
                'ABSCONDED-ABSCONDED',
                'MUNICIPAL-MUNICIPAL',
                'ESCAPED-ESCAPED',
                'TECHNICAL-TECHNICAL']) violation_type  
        )
    )
        SELECT * EXCEPT(violation_subtype_temp ),
        CASE WHEN violation_subtype_temp = '' THEN 'NONE' 
        ELSE violation_subtype_temp 
        END AS violation_type_subtype
    FROM cte
    """

VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=VIOLATION_TYPE_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
