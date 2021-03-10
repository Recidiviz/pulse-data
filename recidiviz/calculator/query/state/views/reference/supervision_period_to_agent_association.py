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
"""Links SupervisionPeriods and their associated supervising agent."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME = (
    "supervision_period_to_agent_association"
)

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION = (
    """Links SupervisionPeriods and their associated agent."""
)

# TODO(#6314): Remove pylint ignore
# pylint: disable=anomalous-backslash-in-string
SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE = """
    /*{description}*/
    -- TODO(#6314): Remove the entire temp_us_pa_associations section
    WITH temp_us_pa_associations AS (
        WITH us_pa_supervision_info AS (
        SELECT *,
            FORMAT('%s#%s',
                FORMAT("%s|%s|%s", IFNULL(district_office, ''),
                    IFNULL(district_sub_office_id, ''),
                    IFNULL(supervision_location_org_code, '')),
                FORMAT("%s %s %s", IFNULL(CONCAT(agent_id, ':'), ''), IFNULL(first_name, ''), IFNULL(last_name, ''))
            ) AS agent_external_id
        FROM
        (SELECT
            parole_number as person_external_id,
            start_date,
            termination_date AS district_assignment_termination_date,
            UPPER(district_office) AS district_office,
            UPPER(district_sub_office_id) AS district_sub_office_id,
            UPPER(supervision_location_org_code) AS supervision_location_org_code,
            supervising_officer_name,
            TRIM(REGEXP_EXTRACT(supervising_officer_name, r'(\d+)$')) AS agent_id,
            UPPER(TRIM(SPLIT(REPLACE(supervising_officer_name, IFNULL(REGEXP_EXTRACT(supervising_officer_name, r'(\d+)$'), ""), ""), ',')[SAFE_OFFSET(1)])) as first_name,
            UPPER(TRIM(SPLIT(supervising_officer_name, ',')[SAFE_OFFSET(0)])) as last_name
            FROM `{project_id}.us_pa_ingest_views.us_pa_supervision_period_TEMP`
        )
    ), us_pa_sps AS (
        SELECT
            person_id,
            supervision_period_id,
            start_date,
            termination_date,
            external_id as person_external_id
        FROM
            (SELECT person_id, supervision_period_id, start_date, termination_date
            FROM `{project_id}.{base_dataset}.state_supervision_period`
            WHERE state_code = 'US_PA' AND external_id IS NOT NULL)
        LEFT JOIN 
            `{project_id}.{base_dataset}.state_person_external_id`
        USING (person_id)
        WHERE id_type = 'US_PA_PBPP'
    ), sps_to_info_ranked AS (
        SELECT
                *,
                ROW_NUMBER() OVER
                    (PARTITION BY supervision_period_id
                     -- Prioritizing non-null district/officer information and periods of assignment that lasted longer to avoid setting
                     -- the information from a single-day assignment on a longer sp with the same start_date
                     ORDER BY (agent_external_id IS NULL), IFNULL(district_assignment_termination_date, CURRENT_DATE()) DESC) AS inclusion_order
            FROM 
                us_pa_sps   
            LEFT JOIN
                us_pa_supervision_info 
            USING (person_external_id, start_date)
    )
        SELECT
            'US_PA' AS state_code,
            person_id,
            supervision_period_id,
            CAST(null AS INT64) as agent_id,
            agent_external_id
        FROM sps_to_info_ranked
        WHERE inclusion_order = 1
    )
    
    SELECT 
      sup.state_code, 
      sup.person_id,
      sup.supervision_period_id, 
      agents.agent_id, 
      CAST(agents.agent_external_id AS STRING) as agent_external_id, 
    FROM 
      `{project_id}.{base_dataset}.state_supervision_period` sup
    LEFT JOIN 
      `{project_id}.{reference_views_dataset}.augmented_agent_info` agents
    ON agents.state_code = sup.state_code AND agents.agent_id = sup.supervising_officer_id
    WHERE agents.external_id IS NOT NULL
    -- TODO(#6314): Delete this and the following UNION
    AND sup.state_code != 'US_PA'
    UNION ALL
    (SELECT * FROM temp_us_pa_associations);
"""

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
    view_query_template=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE,
    description=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER.build_and_print()
