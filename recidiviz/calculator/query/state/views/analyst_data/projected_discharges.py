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
"""List of people actively on supervision along with their projected completion date, across states"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_raw_projected_discharges import (
    US_ID_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_raw_projected_discharges import (
    US_MO_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_raw_projected_discharges import (
    US_ND_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_projected_discharges import (
    US_PA_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PROJECTED_DISCHARGES_VIEW_NAME = "projected_discharges"

PROJECTED_DISCHARGES_VIEW_DESCRIPTION = """List of people actively on supervision along with their projected completion date, across states"""

PROJECTED_DISCHARGES_QUERY_TEMPLATE = (
    """
    /*{description}*/
    WITH """
    + US_PA_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE
    + """
    , """
    + US_MO_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE
    + """
    , """
    + US_ID_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE
    + """
    , """
    + US_ND_RAW_PROJECTED_DISCHARGES_SUBQUERY_TEMPLATE
    + """
    , unioned AS (
        SELECT * FROM us_nd
        UNION ALL 
        SELECT * FROM us_id
        UNION ALL 
        SELECT * FROM us_mo
        UNION ALL 
        SELECT * FROM us_pa
    )
    SELECT 
        unioned.*, 
        ss.start_date AS supervision_start_date,
        agent.given_names AS supervising_officer_first_name,
        agent.surname AS supervising_officer_last_name,
        JSON_VALUE(person.full_name, '$.given_names') AS first_name,
        JSON_VALUE(person.full_name, '$.surname') AS last_name,
    FROM unioned
    INNER JOIN `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` ss
        ON unioned.person_id = ss.person_id
        AND unioned.date_of_supervision BETWEEN ss.start_date AND COALESCE(ss.end_date, '9999-01-01')
    LEFT JOIN 
        ( SELECT DISTINCT * EXCEPT(agent_id, agent_type) FROM `{project_id}.reference_views.augmented_agent_info`) agent
        ON unioned.supervising_officer_external_id = agent.external_id
        AND unioned.state_code = agent.state_code
    INNER JOIN `{project_id}.{base_dataset}.state_person` person
        ON unioned.person_id = person.person_id
    WHERE TRUE
    -- There are some duplicates at this point because of different last names, supervision_type, supervision levels, and officer/district IDs. We deterministically and arbitrarily choose one here
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY supervising_officer_last_name, supervision_type, CASE WHEN supervision_level = 'INTERNAL_UNKNOWN' THEN 1 ELSE 0 END, supervising_officer_external_id, supervising_district_external_id) = 1
    """
)

PROJECTED_DISCHARGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PROJECTED_DISCHARGES_VIEW_NAME,
    view_query_template=PROJECTED_DISCHARGES_QUERY_TEMPLATE,
    description=PROJECTED_DISCHARGES_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PROJECTED_DISCHARGES_VIEW_BUILDER.build_and_print()
