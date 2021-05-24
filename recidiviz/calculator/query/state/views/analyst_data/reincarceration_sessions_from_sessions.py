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
"""A table of incarceration sessions that end in release with session identifiers for a subsequent reincarceration
sessions. Constructed directly from the sessions view."""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_NAME = (
    "reincarceration_sessions_from_sessions"
)

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_DESCRIPTION = """
    A table of incarceration sessions that end in release with session identifiers for a subsequent reincarceration 
    sessions.
    
    This view is constructed directly from sessions. The query does a self join where reincarceration 
    sessions are left joined to incarceration sessions that end in release. The alias "release_session" 
    refers to the session from which a person is released, and the alias "reincarceration_session" refers to the
    incarceration sessions that follow this release.
    
    The table is deduped so that each person's release session is associated with their first reincarceration, if there 
    are more than one. For example, if a person was incarcerated three distinct times, the third reincarceration gets 
    associated with only the second release, not the first. 
    
    At this point, reincarcerations and releases are identified mainly based on inflows and outflows rather than 
    start reasons and end reasons. TODO(#5920) - use start and end reasons instead as to be more internally consistent.
    
    Releases are identified as those incarceration sessions that (1) outflow to SUPERVISION or RELEASE, (2) are not 
    parole board hold sessions, (3) do not have an end reason of ESCAPE.
    
    Reincarcerations are then joined to these releases based on (1) being an incarceration session that is not a parole
    board hold (2) starting after the end date of the last supervision, (3) not inflowing from another incarceration
    term, unless that incarceration term is a parole board hold, (4) not a start reason of RETURN_FROM_ESCAPE.
    """

REINCARCERATION_SESSIONS_FROM_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cte AS
    (
    SELECT 
        release_session.person_id,
        release_session.state_code,
        release_session.session_id AS release_session_id,
        --release_date is the day after the session end_date which represents the last full day in the compartment
        DATE_ADD(release_session.end_date, INTERVAL 1 DAY) AS release_date,
        DATE_DIFF(release_session.last_day_of_data, release_session.end_date, DAY) - 1 AS days_since_release,
        CAST(FLOOR((DATE_DIFF(release_session.last_day_of_data, release_session.end_date,  DAY)-1)/30) AS INT64) AS months_since_release,
        CAST(FLOOR((DATE_DIFF(release_session.last_day_of_data, release_session.end_date, DAY)-1)/365.25) AS INT64) AS years_since_release,
        reincarceration_session.session_id AS reincarceration_session_id,
        reincarceration_session.start_date AS reincarceration_date,
        DATE_DIFF(reincarceration_session.start_date, release_session.end_date, DAY) - 1 AS release_to_reincarceration_days,
        CAST(CEILING((DATE_DIFF(reincarceration_session.start_date, release_session.end_date, DAY)-1)/30) AS INT64) AS release_to_reincarceration_months,
        CAST(CEILING((DATE_DIFF(reincarceration_session.start_date, release_session.end_date, DAY)-1)/365.25) AS INT64) AS release_to_reincarceration_years,
        ROW_NUMBER() OVER(PARTITION BY release_session.person_id, release_session.session_id ORDER BY reincarceration_session.session_id) AS rn
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` release_session
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` reincarceration_session 
        ON reincarceration_session.person_id = release_session.person_id 
        AND reincarceration_session.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
        AND reincarceration_session.start_date > release_session.end_date
        AND reincarceration_session.compartment_level_2 NOT IN ('PAROLE_BOARD_HOLD','TEMPORARY_CUSTODY', 'COMMUNITY_PLACEMENT_PROGRAM')
        AND (reincarceration_session.inflow_from_level_1 NOT IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE') 
            OR reincarceration_session.inflow_from_level_2 IN ('PAROLE_BOARD_HOLD', 'TEMPORARY_CUSTODY','COMMUNITY_PLACEMENT_PROGRAM'))
    WHERE release_session.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
        AND release_session.compartment_level_2 != 'COMMUNITY_PLACEMENT_PROGRAM'
        AND (release_session.outflow_to_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE','RELEASE', 'PENDING_SUPERVISION') OR
            release_session.outflow_to_level_2 = 'COMMUNITY_PLACEMENT_PROGRAM')
        AND release_session.compartment_level_2 NOT IN ('PAROLE_BOARD_HOLD','TEMPORARY_CUSTODY')
    ORDER BY 1,2
    )
    SELECT * EXCEPT(rn)
    FROM cte
    WHERE rn = 1
    ORDER BY 1,2,3
    """

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_NAME,
    view_query_template=REINCARCERATION_SESSIONS_FROM_SESSIONS_QUERY_TEMPLATE,
    description=REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER.build_and_print()
