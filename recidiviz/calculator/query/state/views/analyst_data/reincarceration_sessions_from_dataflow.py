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
sessions. Constructed from the recidivism count metric"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_NAME = (
    "reincarceration_sessions_from_dataflow"
)

REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_DESCRIPTION = """
    A table of incarceration sessions that end in release with session identifiers for a subsequent reincarceration 
    sessions.
    
    This query leverages the recidivism count dataflow metric to identify reincarcerations. This metric has a field
    "days_at_liberty" that is used to identify the prior session from which the person was released.
    
    The dataflow metric is used to identify release and re-incarceration sessions for those cases where someone was 
    reincarcerated, and this is left joined to compartment_sessions so that we have a record for every release.
    """

REINCARCERATION_SESSIONS_FROM_DATAFLOW_QUERY_TEMPLATE = """
    /*{description}*/
    # TODO(#7629): Investigate why this view is missing reincarceration events for all states except ND
    
    WITH recid_metric AS
    /*
    Dedup the dataflow metric which has multiple records for a single reincarceration, taking the record associated with
    the most recent release.
    */
    (
    SELECT 
        person_id, 
        reincarceration_date,
        days_at_liberty
    FROM
        (
        SELECT 
            *, 
            ROW_NUMBER() OVER(PARTITION BY person_id, reincarceration_date ORDER BY days_at_liberty) AS rn
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_recidivism_count_metrics_materialized`
        )
    WHERE rn = 1
    )  
    SELECT 
        release_session.person_id,
        release_session.state_code,
        release_session.session_id AS release_session_id,
        DATE_ADD(release_session.end_date, INTERVAL 1 DAY) AS release_date,
        DATE_DIFF(release_session.last_day_of_data, release_session.end_date, DAY) - 1 AS days_since_release,
        CAST(FLOOR((DATE_DIFF(release_session.last_day_of_data, release_session.end_date,  DAY)-1)/30) AS INT64) AS months_since_release,
        CAST(FLOOR((DATE_DIFF(release_session.last_day_of_data, release_session.end_date, DAY)-1)/365.25) AS INT64) AS years_since_release,
        reincarceration_session.session_id AS reincarceration_session_id,  
        recid_metric.reincarceration_date,
        recid_metric.days_at_liberty AS release_to_reincarceration_days,
        CAST(CEILING(recid_metric.days_at_liberty/30) AS INT64) AS release_to_reincarceration_months,
        CAST(CEILING(recid_metric.days_at_liberty/365.25) AS INT64) AS release_to_reincarceration_years,
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` release_session
    LEFT JOIN recid_metric 
        ON release_session.person_id = recid_metric.person_id
        AND release_session.end_date = DATE_SUB(recid_metric.reincarceration_date, INTERVAL recid_metric.days_at_liberty + 1 DAY) 
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` reincarceration_session
        ON reincarceration_session.person_id = recid_metric.person_id
        AND reincarceration_session.start_date = recid_metric.reincarceration_date
        AND reincarceration_session.compartment_level_2 NOT IN ('SHOCK_INCARCERATION')
    /* 
    Made the call below to identify releases based on a transition from incarceration to either supervision or release
    as opposed to leveraging end reasons explicitly identifying releases. With a fully-hydrated session end reason view
    this logic can be updated. One exception is an exclusion to not consider those that end with "ESCAPE" as releases - 
    this is non-rare occurrence in ND. TODO(#5920) - use start and end reasons instead as to be more internally 
    consistent.
    */
    WHERE release_session.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
        AND release_session.compartment_level_2 NOT IN ('COMMUNITY_PLACEMENT_PROGRAM','SHOCK_INCARCERATION')
        AND (release_session.outflow_to_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE','RELEASE','PENDING_SUPERVISION')
        OR release_session.outflow_to_level_2 = 'COMMUNITY_PLACEMENT_PROGRAM')
    """

REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_NAME,
    view_query_template=REINCARCERATION_SESSIONS_FROM_DATAFLOW_QUERY_TEMPLATE,
    description=REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_BUILDER.build_and_print()
