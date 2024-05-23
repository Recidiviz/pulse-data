# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Preprocessed OVG event (violation / incentive) data, used in us_ar_ovg_timeline."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_OVG_EVENTS_PREPROCESSED_VIEW_NAME = "us_ar_ovg_events_preprocessed"
US_AR_OVG_EVENTS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed OVG event
(violation / incentive) data, used in us_ar_ovg_timeline."""
US_AR_OVG_EVENTS_PREPROCESSED_QUERY_TEMPLATE = f"""
with
supervision_sessions AS (
  /*
  Supervision super-sessions are used as bounds for OVG 'periods'; someone's OVG score
  at a given time depends on the relevant events within their current period, but
  do not take events that fall outside that period into account. This is how we
  conceptualize the 'resetting' that occurs when someone finishes their term of
  supervision or starts a new one.
  */
  SELECT
    pei.external_id AS OFFENDERID,
    s.*
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions` s
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  USING(person_id)
  WHERE
    s.state_code = 'US_AR' AND
    pei.state_code = 'US_AR' AND
    s.compartment_level_1 = 'SUPERVISION'
),
preprocessed_interventions AS (
  SELECT
    pei.person_id,
    io.OFFENDERID,
    io.OFFENSEVIOLATIONLEVEL,
    io.INTERVENTDATETIME,
    io.OVGADJUSTMENTRSN IS NOT NULL AS is_adjusted,
    io.DATELASTUPDATE,
    ss.compartment_level_2_start AS supervision_type,
    ss.start_date AS super_session_start,
    ss.end_date AS super_session_end,
    CASE
      WHEN OFFENSEVIOLATIONLEVEL = '0' THEN 0
      WHEN OFFENSEVIOLATIONLEVEL = '1' THEN 5
      WHEN OFFENSEVIOLATIONLEVEL = '2' THEN 15
      WHEN OFFENSEVIOLATIONLEVEL in ('3','4') AND ss.compartment_level_2_start = 'PAROLE' THEN 40
      WHEN OFFENSEVIOLATIONLEVEL in ('3','4') AND ss.compartment_level_2_start = 'PROBATION' THEN 60
      ELSE NULL
    END AS points
  FROM (
    SELECT
      *,
      DATETIME(DATE(DATETIME(INTERVENTIONDATE)), PARSE_TIME('%H:%M:%S',INTERVENTIONTIME)) AS INTERVENTDATETIME
    FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.INTERVENTOFFENSE_latest`
  ) io
  LEFT JOIN `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.SUPVINTERVENTION_latest` si
  USING(OFFENDERID,INTERVENTIONDATE,INTERVENTIONTIME)
  LEFT JOIN supervision_sessions ss
  ON io.OFFENDERID = ss.OFFENDERID AND
    io.INTERVENTDATETIME BETWEEN ss.start_date AND {nonnull_end_date_clause('ss.end_date')}
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  ON io.OFFENDERID = pei.external_id AND pei.state_code='US_AR'
  WHERE si.INTERVENTIONSTATUS = 'A' AND io.OFFENSEVIOLATIONLEVEL != '0'
),
violations AS (
  -- Cleaned violation response data, including only approved responses with a non-zero
  -- point value.
  SELECT
    person_id,
    points,
    'US_AR' AS state_code,
    INTERVENTDATETIME AS start_date,
    -- Violations are given an adjustment date if they're marked as adjusted (is_adjusted = TRUE)
    -- and the adjustment occurs before the points from the violation would naturally decay.
    CASE
      WHEN is_adjusted AND DATE_DIFF(DATETIME(DATELASTUPDATE),INTERVENTDATETIME,YEAR) < 1
      THEN DATETIME(DATELASTUPDATE)
      ELSE CAST(NULL AS DATETIME)
    END AS adjustment_date,
    -- Outside of adjustments, violations are 'active' until they decay after a year OR
    -- until the OVG period ends, whichever comes first.
    CASE
      WHEN super_session_end IS NULL
      THEN DATETIME_ADD(INTERVENTDATETIME, INTERVAL 1 YEAR)
      ELSE LEAST(
        DATETIME_ADD(INTERVENTDATETIME,INTERVAL 1 YEAR),
        CAST(super_session_end AS DATETIME)
      ) END AS end_date,
    /*
    Violations are grouped into interventions, and the ID for a given intervention is
    used here to identify distinct violation events (which can contain multiple violations).
    Because violations only incur points based on the most severe non-adjusted violation
    within an intervention group, this ID will be used to group violations and identify
    which violation is 'controlling' at each point in time.
    */
    CAST(RANK() OVER (PARTITION BY OFFENDERID ORDER BY INTERVENTDATETIME) AS STRING) AS event_id,
    super_session_start,
    super_session_end
  FROM preprocessed_interventions
),
incentives AS (
  /*
  Incentives are handled similarly to violations, with a start date and an end date,
  except the end date depends on the super-session end date or the date on which the
  incentive makes someone's total OVG score negative (more on this later).
  */
  SELECT
    pei.person_id,
    -1 * CAST(INCENTIVEWEIGHT AS INT64) AS points,
    'US_AR' AS state_code,
    CAST(INCENTIVESTATUSDATE AS DATETIME) AS start_date,
    CAST(NULL AS DATETIME) AS adjustment_date,
    CASE
      WHEN ss.end_date IS NULL
      THEN CAST(NULL AS DATETIME)
      ELSE CAST(ss.end_date AS DATETIME)
    END AS end_date,
    'INCENTIVE' AS event_id,
    ss.start_date AS super_session_start,
    ss.end_date AS super_session_end
  FROM `{{project_id}}.{{us_ar_raw_data_up_to_date_dataset}}.SUPVINCENTIVE_latest` si
  LEFT JOIN supervision_sessions ss
  ON si.OFFENDERID = ss.OFFENDERID AND (
    (CAST(INCENTIVESTATUSDATE AS DATETIME) BETWEEN ss.start_date AND ss.end_date) OR
    (CAST(INCENTIVESTATUSDATE AS DATETIME) >= ss.start_date AND ss.end_date IS NULL)
  )
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  ON si.OFFENDERID = pei.external_id AND pei.state_code='US_AR'
  WHERE INCENTIVESTATUS = 'A' AND PRIOROVGPTS != '0'
)
-- To get only violations or incentives from this union, filter based on event_id, which
-- will be INCENTIVE for incentives and a numeric ID otherwise.
SELECT *
FROM violations
UNION ALL
SELECT *
FROM incentives
"""
US_AR_OVG_EVENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AR_OVG_EVENTS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_AR_OVG_EVENTS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_AR_OVG_EVENTS_PREPROCESSED_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    us_ar_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AR,
        instance=DirectIngestInstance.PRIMARY,
    ),
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_OVG_EVENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
