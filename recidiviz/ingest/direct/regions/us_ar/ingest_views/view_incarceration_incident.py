# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
violations AS (
-- Arkansas aggregates violation information differently than the zero-to-many incident
-- to outcome relationship used in our schema. Each row of violation data in DISCIPLINARYVIOLAT
-- contains all the information for one 'incident', but an 'incident' can consist of several
-- different violation codes (which are mapped to incident types in our schema) and several 
-- types of disciplinary action (mapped to incident outcomes in our schema).

-- This means that multiple incidents (as defined by our schema) can correspond to the same
-- set of outcomes, which makes fitting these two entities into the zero-to-many relationship
-- used in our schema difficult. To avoid ingesting duplicate entities, each violation code 
-- within an incident is ingested as its own StateIncarcerationIncident: if there are multiple
-- violation codes within the same DISCIPLINARYVIOLAT incident, then the ingested incidents will
-- each have the same set (represented as a JSON string) of StateIncarcerationIncidentOutcomes, 
-- identical except for in the external_id (which uses the violation_seq ID from the parent incident). 

-- TODO(#28118): Depending on how these entitities are used downstream, some amount of downstream deduplication
-- will likely be necessary, as any incident outcome associated with N violation codes will
-- look like the same outcome repeated N times. If/when this becomes necessary, this view
-- should be revisited to make sure the outcome arrays have their elements appropriately identified
-- to support downstream deduplication.
  SELECT 
    OFFENDERID,
    CAST(
      CONCAT(
        SPLIT(DISCIPLINARYVIOLATIONDATE, ' ')[OFFSET(0)],' ',DISCIPLINARYVIOLATIONTIME
      ) AS DATETIME
    ) AS VIOLATIONDATETIME,
    DISCVIOLFAC,
    DISCSTATUSCODE,
    DISCVIOLSTATUSDT,
    CAST(
      CONCAT(
        SPLIT(DISCVIOLHRGDT, ' ')[OFFSET(0)],' ',DISCVIOLHRGSTARTTM
      ) AS DATETIME
    ) AS HRGDATETIME,
    CAST(
      CONCAT(
        SPLIT(DATEDISCVIOLRPTRCVD, ' ')[OFFSET(0)],' ',TIMEDISCVIOLRPTRCVD
      ) AS DATETIME
    ) AS RPTDATETIME,
    NULLIF(DISCCODEVIOL1,'') AS DISCCODEVIOL1,
    NULLIF(DISCCODEVIOL2,'') AS DISCCODEVIOL2,
    NULLIF(DISCCODEVIOL3,'') AS DISCCODEVIOL3,
    NULLIF(DISCCODEVIOL4,'') AS DISCCODEVIOL4,
    NULLIF(DISCCODEVIOL5,'') AS DISCCODEVIOL5,
    NULLIF(DISCCODEVIOL6,'') AS DISCCODEVIOL6,
    NULLIF(REPRIMANDCODE,'N') AS REPRIMANDFLAG,
    NULLIF(ISOLATIONDAYSTOSERVE,'0') AS ISOLATIONDAYSTOSERVE,
    NULLIF(EXTRADUTYHOURSASSIGNED,'0') AS EXTRADUTYHOURSASSIGNED,
    NULLIF(
      GREATEST
        (
          RESTRICTDAYSVISITATION,
          RESTRICTDAYSPHONE,
          RESTRICTDAYSMAIL,
          RESTRICTDAYSCOMMISSARY,
          RESTRICTDAYSRECREATION
        ),
    '0'
    ) AS RESTRICTDAYSMAX,
    NULLIF(TIMEFORFEITINDAYS,'0') AS TIMEFORFEITINDAYS,
    NULLIF(RESTITUTIONTOBEPAID,'0.00') AS RESTITUTIONTOBEPAID
  FROM {DISCIPLINARYVIOLAT}
  -- Filter out parole-related disciplinary reports (which are in DISCRPTFMT '2') so 
  -- that incarceration incidents and supervision violations are kept distinct.
  WHERE DISCRPTFMT != '2' 
),
-- TODO(#28118): The following two CTEs use UNION ALL as a workaround for the UNPIVOT operator,
-- which isn't supported in postgres or in the BQ emulator. If the emulator starts supporting
-- UNPIVOT, these CTEs should get cleaned up.
unpivoted_violation_codes AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY OFFENDERID,VIOLATIONDATETIME ORDER BY DISCCODEVIOL) AS violation_seq 
  FROM (
    SELECT
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      1 AS VIOLATIONNUMBER,
      DISCCODEVIOL1 AS DISCCODEVIOL 
    FROM violations
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      2 AS VIOLATIONNUMBER,
      DISCCODEVIOL2 AS DISCCODEVIOL 
    FROM violations
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      3 AS VIOLATIONNUMBER,
      DISCCODEVIOL3 AS DISCCODEVIOL 
    FROM violations
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      4 AS VIOLATIONNUMBER,
      DISCCODEVIOL4 AS DISCCODEVIOL 
    FROM violations
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      5 AS VIOLATIONNUMBER,
      DISCCODEVIOL5 AS DISCCODEVIOL 
    FROM violations
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,REPRIMANDFLAG,ISOLATIONDAYSTOSERVE,EXTRADUTYHOURSASSIGNED,RESTRICTDAYSMAX,TIMEFORFEITINDAYS,RESTITUTIONTOBEPAID,
      6 AS VIOLATIONNUMBER,
      DISCCODEVIOL6 AS DISCCODEVIOL 
    FROM violations
  ) unioned_codes
  WHERE DISCCODEVIOL IS NOT NULL
),
unpivoted_violation_outcomes AS (
  SELECT *
  FROM (
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'REPRIMANDFLAG' AS VIOLATIONOUTCOME,
      REPRIMANDFLAG AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'ISOLATIONDAYSTOSERVE' AS VIOLATIONOUTCOME,
      ISOLATIONDAYSTOSERVE AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'EXTRADUTYHOURSASSIGNED' AS VIOLATIONOUTCOME,
      EXTRADUTYHOURSASSIGNED AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'RESTRICTDAYSMAX' AS VIOLATIONOUTCOME,
      RESTRICTDAYSMAX AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
    UNION ALL 
    SELECT 
      OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'TIMEFORFEITINDAYS' AS VIOLATIONOUTCOME,
      TIMEFORFEITINDAYS AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
    UNION ALL 
      SELECT OFFENDERID,VIOLATIONDATETIME,DISCVIOLFAC,DISCSTATUSCODE,DISCVIOLSTATUSDT,HRGDATETIME,RPTDATETIME,violation_seq,DISCCODEVIOL,
      'RESTITUTIONTOBEPAID' AS VIOLATIONOUTCOME,
      RESTITUTIONTOBEPAID AS OUTCOMEDETAILS 
    FROM unpivoted_violation_codes
  ) unioned_outcomes
  WHERE OUTCOMEDETAILS IS NOT NULL
)

SELECT  
  OFFENDERID,
  VIOLATIONDATETIME,
  DISCVIOLFAC,
  DISCSTATUSCODE,
  DISCVIOLSTATUSDT,
  HRGDATETIME,
  RPTDATETIME,
  DISCCODEVIOL,
  violation_seq,
  TO_JSON_STRING(ARRAY_AGG(STRUCT<outcome_type string,outcome_data string>(VIOLATIONOUTCOME,OUTCOMEDETAILS))) AS outcome_list
FROM unpivoted_violation_outcomes 
GROUP BY
  OFFENDERID,
  VIOLATIONDATETIME,
  DISCVIOLFAC,
  DISCSTATUSCODE,
  DISCVIOLSTATUSDT,
  HRGDATETIME,
  RPTDATETIME,
  DISCCODEVIOL,
  violation_seq
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDERID,VIOLATIONDATETIME",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
