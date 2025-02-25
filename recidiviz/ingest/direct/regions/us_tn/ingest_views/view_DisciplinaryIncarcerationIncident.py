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
"""Query containing incarceration incident information extracted from the output of the `Disciplinary`,
 `DisciplinarySentence`, and `Incident` tables.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH  
-- Disciplinaryclass, disposition, and dispition date from Disciplinary table
disciplinary_base AS (
  SELECT DISTINCT
    OffenderID,
    IncidentID,
    DisciplinaryClass,
    REGEXP_REPLACE(OffenderAccount, '                                      ', ' ') AS OffenderAccount, 
    Disposition,
    DispositionDate, 
  FROM {Disciplinary}
), 
-- base incident information like the extent of the incident and location
inc_base AS (
  SELECT DISTINCT
    IncidentId,
    SiteId,
    Location,
    IncidentType,
    InjuryLevel,
    (DATE(IncidentDateTime)) AS IncidentDate,
  FROM {Incident}
),
-- infraction type is relevant for downstream tools because when it doesn't match with 
-- incident type then infraction type is used to score the incident for reclassifications
infractions AS (
  SELECT 
    OffenderID, 
    IncidentID, 
    InfractionType 
  FROM  {Infraction} inf
  WHERE Disposition = 'G'
  -- there should only be one guilty infractiontype per incident id but there are 2 with multiple 
  -- Guilty dispositions post 2000 (and more before) so this is to make sure we only get the latest
  QUALIFY ROW_NUMBER() OVER(PARTITION BY Offenderid, IncidentID ORDER BY LastUpdateDate DESC ) = 1
),
-- information about the disciplinary outcome of the incident 
disc_outcome AS (
  SELECT DISTINCT
    OffenderID,
    IncidentID,
    SentenceType,
    SentenceDays,
    SentenceMonths,
    SentenceWeeks,
    SentenceHours,
    SentenceDate,
  FROM {DisciplinarySentence}
), 
-- joining all incident, infraction, disciplinary, and disciplinaryoutcome information
full_inc_and_out AS (
  SELECT DISTINCT
    db.OffenderID,
    db.IncidentId,
    SiteId,
    Location,
    IncidentType,
    IncidentDate,
    DisciplinaryClass,
    OffenderAccount, 
    Disposition,
    DispositionDate,
    SentenceType,
    SentenceDays,
    SentenceMonths,
    SentenceWeeks,
    SentenceHours,
    SentenceDate,
    InjuryLevel,
    ROW_NUMBER() OVER (PARTITION BY db.OffenderID, db.IncidentId ORDER BY IncidentDate, DispositionDate, SentenceDate, IncidentType, DisciplinaryClass, SentenceType, SentenceDays, OffenderAccount, Location) AS SentId,
    InfractionType
  FROM disciplinary_base db
  LEFT JOIN inc_base
  USING (IncidentID)
  LEFT JOIN disc_outcome dis
  ON db.OffenderID = dis.OffenderID
  AND db.IncidentID = dis.IncidentID
  LEFT JOIN infractions inf
  ON db.OffenderID = inf.OffenderID
  AND db.IncidentID = inf.IncidentID
  WHERE DisciplinaryClass IS NOT NULL OR DispositionDate IS NULL
)
SELECT * FROM full_inc_and_out
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="DisciplinaryIncarcerationIncident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
