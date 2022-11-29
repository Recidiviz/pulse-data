# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MDOC incarceration period information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# MI categorizes the assessments they conduct into two categories: COMPAS assessments (which are developed and owned by Northpointe/Equivant)
# and non-COMPAS assessments (such as STATIC and STABLE).  COMPAS has its own database (the COMPAS database), so all COMPAS assessments data
# is stored there and will continue to be entered/stored there even with the introduction of COMS (the new data system that's coming)
# Some non-COMPAS assessments are also stored in the COMPAS database, but some non-COMPAS assessments are stored in other databases as well,
# and there's no standardized policy for where they should be stored.  Furthermore, with the introduction of COMS, some non-COMPAS assessments
# data is getting stored there too.  This query gathers all the assessment information that can be found in the COMPAS database, which should
# be sufficient for our upcoming needs since supervision level decisions only currently depend on COMPAS assessments (specifically the VFO recidivism
# risk and non-VFO recidivism risk scales).

VIEW_QUERY_TEMPLATE = """,

-- get all assessment records for COMPAS

-- COMPAS assessments are stored in the COASSESSMENT* tables
--   ADH_COASSESSMENT stores a record for each assessment
--   ADH_COASESSMENTSCORES stores a record for each score.  For COMPAS assessments, each assessment involves multiple scores (one for each scale).

COMPAS as (
  select 
        'COMPAS' as source,
        coassessment.RecId,
        shoffender.OffenderNumber,
        FkCoSyScale,
        corfscaleset.Name as corfscaleset_name,
        cosyscale.Name as cosyscale_name,
        (DATE(coassessment.dateofscreening)) as dateofscreening,
        coassessmentscores.RawScore,
        coassessmentscores.ScoreText,
        coassessment.FkShUserScreener,
        shuser.FirstName,
        shuser.MiddleInitial,
        shuser.LastName
  from {ADH_COASSESSMENT} coassessment
    inner join {ADH_SHOFFENDER} shoffender on coassessment.fkshoffender = shoffender.fkshperson
    left join {ADH_CORFSCALESET} corfscaleset on coassessment.FkCoRfScaleSet = corfscaleset.RecId
    left join {ADH_COASSESSMENTSCORES} coassessmentscores on coassessment.RecId = coassessmentscores.FkCoAssessment
    left join {ADH_COSYSCALE} cosyscale on coassessmentscores.FkCoSyScale = cosyscale.RecId
    left join {ADH_SHPERSON} shperson on shperson.recid = shoffender.fkshperson
    left join {ADH_SHUSER} shuser on shuser.recid = coassessment.fkshuserscreener
  where 
    coassessment.isdeleted = '0' and
    shperson.isdeleted = '0' and
    coassessment.iscomputed = '1' and 
    coassessment.iscompleted = '1'
),

-- STATIC and STABLE assessments are stored in the COALTERNATIVESCREENING* tables
--   COALTERNATIVESCREENING stores a record for each assessment
--   ADH_COALTERNATIVESCREENINGSCORE stores a record for each score.  For STABLE, we're just grabbing the total score.  For STATIC there's just one score.

-- get all assessment records for STATIC and STABLE
STATIC_STABLE as (
  SELECT  
          'STATIC_STABLE' as source,
          screening.RecId,   
          shoffender.offendernumber,
          FkCoSyScale,
          -- there are no applicable scale sets for STATIC/STABLE
          CAST(NULL as string) as corfscaleset_name,
          cosyscale.Name as cosyscale_name,
          (DATE(screening.dateofscreening)) as dateofscreening,
          score.rawscore,
          score.scoretext,
          screening.fkshuserscreener,
          shuser.FirstName,
          shuser.MiddleInitial,
          shuser.LastName
      FROM {ADH_COALTERNATIVESCREENING} screening
        left join {ADH_COALTERNATIVESCREENINGSCORE} score on score.FkCoAlternativeScreening = screening.RecId
        inner join {ADH_SHOFFENDER} shoffender on (screening.FkShOffender = shoffender.FkShPerson and screening.fkshagencycreatedby=shoffender.fkshagencycreatedby)
        left join {ADH_SHUSER} shuser on shuser.recid = screening.fkshuserscreener
        left join {ADH_COSYSCALE} cosyscale on score.FkCoSyScale = cosyscale.RecId
        left join {ADH_SHPERSON} shperson on shperson.recid = shoffender.fkshperson
    WHERE 
      screening.isdeleted = '0' and 
      shperson.isdeleted = '0' and
      (
       -- FkCoSyScreeningType 10 = 'Static-99R',FkCoSyScreeningType 8005 = 'Static-99R 2016'
       -- We use a date filter of 5/1/2016 because data before that is questionable/bad quality according to Ken/Jeff
       -- QUESTION: ^ would this be an issue? do we look at assessments in any historical analyses?
       (screening.FkCoSyScreeningType in ('10', '8005') and (DATE(screening.dateofscreening)) > DATE(2016,5,1)) 
        or 
       -- stable (only grabbing total score which is fkcosyscale '1119')
       (screening.FkCoSyScreeningType = '1000' and score.fkcosyscale = '1119')
      )
      and screening.IsCompleted = '1' 
      and screening.IsComputed = '1'
)

select 
  source,
  RecId,   
  offendernumber,
  FkCoSyScale,
  corfscaleset_name,
  cosyscale_name,
  dateofscreening,
  rawscore,
  scoretext,
  fkshuserscreener,
  FirstName,
  MiddleInitial,
  LastName
from (
  (select * from COMPAS)
  union all
  (select * from STATIC_STABLE)
) unioned
left join {ADH_OFFENDER} off on unioned.offendernumber = off.offender_number
inner join (select distinct offender_id from {ADH_OFFENDER_BOOKING}) book on off.offender_id = book.offender_id

"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mi",
    ingest_view_name="assessments_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    materialize_raw_data_table_views=False,
    order_by_cols="offendernumber, source, RecId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
