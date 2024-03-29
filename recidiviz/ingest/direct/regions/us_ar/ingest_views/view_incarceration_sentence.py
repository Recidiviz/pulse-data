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
"""Query containing incarceration sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

-- At sentencing, AR splits each sentence into components, each of which is associated with
-- one or more statutes (and potentially multiple counts thereof), and has one or more sentence types;
-- the components are not necessarily unique, so there may be multiple components with essentially identical data.
-- These components are grouped under a COMMITMENTPREFIX, with each component identified with a SENTENCECOMPONENT ID,
-- unique within the person's COMMITMENTPREFIX.

-- The incarceration-related components are then synthesized and used to calculate the actual details of a person's sentence,
-- which are stored in SENTENCECOMPUTE. Each entry in this table (identified by a unique combination of OFFENDERID,
-- COMMITMENTPREFIX, and SENTENCECOUNT) is what will actually be considered a 'sentence' during ingest, as it
-- reflects the actual piece of a sentence to be served, not just inputs into sentencing determinations (though concurrent,
-- non-controlling sentences are still included). Components get 'rolled up' into these final sentences, such that each
-- sentence can be associated with a single component (using SENTENCECOUNT = SENTENCECOMPONENT), but the other components
-- that may have gone into the sentence are not specified.

-- Note that the sentences in SENTENCECOMPUTE are updated in place whenever the maximum/projected release dates change, such
-- that the sentence will always reflect the most current sentence adjustment; prior versions of the sentence can be 
-- found in RELEASEDATECHANGE, and the most recent of these versions will line up with SENTENCECOMPUTE. 

SELECT 
  compute.*,

  STATUTE1,
  STATUTE2,
  STATUTE3,
  STATUTE4,
  JUDGEPARTYID,
  FELONYCLASS,
  NUMBERCOUNTS,
  SERIOUSNESSLEVEL,
  SENTENCETYPE,
  SENTENCETYPE2,
  SENTENCETYPE3,
  SENTENCETYPE4,
  -- SENTENCEBEGINDATE is the date the sentence is imposed; there's also a SENTENCEIMPOSEDDATE column in SENTENCECOMPONENT, 
  -- but that value is closer to our conception of an effective date (though TIMESTARTDATE in SENTENCECOMPUTE is actually what 
  -- is used for effective date, as SENTENCEIMPOSEDDATE always provides the effective date for the full sentence, while TIMESTARTDATE is
  -- adjusted as needed for consecutive sentences.)
  SENTENCEBEGINDATE, 
  -- A sentence can have 2 offense dates, but only one is supported in the incarceration sentence schema, so the earliest non-null date
  -- is selected here.
  NULLIF(
    LEAST(
      IF(OFFENSEDATE = '1000-01-01 00:00:00', '9999-01-01 00:00:00', OFFENSEDATE),
      IF(OFFENSEDATE2 = '1000-01-01 00:00:00', '9999-01-01 00:00:00', OFFENSEDATE2)
    ), '9999-01-01 00:00:00'
  ) AS OFFENSEDATE,

  TYPEORDER,
  COUNTYOFCONVICTION,
  COURTID,
  OTHSTATE

FROM (
  SELECT 
    OFFENDERID,
    COMMITMENTPREFIX,
    SENTENCECOUNT,
    SENTENCESTATUSFLAG,
    CNTYJAILTIME,
    PAROLEREVOKEDFLAG,
    NULLIF(TIMESTARTDATE, '1000-01-01 00:00:00') AS TIMESTARTDATE,
    NULLIF(MAXRELEASEDATE, '1000-01-01 00:00:00') AS MAXRELEASEDATE,
    NULLIF(MINIMUMRELEASEDATE, '1000-01-01 00:00:00') AS MINIMUMRELEASEDATE,
    NULLIF(PAROLEELIGIBILITYDATE, '1000-01-01 00:00:00') AS PAROLEELIGIBILITYDATE,
    NULLIF(SENTENCEENDDATE, '1000-01-01 00:00:00') AS SENTENCEENDDATE,
    MAXSENTDAYSMR,
    TIMETOSERVBFORPAROLE,
    NETGTBEFOREPE,
    MRRULINGINDICATOR
  FROM {SENTENCECOMPUTE}
) compute
LEFT JOIN {SENTENCECOMPONENT} component
ON compute.OFFENDERID = component.OFFENDERID AND 
  compute.COMMITMENTPREFIX = component.COMMITMENTPREFIX AND
  compute.SENTENCECOUNT = component.SENTENCECOMPONENT
LEFT JOIN {COMMITMENTSUMMARY} cs
ON compute.OFFENDERID = cs.OFFENDERID AND 
  compute.COMMITMENTPREFIX = cs.COMMITMENTPREFIX 
WHERE COALESCE(SENTENCETYPE, SENTENCETYPE2, SENTENCETYPE3,SENTENCETYPE4) IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="incarceration_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
