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
"""Query containing sentence information from the following tables: eomis_sentencecomponent, eomis_sentencecompute,
    eomis_commitmentsummary, eomis_sentcompchaining, eomis_sentcreditdbt, and eomis_inmateprofile
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
commitprefix AS (
    SELECT DISTINCT
        s.OFFENDERID,
        s.COMMITMENTPREFIX, 
        s.SENTENCECOMPONENT,
        s.COMPSTATUSCODE, 
        s.SENTENCETYPE,
        s.SENTENCEFINDINGDATE, 
        s.COMPONENTEFFECTIVEDATE, 
        IF(s.COMPSTATUSCODE='D' or COMPSTATUSCODE='N', COMPSTATUSDATE, null) AS COMPLETION_DATE, 
        CONCAT(c.OTHSTATE, '-', c.COUNTYOFCONVICTION) AS STATECOUNTY,
        s.LIFESENTENCETYPE AS LIFESENTENCE, 
        IF(t.SENTCREDITDEBITTYPE='12', t.CREDITDEBITDAYS, null) AS GOOD_TIME_DAYS, 
        s.HOWSERVEDSUMMARY,
        s.SENTENCEFINDINGFLAG,
        s.OFFENSEDATE,
        c.CURRCOMMITCONVICTIONDT, 
        s.CJISSTATUTECODE, #or s.STATUTE1
        s.CJISSTATUTEDESC, 
        s.SENTENCEINCHOATEFLAG AS ATTEMPTED,
        s.FELONYCLASS, 
        s.DOCOFFENSECODE, 
        s.MANDATORYVIOLENTSENTENCEFLAG AS VIOLENTSENT, 
        s.SEXCRIMEFLAG, 
        s.CJISCHARGECOUNT,
        s.SENTCOMPCOMMENTS,
        FROM {eomis_sentencecomponent} s
        LEFT JOIN  {eomis_commitmentsummary} c
        USING (OFFENDERID, COMMITMENTPREFIX)
        LEFT JOIN {eomis_sentcreditdbt} t
        USING (OFFENDERID, COMMITMENTPREFIX)
        WHERE s.SENTENCETYPE != 'PT'
),
sequences AS (
    SELECT * 
    FROM 
        (SELECT 
            sc.OFFENDERID, 
            scc.COMMITMENTPREFIX,
            sc.INCARCERATIONBEGINDT, 
            sc.CHAINSEQUENCE,
            sc.SENTENCESEQUENCE,
            sc.TIMESERVEDTO AS INITIAL_TIME_SERVED_DAYS, 
            sc.MINSENTDAYS AS MIN_LENGTH_DAYS, --or MINIMUMTERM, s.MINPRISONTERMD
            sc.MAXSENTDAYSMR AS MAX_LENGTH_DAYS, --MAXIMUMTERM, MAXPRISONTERMD, c.MAXIMUMPRISONTERM
            sc.MANDATORYRELEASEDATEESTIMATED,
            sc.EARNEDTIME, -- or EARNEDRELEASETIME,EARNEDTIMEPE,
            scc.GOVERNS, 
            scc.SENTENCECOMPONENT,
            # For a given sentence, there are a series of events in Chain Sequence or Sentence Sequence that shift things 
            # like initial days served. This window selects the most recent update to the sentence sequence and chain sequence 
            # to ensure we are using the most recently updated information for each sentence. 
            ROW_NUMBER() OVER (PARTITION BY sc.OFFENDERID,scc.COMMITMENTPREFIX, scc.SENTENCECOMPONENT ORDER BY sc.SENTENCESEQUENCE DESC, sc.CHAINSEQUENCE DESC) AS recency_rank
            FROM {eomis_sentencecompute} sc
        JOIN {eomis_sentcompchaining} scc
        USING (OFFENDERID, INCARCERATIONBEGINDT, CHAINSEQUENCE, SENTENCESEQUENCE)
        ) s
    WHERE recency_rank = 1
)
SELECT 
    cp.OFFENDERID,
    COMMITMENTPREFIX, 
    SENTENCECOMPONENT,
    COMPSTATUSCODE, 
    SENTENCETYPE,
    SENTENCEFINDINGDATE, 
    COMPONENTEFFECTIVEDATE, 
    p.PAROLEELIGIBILITYDATE AS PROJ_MIN_RELEASE, -- OR PAROLEELIGIBILITYDATECTRL, sc.PAROLEELIGIBILITYDATEACTUAL,PAROLEELIGIBILITYDATEESTIMATED,PAROLEELIGIBILITYDATEPOSSIBLE, PAROLEELIGIBILITYDATEPOTENTIAL,
    MANDATORYRELEASEDATEESTIMATED AS PROJ_MAX_RELEASE, -- Most up to date calculation of release date #TODO(#17262): Revisit with release list
    COMPLETION_DATE, #COMPSTATUSDATE
    p.PAROLEELIGIBILITYDATE, 
    STATECOUNTY,
    MIN_LENGTH_DAYS,
    MAX_LENGTH_DAYS,
    LIFESENTENCE, 
    INITIAL_TIME_SERVED_DAYS, 
    GOOD_TIME_DAYS, 
    EARNEDTIME, 
    HOWSERVEDSUMMARY, 
    SENTENCEFINDINGFLAG, 
    OFFENSEDATE,
    cp.CURRCOMMITCONVICTIONDT,
    p.PRIMARYOFFENSECURRINCAR, #ncic_code
    CJISSTATUTECODE,
    CJISSTATUTEDESC,
    ATTEMPTED,
    FELONYCLASS,
    DOCOFFENSECODE,
    VIOLENTSENT,
    SEXCRIMEFLAG,
    REPLACE(CJISCHARGECOUNT, '-', '') AS CJISCHARGECOUNT,
    SENTCOMPCOMMENTS,
    GOVERNS
FROM commitprefix cp
LEFT JOIN sequences seq 
USING (OFFENDERID, COMMITMENTPREFIX, SENTENCECOMPONENT)
LEFT JOIN {eomis_inmateprofile} p
ON cp.OFFENDERID = p.OFFENDERID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_co",
    ingest_view_name="IncarcerationSentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
