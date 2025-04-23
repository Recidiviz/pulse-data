# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
This module contains views to subset data for all sentencing entities,
as well as helpful constants to avoid multiple joins in queries.
"""

# This LOOKUP_ID maps to the 'Other' county,
# which is seen with interstate compact sentences.
OUT_OF_STATE_COUNTY_ID = "1783"

# These LOOKUP_IDs correspond to capital punishment
CAPITAL_PUNISHMENT_IDS = ("5381", "5382")

# This LOOKUP_ID corresponds to a parole elligible sentence
PAROLE_POSSIBLE_ID = "12068"

# This LOOKUP_ID corresponds to a consecutive sentence
CONSECUTIVE_SENTENCE_ID = "10635"

# These LOOKUP_IDs correspond to a life sentence
LIFE_SENTENCE_IDS = (
    "5383",
    "5384",
    "5385",
    "5386",
    "5387",
)


# This query links incarceration sentences to the person ID of the person
# serving that sentence. The output of this query should be used to
# subset sentence IDs to what we consider valid. This ensures we
# don't partially hydrate invalid sentences when hydrating sentence
# lengths and statuses.
# ------------------------------------------------------------------------
# TODO(#30796): Figure out if there are probation or parole sentence
#               available to hydrate.
#               Some sentences are through an interstate compact.
# TODO(#33341): Figure out how to incorporate vacated sentences.
VALID_PEOPLE_AND_SENTENCES = """
SELECT
    OFFENSE_ID,
    COMMITMENT_ID,
    SC_EPISODE_ID,
    PERSON_ID,
    COALESCE(FINAL_OFFENSE_ID_ML, FINAL_OFFENSE_ID) AS FINAL_OFFENSE_ID -- used to find the 'controlling' charge
FROM
    {AZ_DOC_SC_OFFENSE} AS sentence
-- Many sentences can be linked to a single commitment
JOIN
    {AZ_DOC_SC_COMMITMENT} AS commitment 
USING
    (COMMITMENT_ID)
-- Many commitments can be linked to the same episode, but
-- every commitment is linked to a single episode
-- Episodes are essentially sentence groups
JOIN
    -- This has the common IDs to episodes, SC_EPISODE_ID and DOC_ID
    {AZ_DOC_SC_EPISODE}
USING
    (SC_EPISODE_ID)
-- An episode belongs to a single person and is linked via DOC_ID
JOIN
    {DOC_EPISODE}
USING
    (DOC_ID)
LEFT JOIN 
    {LOOKUPS} status_lookup 
ON 
    sentence.SENTENCE_STATUS_ID = status_lookup.LOOKUP_ID
WHERE 
    -- These are often vacated sentences.
    -- recall this is not imposed_date
    sentence.SENTENCE_BEGIN_DTM != 'NULL'
AND
    sentence.SENTENCE_BEGIN_DTM IS NOT NULL
AND
    UPPER(status_lookup.description) NOT LIKE "%VACATE%"
"""
