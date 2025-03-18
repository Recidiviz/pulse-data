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

# This query links sentences to the person ID of the person
# serving that sentence and the type of sentence (INCARCERATION or PROBATION).
# The output of this query should be used to subset sentence IDs to what we consider valid.
# This ensures we don't partially hydrate invalid sentences when hydrating sentence
# lengths and statuses, and that we don't unintentionally cross-hydrate incarceration
# and probation sentences with the same intr_case_num.

# TODO(#38008): Find out what the situation is for cases that appear in crt_case but not est_dt.
# Add a UNION ALL here with their case if we need to include it. Also add sentence lengths for those if needed.
# ------------------------------------------------------------------------

from recidiviz.persistence.entity.state.entities import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
)

VALID_PEOPLE_AND_SENTENCES = f"""
-- To create proper external IDs we need to know if each sentence is to incarceration or probation,
-- since the external IDs are otherwise the same.
SELECT DISTINCT
  crt.ofndr_num,
  crt.intr_case_num,
  'INCARCERATION' AS sentence_type,
FROM {{crt_case}} crt
JOIN 
  {{rfrd_ofnse}} ofnse
USING 
  (intr_case_num)
JOIN 
  {{est_dt}} dt
USING 
  (intr_case_num)
-- Exclude sentences with no imposed date. These are all in the past, and make up 5% of closed cases.
WHERE sent_dt IS NOT NULL
-- Exclude sentences with unreasonable sentence dates. This only excludes 8 rows.
AND cast(sent_dt as datetime) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' and '{STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND}'
-- Rows that have exclusively incarceration sentence information are ingested as prison sentences.
AND sched_expire_dt IS NOT NULL 
-- These are dates corresponding to probation sentences. We want to ensure they are null.
AND sched_trmn_dt IS NULL AND early_trmn_dt IS NULL

UNION ALL 

SELECT DISTINCT
  crt.ofndr_num,
  crt.intr_case_num,
  'PROBATION' AS sentence_type,
FROM {{crt_case}} crt
JOIN 
  {{rfrd_ofnse}} ofnse
USING 
  (intr_case_num)
JOIN 
  {{est_dt}} dt
USING 
  (intr_case_num)
-- Exclude sentences with no imposed date. These are all in the past, and make up 5% of closed cases.
WHERE sent_dt IS NOT NULL
-- Exclude sentences with unreasonable sentence dates. This only excludes 8 rows.
AND cast(sent_dt as datetime) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' and '{STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND}'
-- Rows with any probation sentence information are ingested as probation sentences.
-- This includes cases where a prison sentence is suspended while a person serves probation.
AND (sched_trmn_dt IS NOT NULL OR early_trmn_dt IS NOT NULL)
"""
