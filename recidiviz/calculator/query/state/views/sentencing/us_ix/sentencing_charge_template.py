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
"""View logic to prepare US_IX Sentencing charge data for PSI tools"""


# TODO(#40354): Once we've switched to sentencing v2 we should have non-sentenced charges and we won't have to manually include offenses from scl_OffenseType_latest.
US_IX_SENTENCING_CHARGE_TEMPLATE = """
WITH
  charges AS (
  SELECT
    description,
    -- Only count the number of times a charge appears in the past two years to determine frequency
    SUM(
    IF
      (DATE_DIFF(CURRENT_DATE("US/Eastern"), offense_date, YEAR) <= 2, 1, 0)) AS frequency,
  IF
    -- Determine if a charge is violent based what percentage of the time it is tagged as such, rounded down or up
    (ROUND(SUM(
        IF
          (is_violent, 1, 0)) / COUNT(*)) = 1,
      TRUE,
      FALSE) AS is_violent,
  IF
    -- Determine if a charge is a sex offense based what percentage of the time it is tagged as such, rounded down or up
    (ROUND(SUM(
        IF
          (is_sex_offense, 1, 0)) / COUNT(*)) = 1,
      TRUE,
      FALSE) AS is_sex_offense,
  IF
    -- Determine if a charge is a drug offense based what percentage of the time it is tagged as such, rounded down or up
    (ROUND(SUM(
        IF
          (is_drug, 1, 0)) / COUNT(*)) = 1,
      TRUE,
      FALSE) AS is_drug,
  FROM
    `{project_id}.normalized_state.state_charge`
  WHERE
    state_code = "US_IX"
    AND description IS NOT NULL
  GROUP BY
    description),
  non_sentenced_charges AS (
  SELECT
    DISTINCT OffenseTypeDesc AS description,
    0 AS frequency,
    FALSE AS is_violent,
    FALSE AS is_sex_offense,
    FALSE AS is_drug,
  FROM
    `{project_id}.{us_ix_raw_data_up_to_date_dataset}.scl_OffenseType_latest`
  WHERE
    OffenseTypeDesc NOT IN (
    SELECT
      description
    FROM
      charges)
    AND Inactive = '0'
    AND LOWER(OffenseTypeDesc) IN (LOWER('Computer Crime-Alters, Damages or Destroys Computer or Computer Equipment'))),
  mms AS (
  SELECT
    OffenseName,
    ARRAY_AGG(TO_JSON((
        SELECT
          AS STRUCT SentenceType,
          MinimumSentenceLength,
          MaximumSentenceLength,
          StatuteNumber,
          StatuteLink ) )
    ORDER BY
      SentenceType) AS mandatory_minimums
  FROM
    `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_mandatory_minimums_latest`
  GROUP BY
    OffenseName )
SELECT
  "US_IX" AS state_code,
  description AS charge,
  frequency,
  is_violent,
  is_sex_offense,
  is_drug,
  clean_offense.ncic_category_uniform,
  mandatory_minimums
FROM (
  SELECT
    *
  FROM
    charges
  UNION ALL
  SELECT
    *
  FROM
    non_sentenced_charges)
LEFT JOIN
  `{project_id}.reference_views.cleaned_offense_description_to_labels` clean_offense
  ON description = clean_offense.offense_description
LEFT JOIN
  mms
ON
  description = OffenseName
"""
