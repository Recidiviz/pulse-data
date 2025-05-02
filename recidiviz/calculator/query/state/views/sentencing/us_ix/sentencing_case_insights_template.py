# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View logic to prepare US_IX recidivism and disposition data for PSI tools"""

# TODO(#39399): Switch over to risk-specific assessment scores in this view (since
# `assessment_score`, `assessment_score_start`, `assessment_score_end`, and related
# fields aren't generally specific to assessments in the 'RISK' class).
US_IX_SENTENCING_CASE_INSIGHTS_TEMPLATE = """
SELECT DISTINCT
  state_code,
  gender,
  assessment_score_bucket_start,
  assessment_score_bucket_end,
  most_severe_description,
  recidivism_rollup,
  recidivism_num_records,
  recidivism_series,
  recidivism_probation_series,
  recidivism_rider_series,
  recidivism_term_series,
  disposition_num_records,
  disposition_probation_pc,
  disposition_rider_pc,
  disposition_term_pc,
  dispositions
FROM
  `{project_id}.sentencing.case_insights_rates`
WHERE
  state_code = "US_IX"
"""
