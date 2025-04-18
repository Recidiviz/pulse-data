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
"""View logic for US_ND recidivism events.

This template uses sentencing v2 and captures all state prison or probation sentences for felonies. It groups by court
case id, taking the earliest instance, and uses the earlier of offense date and imposed date (usually the former).

This is augmented in the generic recidivism_event.py with all actual state prison incarceration events.
"""


US_ND_SENTENCING_RECIDIVISM_EVENT_TEMPLATE = """
    WITH unique_events AS ( 
    SELECT
      state_code,
      person_id,
      SPLIT(sentence_group_external_id, "|")[SAFE_OFFSET(0)] AS court_case_id,
      sentence_type,
      MIN(LEAST(IFNULL(offense_date, "9999-12-31"), imposed_date)) AS recidivism_date,
    FROM `{project_id}.sentence_sessions_v2_all.sentences_and_charges_materialized`
    WHERE state_code = 'US_ND'
      AND IFNULL(classification_type, "UNKNOWN") = "FELONY"
      AND sentence_type IN ("STATE_PRISON", "PROBATION")
    GROUP BY 1, 2, 3, 4
    )
    SELECT
        state_code,
        person_id,
        recidivism_date
    FROM unique_events
    """
