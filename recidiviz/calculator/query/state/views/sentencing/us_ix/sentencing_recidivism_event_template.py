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
"""View logic for US_IX recidivism events.

This template uses sentencing v1 and captures initial sentences.

This is augmented in the generic recidivism_event.py with all actual state prison incarceration events.
"""


US_IX_SENTENCING_RECIDIVISM_EVENT_TEMPLATE = """
    SELECT
      sigs.state_code,
      sigs.person_id,
      sigs.date_imposed as recidivism_date,
    FROM `{project_id}.sessions.sentence_imposed_group_summary_materialized` sigs
    JOIN `{project_id}.sessions.sentences_preprocessed_materialized` sp
      ON sigs.parent_sentence_id = sp.sentence_id
    WHERE JSON_EXTRACT_SCALAR(sp.sentence_metadata, "$.sentence_event_type") = 'INITIAL'
        AND sigs.state_code = 'US_IX'
"""
