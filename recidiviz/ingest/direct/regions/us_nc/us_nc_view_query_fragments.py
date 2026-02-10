# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helper SQL fragments that import raw tables for NC
"""


def compute_stints() -> str:
    return """
    -- We use OPUS and CDBGDTSP (start_date) as the primary key in the offenders file. If the start date
    -- changes, but the person was still continously on supervision, we have been told
    -- by NCDIT to interpret this as a correction in the start date. However, because
    -- CDBGDTSP is included in the primary key for `offenders`, this will appear as
    -- if the original OPUS + Start Date row was deleted (is_deleted = True), and a new
    -- record with OPUS + New Start Date will appear. This CTE ignores the deleted row
    -- in this situation -- later, we'll make sure to use the most recent start time as
    -- the sentence start time.
    drop_unecessary_deletions AS (
        SELECT *
        FROM {offenders@ALL_WITH_DELETED}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY opus, update_datetime ORDER BY is_deleted) = 1
    ), 
    -- Identify a stint on supervision by continous presence in the offenders file and
    -- begin creating a "sentence" flag.
    with_change_flag AS (
        SELECT
            *,
            -- This creates a change flag when a new "sentence" begins. See below for
            -- why we call this a "sentence"
            CASE
                WHEN 
                    NOT is_deleted AND 
                    LAG(is_deleted) OVER (opus) AND
                    -- Sometimes people are deleted from offenders, then return a few
                    -- days later with the same date. I think this is mostly due to
                    -- changes DIT made around 12/12, but shouldn't happen as often
                    -- after that.
                    CDBGDTSP != LAG(CDBGDTSP) OVER (opus)
                THEN 1
                ELSE 0
            END AS sentence_flag
        FROM drop_unecessary_deletions
        WINDOW opus AS (PARTITION BY OPUS ORDER BY update_datetime)
    ),
    -- Create a sentence identifier by doing a cumulative sum of the sentence flags. We call
    -- this "sentence_seq" because it effectively flags different stints on supervision.
    -- So if someone leaves supervision, then returns months later, this flag will
    -- increment, reflecting the new sentence
    with_groups AS (
        SELECT
            *,
            SUM(sentence_flag) OVER (PARTITION BY OPUS ORDER BY update_datetime) AS sentence_seq
        FROM with_change_flag
    ),
  """
