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
"""Helper templates for the US_ID sentence queries."""
from enum import Enum, auto

from recidiviz.utils.string import StrictStringFormatter


class SentenceType(Enum):
    INCARCERATION = auto()
    SUPERVISION = auto()


# Gets all probation sentences
PROBATION_SENTENCES_QUERY = """
    SELECT
        *
    FROM
        {sentence}
    # Inner join against sentences that are supervision sentences.
    JOIN
        {sentprob}
    USING
        (mitt_srl, sent_no)
"""

# Gets all incarceration sentences
INCARCERATION_SENTENCES_QUERY = """
    SELECT
        *
    FROM
        {sentence}
    JOIN
        # Inner join against sentences that are incarceration sentences.
        (SELECT
            mitt_srl,
            sent_no
        FROM
            {sentence}
        LEFT JOIN
            {sentprob}
        USING
            (mitt_srl, sent_no)
        WHERE
            {sentprob}.mitt_srl IS NULL)
    USING
        (mitt_srl, sent_no)
"""


# Get all sentences, either probation or incarceration
SENTENCE_QUERY_TEMPLATE = """WITH
relevant_sentences AS ({sentence_query}),
# Only non-amended sentences. Amended sentences are just snapshots of what a sentence looked like at the time of
# amendment. Our schema only keeps track of the most up-to-date version of all entities, and therefore we do not want to
# ingest these historical records.
non_amended_sentences AS (
    SELECT
        * 
      FROM
      relevant_sentences
    WHERE sent_disp != 'A'
)
# Join sentence level information with that from mittimus, county, judge, and offense to get descriptive details.
SELECT
    *
FROM
    {{mittimus}}
LEFT JOIN
    {{county}}
USING
    (cnty_cd)
# Only ignore jud_cd because already present in `county` table
LEFT JOIN
    (SELECT
            judge_cd,
            # TODO(#3345): Keep just judge_nam once we have a full historical dump from an automated feed.
            COALESCE(judge_name, judge_nam) AS judge_name
    FROM
        {{judge}})
USING
    (judge_cd)
JOIN
    non_amended_sentences
USING
    (mitt_srl)
LEFT JOIN
    (SELECT
        *
        # TODO(#3345): Remove this exclude once we have a full historical dump from an automated feed OR a reason to
        #  parse these fields. The format between manual and automatic differs by some 0 padding.
        EXCEPT (off_rank1, off_rank2, off_rank3)
    FROM {{offense}})
USING
    (off_cat, off_cd, off_deg)
LEFT JOIN
    {{offense_crime_grp}}
USING
    (off_cat, off_cd, off_deg)
LEFT JOIN
    {{crime_grp_cd}}
USING
    (crm_grp_cd)
"""


def _get_relevant_sentence_query_for_type(sentence_type: SentenceType) -> str:
    if sentence_type == SentenceType.INCARCERATION:
        return INCARCERATION_SENTENCES_QUERY
    if sentence_type == SentenceType.SUPERVISION:
        return PROBATION_SENTENCES_QUERY

    raise ValueError(f"Unexpected discharge type {sentence_type}")


def sentence_view_template(sentence_type: SentenceType) -> str:
    return StrictStringFormatter().format(
        SENTENCE_QUERY_TEMPLATE,
        sentence_query=_get_relevant_sentence_query_for_type(sentence_type),
    )
