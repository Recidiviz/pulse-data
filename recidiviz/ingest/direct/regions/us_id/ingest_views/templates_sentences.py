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
from enum import auto, Enum


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
    ORDER BY
        CAST(mitt_srl AS INT64),
        CAST(sent_no AS INT64)
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
"""


def _get_relevant_sentence_query_for_type(sentence_type: SentenceType) -> str:
    if sentence_type == SentenceType.INCARCERATION:
        return INCARCERATION_SENTENCES_QUERY
    if sentence_type == SentenceType.SUPERVISION:
        return PROBATION_SENTENCES_QUERY

    raise ValueError(f"Unexpected discharge type {sentence_type}")


def _create_sentence_query_fragment_args(add_supervision_args: bool = False) -> str:
    """Helper method that creates query parameters meant for combining sentences rows with their corresponding
    amendment rows. If |add_supervision_args| is True, adds arguments unique to supervision sentences.
    """
    base_sentence_args = [
        "mitt_srl",
        "sent_no",
        "sent_disp",
    ]
    supervision_base_sentence_args = ["prob_no"]

    amended_sentence_args = [
        "off_dtd",
        "off_cat",
        "off_cd",
        "off_deg",
        "off_cnt",
        "sent_min_yr",
        "sent_min_mo",
        "sent_min_da",
        "sent_max_yr",
        "sent_max_mo",
        "sent_max_da",
        "law_cd",
        "vio_doc",
        "vio_1311",
        "lifer",
        "enhanced",
        "govn_sent",
        "sent_gtr_dtd",
        "sent_beg_dtd",
        "sent_par_dtd",
        "sent_ind_dtd",
        "sent_ft_dtd",
        "sent_sat_dtd",
        "consec_typ",
        "consec_sent_no",
        "am_sent_no",
        "string_no",
    ]
    supervision_amended_sentence_args = [
        "prob_strt_dtd",
        "prob_yr",
        "prob_mo",
        "prob_da",
        "prob_end_dtd",
    ]

    if add_supervision_args:
        base_sentence_args += supervision_base_sentence_args
        amended_sentence_args += supervision_amended_sentence_args

    n_spaces = 8 * " "
    query_fragment = "\n"
    for arg in base_sentence_args:
        query_fragment += f"{n_spaces}COALESCE(s.{arg}, a.{arg}) AS {arg},\n"
    for arg in amended_sentence_args:
        query_fragment += f"{n_spaces}COALESCE(a.{arg}, s.{arg}) AS {arg},\n"
    return query_fragment


def sentence_view_template(sentence_type: SentenceType) -> str:
    add_supervision_args = sentence_type == SentenceType.SUPERVISION
    return SENTENCE_QUERY_TEMPLATE.format(
        sentence_query=_get_relevant_sentence_query_for_type(sentence_type),
        sentence_args=_create_sentence_query_fragment_args(
            add_supervision_args=add_supervision_args
        ),
    )
