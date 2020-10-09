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
"""Helper templates for the US_ID early discharge queries."""
from enum import auto, Enum


class EarlyDischargeType(Enum):
    INCARCERATION = auto()
    SUPERVISION = auto()


INCARCERATION_SENTENCE_IDS_QUERY = """
      SELECT
          mitt_srl,
          incrno,
          sent_no
      FROM
          {sentence}
      LEFT JOIN
          {mittimus}
      USING
          (mitt_srl)
      LEFT JOIN
          {sentprob}
      USING
          (mitt_srl, sent_no)
      WHERE
          {sentprob}.mitt_srl IS NULL
"""

PROBATION_SENTENCE_IDS_QUERY = """
      SELECT
          mitt_srl,
          incrno,
          sent_no
      FROM
          {sentence}
      LEFT JOIN
          {mittimus}
      USING
          (mitt_srl)
      LEFT JOIN
          {sentprob}
      USING
          (mitt_srl, sent_no)
      WHERE
          {sentprob}.mitt_srl IS NOT NULL
"""

EARLY_DISCHARGE_QUERY_TEMPLATE = """
WITH 
relevant_sentences AS ({relevant_sentence_query}),
filtered_early_discharge AS (
  SELECT 
    * 
  EXCEPT (
     # Ignore fields which are always NULL.
    jurisdiction_decision_code_id, 
    supervisor_review_date,
    jurisdiction_authize_date,
    # TODO(#3345): Remove these excludes once we have a full historical dump from an automated feed OR a reason to
    #  parse these fields. The format between the manual and automated exports differs for booleans (t vs True, etc)
    #  and money amounts (15.5 vs 15.50).
    meets_criteria,
    compliance,
    ncic_chk,
    restitution_init_bal)
  FROM 
    {{early_discharge}}
),
form_type AS (
  SELECT 
    early_discharge_form_typ_id,
    early_discharge_form_typ_desc,
  FROM 
    {{early_discharge_form_typ}}
),
jurisdiction_code AS (
  SELECT
    jurisdiction_decision_code_id,
    jurisdiction_decision_description
  FROM 
   {{jurisdiction_decision_code}}
),
all_early_discharge_info AS (
    SELECT 
      *
    FROM 
      {{early_discharge_sent}}
    LEFT JOIN 
      filtered_early_discharge
    USING 
      (early_discharge_id)
    LEFT JOIN 
      form_type
    USING 
      (early_discharge_form_typ_id)
    LEFT JOIN 
      jurisdiction_code
    USING 
      (jurisdiction_decision_code_id)
    JOIN 
      relevant_sentences
    USING
      (mitt_srl, sent_no)
)
SELECT
    {relevant_select_args}
FROM 
    all_early_discharge_info
"""


def _get_relevant_select_args(ids_only: bool) -> str:
    if ids_only:
        return 'ofndr_num, incrno, sent_no, early_discharge_id, early_discharge_sent_id'
    return '*'


def _get_relevant_sentence_query_for_type(discharge_type: EarlyDischargeType) -> str:
    if discharge_type == EarlyDischargeType.INCARCERATION:
        return INCARCERATION_SENTENCE_IDS_QUERY
    if discharge_type == EarlyDischargeType.SUPERVISION:
        return PROBATION_SENTENCE_IDS_QUERY

    raise ValueError(f'Unexpected discharge type {discharge_type}')


def early_discharge_view_template(discharge_type: EarlyDischargeType, ids_only: bool = False) -> str:
    return EARLY_DISCHARGE_QUERY_TEMPLATE.format(
        relevant_sentence_query=_get_relevant_sentence_query_for_type(discharge_type),
        relevant_select_args=_get_relevant_select_args(ids_only)
    )
