# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MDOC incarceration period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """,

-- aggregate supervision conditions so that it's one list of supervision conditions per legal order

supervision_conditions_per_order as (
  select 
    legal_order_id,
    STRING_AGG(distinct special_condition_id, ',' order by special_condition_id) as special_condition_id_list
  from {ADH_SUPERVISION_CONDITION}
  group by legal_order_id
),

-- compile all sentence records and narrow down to probation and prison

sentence_records as (
  select
    offender_sentence_id,
    book.offender_booking_id,
    sentence_status_id,
    sentence.offender_charge_id,
    sentence_type_id,
    (date(sentence.sentenced_date)) as sentenced_date,
    (date(sentence.effective_date)) as effective_date,
    (date(sentence.expiration_date)) as expiration_date,
    (date(sentence.closing_date)) as closing_date,
    min_length_days,
    min_length_months,
    min_length_years,
    max_length_days,
    max_length_months,
    max_length_years,
    sentence.closing_reason_code as sentence_closing_reason_code,
    legal.closing_reason_code as legal_closing_reason_code,
    sentence.legal_order_id,
    ref5.description as county,
    cond.special_condition_id_list,
    charge.jail_credit_days,
    charge.attempted_id,
    offense.offense_code,
    min_life_flag,
    max_life_flag,
    -- calculate previous sentence closing reason within each set of related sentence records
    -- related sentence records defined as same offender booking, legal order, and charge
    LAG(sentence.closing_reason_code) 
      OVER(PARTITION BY sentence.offender_booking_id, sentence.legal_order_id, sentence.offender_charge_id 
           ORDER BY sentenced_date, sentence.closing_date, offender_sentence_id) 
      as prev_sentence_closing_reason_code,
    -- calculate the first offender sentence id within each set of related sentence records  
    -- related sentence records defined as same offender booking, legal order, and charge 
    FIRST_VALUE(offender_sentence_id) OVER (
      PARTITION BY sentence.offender_booking_id, sentence.legal_order_id, sentence.offender_charge_id
      ORDER BY sentenced_date, sentence.closing_date, offender_sentence_id
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_offender_sentence_id
  FROM {ADH_OFFENDER_SENTENCE} sentence
    -- 10/7 update: revise to use inner join so that we won't be left with null state_person ever 
    -- (which would occur as a result of data quality issues or data pruning timing)
    INNER JOIN {ADH_OFFENDER_BOOKING} book ON sentence.offender_booking_id = book.offender_booking_id
    LEFT JOIN {ADH_LEGAL_ORDER} legal on sentence.legal_order_id = legal.legal_order_id
    LEFT JOIN supervision_conditions_per_order cond on sentence.legal_order_id = cond.legal_order_id
    LEFT JOIN {ADH_REFERENCE_CODE} ref5 on legal.county_code_id = ref5.reference_code_id
    LEFT JOIN {ADH_OFFENDER_CHARGE} charge on sentence.offender_charge_id = charge.offender_charge_id 
    LEFT JOIN {ADH_OFFENSE} offense on charge.offense_id = offense.offense_id    
  -- select probation, prison, and parole sentences (since parole sentences have different term end dates than their original prison sentence)
  where sentence_type_id in ('431', '430', '2165')
),

sentence_records_grouped as (
  SELECT
    *,
    case -- if closing reason is "Sentence Extended" or "Sentence Modified" then assign original_offender_sentence_id as first offender_sentence_id
         when prev_sentence_closing_reason_code in ('5283', '5287') then first_offender_sentence_id
         -- else assign original_offender_sentence_id as the offender_sentence_id for the record
         else offender_sentence_id
      end as original_offender_sentence_id
  FROM sentence_records
)

select
  offender_booking_id,
  offender_sentence_id,
  sentence_status_id,
  sentence_closing_reason_code,
  legal_closing_reason_code,
  sentence_type_id,
  sentenced_date,
  effective_date,
  closing_date,
  case when extract(YEAR from expiration_date) = 9999
       then DATE(9999,12,31)
       else expiration_date
       end as expiration_date,
  county,
  min_length_days,
  min_length_months,
  min_length_years,
  max_length_days,
  max_length_months,
  max_length_years,
  original_offender_sentence_id,
  special_condition_id_list,
  jail_credit_days,
  attempted_id,
  offense_code,
  min_life_flag,
  max_life_flag
from sentence_records_grouped
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="sentences_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    materialize_raw_data_table_views=False,
    order_by_cols="offender_booking_id, sentenced_date, effective_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
