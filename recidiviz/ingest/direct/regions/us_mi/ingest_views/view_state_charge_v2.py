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
"""Query containing MDOC state charge information."""

# pylint: disable=anomalous-backslash-in-string
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
,
-- each offense can map to multiple offense types, so aggregate by offense_id
offense_types as (
  select offense_id, 
         STRING_AGG(distinct offense_type_id, ',' ORDER BY offense_type_id) as offense_type_ids,
         STRING_AGG(distinct ref.description, ', ' ORDER BY ref.description) as offense_type_description
  from {ADH_OFFENSE_CATEGORY_LINK} off_cat_link
    left join {ADH_OFFENSE_CATEGORY} off_cat on off_cat_link.offense_category_id = off_cat.offense_category_id
    left join {ADH_REFERENCE_CODE} ref on off_cat.offense_type_id = ref.reference_code_id
  group by offense_id
),

-- each charge can map to multiple sentences, so take sentence-level information (controlling, county, judge, and counts information) 
-- from earliest/initial sentence with this charge (as discussed with Justine)
earliest_sentence_information as (
  select *
  from (
    select 
      offender_charge_id,
      controlling_sent_code_id,
      -- if there are multiple initial sentences all from the same day, prioritize sentences where controlling_sent_code_is is not null (aka sentence was controlling in some way)  
      RANK() OVER (partition by offender_charge_id order by (DATE(sentenced_date)), controlling_sent_code_id NULLS LAST, offender_sentence_id) as sentence_sequence,
      ref.description as county,
      judge_id,
      counts
    from (select * from {ADH_OFFENDER_SENTENCE} where sentence_type_id in ('431', '430')) sent  -- only consider sentence information from incarceration/supervision sentences
      left join {ADH_LEGAL_ORDER} leg on sent.legal_order_id = leg.legal_order_id
      left join {ADH_REFERENCE_CODE} ref on leg.county_code_id = ref.reference_code_id
  ) alias1
  where sentence_sequence = 1
)

select 
    charge.offender_booking_id,
    charge.offender_charge_id,
    charge.closing_reason_code,
    (DATE(offense_date)) as offense_date,
    offense.ncic_code,
    offense.offense_code,
    offense.description,
    attempted_id,
    offense_types.offense_type_ids,
    offense_types.offense_type_description,
    earliest.counts,
    earliest.controlling_sent_code_id,
    earliest.county,
    earliest.judge_id,
    judge.first_name,
    judge.last_name,
    judge.middle_name,
    judge.name_suffix,
    ref5.description as circuit_description,
    xwalk.StatRpt_term,
    sent.offender_sentence_id,
    sent.sentence_type_id
from {ADH_OFFENDER_CHARGE} charge
    left join {ADH_OFFENSE} offense on charge.offense_id = offense.offense_id
    left join offense_types on charge.offense_id = offense_types.offense_id
    left join earliest_sentence_information earliest on charge.offender_charge_id = earliest.offender_charge_id
    left join {ADH_JUDGE} judge on earliest.judge_id = judge.judge_id
    left join {ADH_REFERENCE_CODE} ref5 on judge.circuit_id = ref5.reference_code_id
    -- inner join on offender_booking to make sure that this will be attached to a state_
    inner join {ADH_OFFENDER_BOOKING} book on charge.offender_booking_id = book.offender_booking_id
    left join {RECIDIVIZ_REFERENCE_mclxwalk} xwalk on offense.offense_id = xwalk.offense_id
    -- since charges hang of off sentences, we want to merge on all sentences associated with this charge
    -- inner join because sentence_id is a required external_id for the sentence object
    inner join {ADH_OFFENDER_SENTENCE} sent on charge.offender_charge_id = sent.offender_charge_id
where sent.sentence_type_id in ('431', '430') -- only keep incarceration and supervision sentences
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_charge_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
