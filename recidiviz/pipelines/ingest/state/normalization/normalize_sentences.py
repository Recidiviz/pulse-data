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
"""
Houses logic for normalizing sentencing V2 entities:
  - StateSentence
  - StateSentenceLength
  - StateSentenceGroup
  - StateSentenceGroupLength
  - StateSentenceStatusSnapshot
  - StateSentenceServingPeriod
  - StateChargeV2

It also produces ingerred sentence groups that only exist in normalized_state:
  - StateSentenceGroupInferred
  - StateSentenceGroupInferredLength
"""
from typing import Dict, List

from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.common.ncic import get_description
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.state.entities import StateChargeV2, StateSentence
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
)
from recidiviz.utils.types import assert_type


def normalize_charge_v2(charge: StateChargeV2) -> NormalizedStateChargeV2:
    """
    Default normalization for StateChargeV2.
    No fields are changed, but we do add "external" fields for
    use in downstream views.

    Note this does not produce references to the sentences or
    person associated with this charge.
    """
    return NormalizedStateChargeV2(
        external_id=charge.external_id,
        state_code=charge.state_code,
        charge_v2_id=assert_type(charge.charge_v2_id, int),
        status=charge.status,
        status_raw_text=charge.status_raw_text,
        offense_date=charge.offense_date,
        date_charged=charge.date_charged,
        county_code=charge.county_code,
        ncic_code=charge.ncic_code,
        statute=charge.statute,
        description=charge.description,
        attempted=charge.attempted,
        classification_type=charge.classification_type,
        classification_type_raw_text=charge.classification_type_raw_text,
        classification_subtype=charge.classification_subtype,
        offense_type=charge.offense_type,
        is_violent=charge.is_violent,
        is_sex_offense=charge.is_sex_offense,
        is_drug=charge.is_drug,
        counts=charge.counts,
        charge_notes=charge.charge_notes,
        is_controlling=charge.is_controlling,
        charging_entity=charge.charging_entity,
        judge_full_name=charge.judge_full_name,
        judge_external_id=charge.judge_external_id,
        judicial_district_code=charge.judicial_district_code,
        # We duplicate these fields to external
        ncic_code_external=charge.ncic_code,
        ncic_category_external=(
            get_description(charge.ncic_code) if charge.ncic_code else None
        ),
        description_external=charge.description,
        is_violent_external=charge.is_violent,
        is_drug_external=charge.is_drug,
        is_sex_offense_external=charge.is_sex_offense,
    )


def normalize_sentence(
    sentence: StateSentence,
    charge_id_to_normalized_charge_cache: Dict[int, NormalizedStateChargeV2],
) -> NormalizedStateSentence:
    """Normalizes the sentencing V2 entities for a given StateSentence."""
    normalized_charges = []
    for charge in sentence.charges:
        charge_id = assert_type(charge.charge_v2_id, int)
        if charge_id not in charge_id_to_normalized_charge_cache:
            normalized_charge = normalize_charge_v2(charge)
            charge_id_to_normalized_charge_cache[charge_id] = normalized_charge
        normalized_charges.append(charge_id_to_normalized_charge_cache[charge_id])
    return NormalizedStateSentence(
        sentence_id=assert_type(sentence.sentence_id, int),
        external_id=sentence.external_id,
        state_code=sentence.state_code,
        sentence_type=assert_type(sentence.sentence_type, StateSentenceType),
        sentence_type_raw_text=sentence.sentence_type_raw_text,
        sentencing_authority=sentence.sentencing_authority,
        sentencing_authority_raw_text=sentence.sentencing_authority_raw_text,
        sentence_group_external_id=sentence.sentence_group_external_id,
        imposed_date=sentence.imposed_date,
        initial_time_served_days=sentence.initial_time_served_days,
        is_life=sentence.is_life,
        is_capital_punishment=sentence.is_capital_punishment,
        parole_possible=sentence.parole_possible,
        county_code=sentence.county_code,
        parent_sentence_external_id_array=sentence.parent_sentence_external_id_array,
        conditions=sentence.conditions,
        sentence_metadata=sentence.sentence_metadata,
        # Relationships
        charges=normalized_charges,
        # TODO(#32303): Normalize sentence_statuses
        # TODO(#32304): Normalize sentence_serving_periods
        # TODO(#32306): Create sentence_group_inferred from group and serving periods
        # TODO(#32307): Normalize sentence_lengths
    )


def get_normalized_sentences(
    sentences: List[StateSentence],
) -> List[NormalizedStateSentence]:
    """
    Normalizes the list of sentences for a given person.
    It also normalizes all sentencing v2 entities, and maintains
    the many-to-many relationship for sentences and charges.
    """
    charge_id_to_normalized_charge_cache: Dict[int, NormalizedStateChargeV2] = {}
    normalized_sentences = [
        normalize_sentence(sentence, charge_id_to_normalized_charge_cache)
        for sentence in sentences
    ]
    for sentence in normalized_sentences:
        set_backedges(sentence)
    return normalized_sentences
