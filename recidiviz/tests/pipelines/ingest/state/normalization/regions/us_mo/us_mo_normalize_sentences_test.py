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
Tests normalization of sentencing V2 entities in US_MO.

Data is taken from the ingest tests.
"""
import datetime

from recidiviz.common.constants.state.state_charge import (
    StateChargeV2ClassificationType,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.generate_primary_key import generate_primary_key
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_keys_for_root_entity_tree,
    string_representation,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_sentences import (
    get_normalized_sentences,
)
from recidiviz.utils.types import assert_type


def test_person_001_sentencing_normalization() -> None:
    person_001 = entities.StatePerson(
        state_code="US_MO",
        external_ids=[
            entities.StatePersonExternalId(
                external_id="TEST_001",
                id_type="US_MO_DOC",
                state_code="US_MO",
            )
        ],
    )

    # SETUP PRE-NORMALIZED SENTENCE DATA
    CHARGE_001_19900117_1 = entities.StateChargeV2(
        external_id="TEST_001-19900117-1",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=None,
        date_charged=None,
        county_code=None,
        ncic_code="2399",
        statute=None,
        description='"STLG U/150 "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.MISDEMEANOR,
        classification_type_raw_text="M",
        classification_subtype=None,
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
    )
    SENTENCE_001_19900117_1 = entities.StateSentence(
        external_id="TEST_001-19900117-1",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(1989, 9, 25),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-19900117",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='15I1000@@"NEW COURT PROBATION "',
        sentencing_authority_raw_text='STLO@@"NEW COURT PROBATION "',
        is_capital_punishment=False,
        charges=[CHARGE_001_19900117_1],
    )
    CHARGE_001_19900117_1.sentences.append(SENTENCE_001_19900117_1)

    CHARGE_001_20040224_1 = entities.StateChargeV2(
        external_id="TEST_001-20040224-1",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=datetime.date(2002, 6, 1),
        date_charged=None,
        county_code="US_MO_ST_LOUIS_COUNTY",
        ncic_code="3801",
        statute="26031010",
        description='"CRIMINAL NON-SUPPORT "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="D",
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
    )
    SENTENCE_001_20040224_1 = entities.StateSentence(
        external_id="TEST_001-20040224-1",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 2, 24),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='15I1000@@"NEW COURT PROBATION "',
        sentencing_authority_raw_text='STLO@@"NEW COURT PROBATION "',
        is_capital_punishment=False,
        charges=[CHARGE_001_20040224_1],
    )
    CHARGE_001_20040224_1.sentences.append(SENTENCE_001_20040224_1)

    CHARGE_001_20040224_2 = entities.StateChargeV2(
        external_id="TEST_001-20040224-2",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=datetime.date(2003, 11, 20),
        date_charged=None,
        county_code="US_MO_ST_LOUIS_COUNTY",
        ncic_code="3599",
        statute="32450990",
        description='"POSSESSION OF CONTROLLED SUBSTANCE "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="C",
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
    )
    SENTENCE_001_20040224_2 = entities.StateSentence(
        external_id="TEST_001-20040224-2",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 11, 5),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
        sentencing_authority_raw_text='STLO@@"COURT PROBATION-ADDL CHARGE "',
        is_capital_punishment=False,
        charges=[CHARGE_001_20040224_2],
    )
    CHARGE_001_20040224_2.sentences.append(SENTENCE_001_20040224_2)

    SENTENCE_001_19900117_1.sentence_lengths = [
        entities.StateSentenceLength(
            state_code="US_MO",
            length_update_datetime=datetime.datetime(2004, 3, 17),
            projected_completion_date_max_external=datetime.date(1991, 9, 24),
            projected_completion_date_min_external=None,
            person=person_001,
            sentence=SENTENCE_001_19900117_1,
        )
    ]
    SENTENCE_001_20040224_1.sentence_lengths = [
        entities.StateSentenceLength(
            state_code="US_MO",
            length_update_datetime=datetime.datetime(2009, 3, 9),
            projected_completion_date_max_external=datetime.date(2009, 2, 23),
            projected_completion_date_min_external=None,
            person=person_001,
            sentence=SENTENCE_001_20040224_1,
        ),
    ]
    SENTENCE_001_20040224_2.sentence_lengths = [
        entities.StateSentenceLength(
            state_code="US_MO",
            length_update_datetime=datetime.datetime(2009, 11, 4),
            projected_completion_date_max_external=datetime.date(2009, 11, 4),
            projected_completion_date_min_external=None,
            person=person_001,
            sentence=SENTENCE_001_20040224_2,
        ),
    ]

    person_001.sentences.extend(
        [
            SENTENCE_001_19900117_1,
            SENTENCE_001_20040224_1,
            SENTENCE_001_20040224_2,
        ]
    )
    set_backedges(person_001)
    person_001_pk = generate_primary_key(
        string_representation(
            {
                (external_id.external_id, external_id.id_type)
                for external_id in person_001.external_ids
            }
        ),
        state_code=StateCode.US_MO,
    )
    generate_primary_keys_for_root_entity_tree(
        root_primary_key=person_001_pk,
        root_entity=person_001,
        state_code=StateCode.US_MO,
    )

    # SETUP EXPECTED NORMALIZED SENTENCE DATA
    NORMALIZED_CHARGE_001_19900117_1 = normalized_entities.NormalizedStateChargeV2(
        charge_v2_id=assert_type(CHARGE_001_19900117_1.charge_v2_id, int),
        external_id="TEST_001-19900117-1",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=None,
        date_charged=None,
        county_code=None,
        ncic_code="2399",
        statute=None,
        description='"STLG U/150 "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.MISDEMEANOR,
        classification_type_raw_text="M",
        classification_subtype=None,
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
        ncic_code_external="2399",
        ncic_category_external="LARCENY",
        description_external='"STLG U/150 "',
        is_violent_external=False,
        is_drug_external=None,
        is_sex_offense_external=None,
    )
    NORMALIZED_SENTENCE_001_19900117_1 = normalized_entities.NormalizedStateSentence(
        sentence_id=assert_type(SENTENCE_001_19900117_1.sentence_id, int),
        external_id="TEST_001-19900117-1",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(1989, 9, 25),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-19900117",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='15I1000@@"NEW COURT PROBATION "',
        sentencing_authority_raw_text='STLO@@"NEW COURT PROBATION "',
        is_capital_punishment=False,
        charges=[NORMALIZED_CHARGE_001_19900117_1],
        sentence_lengths=[
            normalized_entities.NormalizedStateSentenceLength(
                sequence_num=1,
                state_code="US_MO",
                sentence_length_id=assert_type(
                    SENTENCE_001_19900117_1.sentence_lengths[0].sentence_length_id, int
                ),
                length_update_datetime=datetime.datetime(2004, 3, 17),
                projected_completion_date_max_external=datetime.date(1991, 9, 24),
                projected_completion_date_min_external=None,
            )
        ],
    )
    NORMALIZED_CHARGE_001_19900117_1.sentences.append(
        NORMALIZED_SENTENCE_001_19900117_1
    )

    NORMALIZED_CHARGE_001_20040224_1 = normalized_entities.NormalizedStateChargeV2(
        charge_v2_id=assert_type(CHARGE_001_20040224_1.charge_v2_id, int),
        external_id="TEST_001-20040224-1",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=datetime.date(2002, 6, 1),
        date_charged=None,
        county_code="US_MO_ST_LOUIS_COUNTY",
        ncic_code="3801",
        statute="26031010",
        description='"CRIMINAL NON-SUPPORT "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="D",
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
        ncic_code_external="3801",
        ncic_category_external="NEGLECT FAMILY",
        description_external='"CRIMINAL NON-SUPPORT "',
        is_violent_external=False,
        is_drug_external=None,
        is_sex_offense_external=None,
    )
    NORMALIZED_SENTENCE_001_20040224_1 = normalized_entities.NormalizedStateSentence(
        sentence_id=assert_type(SENTENCE_001_20040224_1.sentence_id, int),
        external_id="TEST_001-20040224-1",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 2, 24),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='15I1000@@"NEW COURT PROBATION "',
        sentencing_authority_raw_text='STLO@@"NEW COURT PROBATION "',
        is_capital_punishment=False,
        charges=[NORMALIZED_CHARGE_001_20040224_1],
        sentence_lengths=[
            normalized_entities.NormalizedStateSentenceLength(
                sequence_num=1,
                state_code="US_MO",
                sentence_length_id=assert_type(
                    SENTENCE_001_20040224_1.sentence_lengths[0].sentence_length_id, int
                ),
                length_update_datetime=datetime.datetime(2009, 3, 9),
                projected_completion_date_max_external=datetime.date(2009, 2, 23),
                projected_completion_date_min_external=None,
            )
        ],
    )
    NORMALIZED_CHARGE_001_20040224_1.sentences.append(
        NORMALIZED_SENTENCE_001_20040224_1
    )

    NORMALIZED_CHARGE_001_20040224_2 = normalized_entities.NormalizedStateChargeV2(
        charge_v2_id=assert_type(CHARGE_001_20040224_2.charge_v2_id, int),
        external_id="TEST_001-20040224-2",
        state_code="US_MO",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        status_raw_text=None,
        offense_date=datetime.date(2003, 11, 20),
        date_charged=None,
        county_code="US_MO_ST_LOUIS_COUNTY",
        ncic_code="3599",
        statute="32450990",
        description='"POSSESSION OF CONTROLLED SUBSTANCE "',
        attempted=None,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="C",
        offense_type=None,
        is_violent=False,
        is_sex_offense=None,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="21",
        ncic_code_external="3599",
        ncic_category_external="DANGEROUS DRUGS",
        description_external='"POSSESSION OF CONTROLLED SUBSTANCE "',
        is_violent_external=False,
        is_drug_external=None,
        is_sex_offense_external=None,
    )
    NORMALIZED_SENTENCE_001_20040224_2 = normalized_entities.NormalizedStateSentence(
        sentence_id=assert_type(SENTENCE_001_20040224_2.sentence_id, int),
        external_id="TEST_001-20040224-2",
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 11, 5),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        is_life=False,
        state_code="US_MO",
        county_code="US_MO_ST_LOUIS_COUNTY",
        sentence_type_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
        sentencing_authority_raw_text='STLO@@"COURT PROBATION-ADDL CHARGE "',
        is_capital_punishment=False,
        charges=[NORMALIZED_CHARGE_001_20040224_2],
        sentence_lengths=[
            normalized_entities.NormalizedStateSentenceLength(
                sequence_num=1,
                state_code="US_MO",
                sentence_length_id=assert_type(
                    SENTENCE_001_20040224_2.sentence_lengths[0].sentence_length_id, int
                ),
                length_update_datetime=datetime.datetime(2009, 11, 4),
                projected_completion_date_max_external=datetime.date(2009, 11, 4),
                projected_completion_date_min_external=None,
            ),
        ],
    )
    NORMALIZED_CHARGE_001_20040224_2.sentences.append(
        NORMALIZED_SENTENCE_001_20040224_2
    )
    expected_normalized_sentences = [
        set_backedges(NORMALIZED_SENTENCE_001_19900117_1),
        set_backedges(NORMALIZED_SENTENCE_001_20040224_1),
        set_backedges(NORMALIZED_SENTENCE_001_20040224_2),
    ]
    actual_normalized_sentences = sorted(
        get_normalized_sentences(person_001.sentences), key=lambda s: s.external_id
    )
    assert actual_normalized_sentences == expected_normalized_sentences