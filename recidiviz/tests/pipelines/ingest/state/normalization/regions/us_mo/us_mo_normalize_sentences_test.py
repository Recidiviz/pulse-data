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
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    set_backedges,
)
from recidiviz.persistence.entity.generate_primary_key import generate_primary_key
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_keys_for_root_entity_tree,
    string_representation,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.infer_sentence_groups import (
    NormalizedStateSentenceInferredGroup,
    build_imposed_group_from_sentences,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_all_sentencing_entities import (
    get_normalized_sentencing_entities,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_normalization_delegate import (
    UsMoSentenceNormalizationDelegate,
)
from recidiviz.utils.types import assert_type

MO_DELEGATE = UsMoSentenceNormalizationDelegate()


def _set_backedges(entity: Entity) -> Entity | RootEntity:
    return set_backedges(entity, entities_module_context_for_entity(entity))


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
    SENTENCE_001_19900117_1.sentence_status_snapshots = [
        entities.StateSentenceStatusSnapshot(
            sequence_num=1,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(1990, 1, 17, 0, 0),
            status=StateSentenceStatus.SERVING,
            status_raw_text='15I1000@@"NEW COURT PROBATION "',
            sentence_status_snapshot_id=None,
            person=person_001,
            sentence=SENTENCE_001_19900117_1,
        ),
        entities.StateSentenceStatusSnapshot(
            sequence_num=2,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(1992, 2, 20, 0, 0),
            status=StateSentenceStatus.COMPLETED,
            status_raw_text='99O2100@@"PROB REV-TECHNICAL-JAIL "',
            person=person_001,
            sentence=SENTENCE_001_19900117_1,
        ),
    ]
    SENTENCE_001_20040224_1.sentence_status_snapshots = [
        entities.StateSentenceStatusSnapshot(
            sequence_num=1,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(2004, 2, 24, 0, 0),
            status=StateSentenceStatus.SERVING,
            status_raw_text='15I1000@@"NEW COURT PROBATION "',
            sentence_status_snapshot_id=None,
            person=person_001,
            sentence=SENTENCE_001_20040224_1,
        ),
        entities.StateSentenceStatusSnapshot(
            sequence_num=3,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(2009, 1, 26, 0, 0),
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
            sentence_status_snapshot_id=None,
            person=person_001,
            sentence=SENTENCE_001_20040224_1,
        ),
        entities.StateSentenceStatusSnapshot(
            sequence_num=4,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(2009, 2, 27, 0, 0),
            status=StateSentenceStatus.COMPLETED,
            status_raw_text='95O7000@@"RELIEVED OF SUPV-COURT "',
            sentence_status_snapshot_id=None,
            person=person_001,
            sentence=SENTENCE_001_20040224_1,
        ),
    ]
    SENTENCE_001_20040224_2.sentence_status_snapshots = [
        entities.StateSentenceStatusSnapshot(
            sequence_num=2,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(2004, 11, 5, 0, 0),
            status=StateSentenceStatus.SERVING,
            status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
            person=person_001,
            sentence=SENTENCE_001_20040224_2,
        ),
        entities.StateSentenceStatusSnapshot(
            sequence_num=5,
            state_code="US_MO",
            status_update_datetime=datetime.datetime(2009, 11, 4, 0, 0),
            status=StateSentenceStatus.COMPLETED,
            status_raw_text='99O1010@@"COURT PROB DISC-CONFIDENTIAL "',
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

    SG_001_19900117 = entities.StateSentenceGroup(
        state_code="US_MO",
        external_id="TEST_001-19900117",
    )
    SG_001_20040224 = entities.StateSentenceGroup(
        state_code="US_MO",
        external_id="TEST_001-20040224",
    )
    person_001.sentence_groups.extend(
        [
            SG_001_19900117,
            SG_001_20040224,
        ]
    )

    _set_backedges(person_001)
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
        current_start_date=datetime.date(1990, 1, 17),
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(1989, 9, 25),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-19900117",
        sentence_inferred_group_id=None,
        sentence_imposed_group_id=None,
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
        sentence_status_snapshots=[
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=1,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(1990, 1, 17, 0, 0),
                status_end_datetime=datetime.datetime(1992, 2, 20, 0, 0),
                status=StateSentenceStatus.SERVING,
                status_raw_text='15I1000@@"NEW COURT PROBATION "',
                sentence_status_snapshot_id=-1,
            ),
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=2,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(1992, 2, 20, 0, 0),
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='99O2100@@"PROB REV-TECHNICAL-JAIL "',
                sentence_status_snapshot_id=-1,
            ),
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
        current_start_date=datetime.date(2004, 2, 24),
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 2, 24),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        sentence_inferred_group_id=None,
        sentence_imposed_group_id=None,
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
        sentence_status_snapshots=[
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=1,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(2004, 2, 24, 0, 0),
                status_end_datetime=datetime.datetime(2009, 1, 26, 0, 0),
                status=StateSentenceStatus.SERVING,
                status_raw_text='15I1000@@"NEW COURT PROBATION "',
                sentence_status_snapshot_id=-1,
            ),
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=2,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(2009, 1, 26, 0, 0),
                status=StateSentenceStatus.SUSPENDED,
                status_end_datetime=datetime.datetime(2009, 2, 27, 0, 0),
                status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
                sentence_status_snapshot_id=-1,
            ),
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=3,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(2009, 2, 27, 0, 0),
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='95O7000@@"RELIEVED OF SUPV-COURT "',
                sentence_status_snapshot_id=-1,
            ),
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
        current_start_date=datetime.date(2004, 11, 5),
        sentence_type=StateSentenceType.PROBATION,
        imposed_date=datetime.date(2004, 11, 5),
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_group_external_id="TEST_001-20040224",
        sentence_inferred_group_id=None,
        sentence_imposed_group_id=None,
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
        sentence_status_snapshots=[
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=1,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(2004, 11, 5, 0, 0),
                status_end_datetime=datetime.datetime(2009, 11, 4, 0, 0),
                status=StateSentenceStatus.SERVING,
                status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
                sentence_status_snapshot_id=-1,
            ),
            normalized_entities.NormalizedStateSentenceStatusSnapshot(
                sequence_num=2,
                state_code="US_MO",
                status_update_datetime=datetime.datetime(2009, 11, 4, 0, 0),
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='99O1010@@"COURT PROB DISC-CONFIDENTIAL "',
                sentence_status_snapshot_id=-1,
            ),
        ],
    )
    NORMALIZED_CHARGE_001_20040224_2.sentences.append(
        NORMALIZED_SENTENCE_001_20040224_2
    )

    INFERRED_GROUP_FROM_19900117 = (
        NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
            StateCode.US_MO, [NORMALIZED_SENTENCE_001_19900117_1.external_id]
        )
    )
    IMPOSED_GROUP_FROM_19900117 = build_imposed_group_from_sentences(
        StateCode.US_MO,
        MO_DELEGATE,
        [
            assert_type(
                _set_backedges(NORMALIZED_SENTENCE_001_19900117_1),
                normalized_entities.NormalizedStateSentence,
            )
        ],
    )
    NORMALIZED_SENTENCE_001_19900117_1.sentence_inferred_group_id = (
        INFERRED_GROUP_FROM_19900117.sentence_inferred_group_id
    )
    NORMALIZED_SENTENCE_001_19900117_1.sentence_imposed_group_id = (
        IMPOSED_GROUP_FROM_19900117.sentence_imposed_group_id
    )
    INFERRED_GROUP_FROM_20040224 = (
        NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
            StateCode.US_MO,
            [
                NORMALIZED_SENTENCE_001_20040224_1.external_id,
                NORMALIZED_SENTENCE_001_20040224_2.external_id,
            ],
        )
    )
    IMPOSED_GROUP_FROM_20040224_1 = build_imposed_group_from_sentences(
        StateCode.US_MO,
        MO_DELEGATE,
        [
            assert_type(
                _set_backedges(NORMALIZED_SENTENCE_001_20040224_1),
                normalized_entities.NormalizedStateSentence,
            )
        ],
    )
    IMPOSED_GROUP_FROM_20040224_2 = build_imposed_group_from_sentences(
        StateCode.US_MO,
        MO_DELEGATE,
        [
            assert_type(
                _set_backedges(NORMALIZED_SENTENCE_001_20040224_2),
                normalized_entities.NormalizedStateSentence,
            )
        ],
    )
    NORMALIZED_SENTENCE_001_20040224_1.sentence_inferred_group_id = (
        INFERRED_GROUP_FROM_20040224.sentence_inferred_group_id
    )
    NORMALIZED_SENTENCE_001_20040224_2.sentence_inferred_group_id = (
        INFERRED_GROUP_FROM_20040224.sentence_inferred_group_id
    )
    NORMALIZED_SENTENCE_001_20040224_1.sentence_imposed_group_id = (
        IMPOSED_GROUP_FROM_20040224_1.sentence_imposed_group_id
    )
    NORMALIZED_SENTENCE_001_20040224_2.sentence_imposed_group_id = (
        IMPOSED_GROUP_FROM_20040224_2.sentence_imposed_group_id
    )

    expected_normalized_sentences = [
        _set_backedges(NORMALIZED_SENTENCE_001_19900117_1),
        _set_backedges(NORMALIZED_SENTENCE_001_20040224_1),
        _set_backedges(NORMALIZED_SENTENCE_001_20040224_2),
    ]

    NORMALIZED_SG_001_19900117 = normalized_entities.NormalizedStateSentenceGroup(
        state_code="US_MO",
        external_id="TEST_001-19900117",
        sentence_group_id=assert_type(SG_001_19900117.sentence_group_id, int),
        sentence_inferred_group_id=INFERRED_GROUP_FROM_19900117.sentence_inferred_group_id,
    )
    NORMALIZED_SG_001_20040224 = normalized_entities.NormalizedStateSentenceGroup(
        state_code="US_MO",
        external_id="TEST_001-20040224",
        sentence_group_id=assert_type(SG_001_20040224.sentence_group_id, int),
        sentence_inferred_group_id=INFERRED_GROUP_FROM_20040224.sentence_inferred_group_id,
    )
    expected_normalized_sentence_groups = [
        _set_backedges(NORMALIZED_SG_001_19900117),
        _set_backedges(NORMALIZED_SG_001_20040224),
    ]

    expected_inferred_groups = sorted(
        [
            _set_backedges(INFERRED_GROUP_FROM_19900117),
            _set_backedges(INFERRED_GROUP_FROM_20040224),
        ],
        key=lambda g: g.external_id,  # type: ignore
    )
    expected_imposed_groups = sorted(
        [
            _set_backedges(IMPOSED_GROUP_FROM_19900117),
            _set_backedges(IMPOSED_GROUP_FROM_20040224_1),
            _set_backedges(IMPOSED_GROUP_FROM_20040224_2),
        ],
        key=lambda g: g.external_id,  # type: ignore
    )

    (
        actual_normalized_sentences,
        actual_normalized_sentence_groups,
        actual_inferred_groups,
        actual_imposed_groups,
    ) = get_normalized_sentencing_entities(
        StateCode.US_MO,
        person_001,
        MO_DELEGATE,
        expected_output_entities=get_all_entity_classes_in_module(normalized_entities),
    )

    # Migrate the IDs from normalization to the expected entities.
    # This can only work if all other fields match.
    for actual, expected in zip(
        actual_normalized_sentences, expected_normalized_sentences
    ):
        for actual_snap, expected_snap in zip(
            assert_type(
                actual, normalized_entities.NormalizedStateSentence
            ).sentence_status_snapshots,
            assert_type(
                expected, normalized_entities.NormalizedStateSentence
            ).sentence_status_snapshots,
        ):
            expected_snap.sentence_status_snapshot_id = (
                actual_snap.sentence_status_snapshot_id
            )

    assert actual_normalized_sentences == expected_normalized_sentences
    assert actual_normalized_sentence_groups == expected_normalized_sentence_groups
    assert actual_inferred_groups == expected_inferred_groups
    assert actual_imposed_groups == expected_imposed_groups
