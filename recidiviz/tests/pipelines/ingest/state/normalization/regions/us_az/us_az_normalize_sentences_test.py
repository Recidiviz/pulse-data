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
Tests normalization of sentencing V2 entities in US_AZ.
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
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_all_sentencing_entities import (
    get_normalized_sentencing_entities,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_sentence_normalization_delegate import (
    UsAzSentenceNormalizationDelegate,
)


def test_sentencing_normalization() -> None:
    """
    Tests a person that has an inferred sentence group only through
    overlapping serving. The second sentence is the result of a violation
    during serving of the first.
    """

    # SETUP PRE-NORMALIZED SENTENCE DATA
    person = entities.StatePerson(
        state_code="US_AZ",
        external_ids=[
            entities.StatePersonExternalId(
                external_id="TEST_01",
                id_type="US_AZ_DOC",
                state_code="US_AZ",
            )
        ],
    )
    SG_1 = entities.StateSentenceGroup(
        state_code="US_AZ",
        external_id="SG-1",
    )
    SG_2 = entities.StateSentenceGroup(
        state_code="US_AZ",
        external_id="SG-2",
    )

    CHARGE_01 = entities.StateChargeV2(
        external_id="CHARGE_01",
        state_code="US_AZ",
        status=StateChargeV2Status.CONVICTED,
        status_raw_text=None,
        offense_date=None,
        date_charged=None,
        county_code=None,
        ncic_code=None,
        statute=None,
        description=None,
        attempted=None,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="FELONY",
        classification_subtype=None,
        offense_type=None,
        is_violent=False,
        is_sex_offense=True,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="CODE",
    )
    CHARGE_02 = entities.StateChargeV2(
        external_id="CHARGE_02",
        state_code="US_AZ",
        status=StateChargeV2Status.CONVICTED,
        status_raw_text=None,
        offense_date=None,
        date_charged=None,
        county_code=None,
        ncic_code=None,
        statute=None,
        description=None,
        attempted=None,
        classification_type=StateChargeV2ClassificationType.MISDEMEANOR,
        classification_type_raw_text="MISDEMEANOR",
        classification_subtype=None,
        offense_type=None,
        is_violent=False,
        is_sex_offense=True,
        is_drug=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        judge_full_name=None,
        judge_external_id=None,
        judicial_district_code="CODE",
    )

    SENTENCE_01 = entities.StateSentence(
        state_code="US_AZ",
        external_id="SENTENCE-1",
        sentence_group_external_id="SG-1",
        imposed_date=datetime.date(2012, 7, 16),
        sentencing_authority=StateSentencingAuthority.STATE,
        sentence_type=StateSentenceType.STATE_PRISON,
        charges=[CHARGE_01],
        sentence_status_snapshots=[
            entities.StateSentenceStatusSnapshot(
                state_code="US_AZ",
                status_update_datetime=datetime.datetime(2012, 3, 13),
                status=StateSentenceStatus.SERVING,
            ),
            entities.StateSentenceStatusSnapshot(
                state_code="US_AZ",
                status_update_datetime=datetime.datetime(2013, 9, 11),
                status=StateSentenceStatus.COMPLETED,
            ),
        ],
    )
    SENTENCE_02 = entities.StateSentence(
        state_code="US_AZ",
        external_id="SENTENCE-2",
        sentence_group_external_id="SG-2",
        imposed_date=datetime.date(2015, 6, 9),
        sentencing_authority=StateSentencingAuthority.STATE,
        sentence_type=StateSentenceType.STATE_PRISON,
        charges=[CHARGE_02],
        sentence_status_snapshots=[
            entities.StateSentenceStatusSnapshot(
                state_code="US_AZ",
                status_update_datetime=datetime.datetime(2013, 6, 19),
                status=StateSentenceStatus.SERVING,
            ),
            entities.StateSentenceStatusSnapshot(
                state_code="US_AZ",
                status_update_datetime=datetime.datetime(2019, 6, 19),
                status=StateSentenceStatus.COMPLETED,
            ),
        ],
    )

    person.sentences = [SENTENCE_01, SENTENCE_02]
    person.sentence_groups = [SG_1, SG_2]
    set_backedges(person, entities_module_context_for_entity(person))
    person_pk = generate_primary_key(
        string_representation(
            {
                (external_id.external_id, external_id.id_type)
                for external_id in person.external_ids
            }
        ),
        state_code=StateCode.US_AZ,
    )
    generate_primary_keys_for_root_entity_tree(
        root_primary_key=person_pk,
        root_entity=person,
        state_code=StateCode.US_AZ,
    )

    (
        actual_normalized_sentences,
        actual_normalized_sentence_groups,
        actual_inferred_groups,
        actual_imposed_groups,
    ) = get_normalized_sentencing_entities(
        StateCode.US_AZ,
        person,
        UsAzSentenceNormalizationDelegate(
            incarceration_periods=person.incarceration_periods,
            sentences=person.sentences,
        ),
        expected_output_entities=get_all_entity_classes_in_module(normalized_entities),
    )

    expected_inferred_group = normalized_entities.NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
        StateCode.US_AZ, ["SENTENCE-1", "SENTENCE-2"]
    )
    assert len(actual_imposed_groups) == 2
    assert actual_inferred_groups == [expected_inferred_group]
    for sentence in actual_normalized_sentences:
        assert (
            sentence.sentence_inferred_group_id
            == expected_inferred_group.sentence_inferred_group_id
        )
        assert sentence.sentence_imposed_group_id is not None
    for group in actual_normalized_sentence_groups:
        assert (
            group.sentence_inferred_group_id
            == expected_inferred_group.sentence_inferred_group_id
        )
