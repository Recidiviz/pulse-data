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
"""Tests the sentence_imposed_group_summary view in sentence_sessions."""
import datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.cleaned_offense_description_to_labels import (
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.common.constants.state.state_charge import StateChargeV2Status
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.normalized_entities_utils import (
    queryable_address_for_normalized_entity,
)
from recidiviz.persistence.entity.state import (
    normalized_entities as normalized_state_module,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStatePerson,
    NormalizedStateSentence,
    NormalizedStateSentenceImposedGroup,
    NormalizedStateSentenceLength,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.infer_sentence_groups import (
    build_imposed_group_from_sentences,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.tests.big_query.load_entities_to_emulator_via_pipeline import (
    write_root_entities_to_emulator,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
    TableRow,
)
from recidiviz.utils.types import assert_subclass, assert_type

CLEANED_OFFENSE_ADDRESS = CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER.address
CLEANED_OFFENSE_DATA = [
    {
        "offense_description": "DESCRIPTION",
        "probability": 0.99,
        "uccs_code_uniform": None,
        "uccs_description_uniform": None,
        "uccs_category_uniform": None,
        "ncic_code_uniform": "NCIC UNIFORM",
        "ncic_description_uniform": None,
        "ncic_category_uniform": None,
        "nibrs_code_uniform": None,
        "nibrs_description_uniform": None,
        "nibrs_category_uniform": None,
        "crime_against_uniform": None,
        "is_drug_uniform": None,
        "is_violent_uniform": None,
        "offense_completed_uniform": None,
        "offense_attempted_uniform": None,
        "offense_conspired_uniform": None,
    },
    {
        "offense_description": "IS DRUG DESCRIPTION",
        "probability": 0.99,
        "uccs_code_uniform": None,
        "uccs_description_uniform": None,
        "uccs_category_uniform": None,
        "ncic_code_uniform": "NCIC UNIFORM",
        "ncic_description_uniform": None,
        "ncic_category_uniform": None,
        "nibrs_code_uniform": None,
        "nibrs_description_uniform": None,
        "nibrs_category_uniform": None,
        "crime_against_uniform": None,
        "is_drug_uniform": True,
        "is_violent_uniform": None,
        "offense_completed_uniform": None,
        "offense_attempted_uniform": None,
        "offense_conspired_uniform": None,
    },
    {
        "offense_description": "IS VIOLENT DRUG DESCRIPTION",
        "probability": 0.99,
        "uccs_code_uniform": None,
        "uccs_description_uniform": None,
        "uccs_category_uniform": None,
        "ncic_code_uniform": "NCIC UNIFORM",
        "ncic_description_uniform": None,
        "ncic_category_uniform": None,
        "nibrs_code_uniform": None,
        "nibrs_description_uniform": None,
        "nibrs_category_uniform": None,
        "crime_against_uniform": None,
        "is_drug_uniform": True,
        "is_violent_uniform": True,
        "offense_completed_uniform": None,
        "offense_attempted_uniform": None,
        "offense_conspired_uniform": None,
    },
]


class InferredProjectedDatesTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER."""

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    imposed_group_id = 42

    critical_date_1 = datetime.datetime(2022, 1, 1, 6)
    critical_date_2 = datetime.datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime.datetime(2022, 3, 4)

    big_projected_date_min = datetime.date(2025, 1, 1)
    small_projected_date_min = datetime.date(2024, 8, 1)

    big_projected_date_1_max = big_projected_date_min + datetime.timedelta(days=30)
    small_projected_date_3_max = small_projected_date_min + datetime.timedelta(days=30)

    delegate = StateSpecificSentenceNormalizationDelegate()

    # Show full diffs on test failure
    maxDiff = None

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        schemas = {
            queryable_address_for_normalized_entity(
                entity
            ): get_bq_schema_for_entity_table(
                normalized_state_module, assert_subclass(entity, Entity).get_table_id()
            )
            for entity in [
                NormalizedStateSentence,
                NormalizedStateSentenceLength,
                NormalizedStateSentenceImposedGroup,
                NormalizedStateChargeV2,
            ]
        }
        schemas[
            BigQueryAddress.from_str(
                "normalized_state.state_charge_v2_state_sentence_association"
            )
        ] = [
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("sentence_id", "INTEGER"),
            bigquery.SchemaField("charge_v2_id", "INTEGER"),
        ]
        schemas[CLEANED_OFFENSE_ADDRESS] = [
            bigquery.SchemaField("offense_description", "STRING"),
            bigquery.SchemaField("probability", "FLOAT"),
            bigquery.SchemaField("uccs_code_uniform", "INTEGER"),
            bigquery.SchemaField("uccs_description_uniform", "STRING"),
            bigquery.SchemaField("uccs_category_uniform", "STRING"),
            bigquery.SchemaField("ncic_code_uniform", "STRING"),
            bigquery.SchemaField("ncic_description_uniform", "STRING"),
            bigquery.SchemaField("ncic_category_uniform", "STRING"),
            bigquery.SchemaField("nibrs_code_uniform", "STRING"),
            bigquery.SchemaField("nibrs_description_uniform", "STRING"),
            bigquery.SchemaField("nibrs_category_uniform", "STRING"),
            bigquery.SchemaField("crime_against_uniform", "STRING"),
            bigquery.SchemaField("is_drug_uniform", "BOOLEAN"),
            bigquery.SchemaField("is_violent_uniform", "BOOLEAN"),
            bigquery.SchemaField("offense_completed_uniform", "BOOLEAN"),
            bigquery.SchemaField("offense_attempted_uniform", "BOOLEAN"),
            bigquery.SchemaField("offense_conspired_uniform", "BOOLEAN"),
        ]
        return schemas

    def _make_sentence(
        self,
        id_: int,
        charges: list[NormalizedStateChargeV2] | None = None,
        lengths: list[NormalizedStateSentenceLength] | None = None,
        statuses: list[NormalizedStateSentenceStatusSnapshot] | None = None,
    ) -> NormalizedStateSentence:
        if not charges:
            charges = []
        if not lengths:
            lengths = []
        if not statuses:
            statuses = []
        return NormalizedStateSentence(
            state_code=self.state_code.value,
            imposed_date=self.critical_date_1.date(),
            external_id=f"SENTENCE-{id_}",
            sentence_id=id_,
            sentence_group_external_id=None,
            sentence_imposed_group_id=None,
            sentence_inferred_group_id=None,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            charges=charges,
            sentence_lengths=lengths,
            sentence_status_snapshots=statuses,
        )

    def build_person_with_sentences(
        self, sentences: list[NormalizedStateSentence]
    ) -> NormalizedStatePerson:
        """Pass in sentencing entities for a person and receive a hydrated person with an imposed group."""
        sentences = [
            assert_type(set_backedges(s), NormalizedStateSentence) for s in sentences
        ]
        imposed_group = build_imposed_group_from_sentences(
            self.state_code, self.delegate, sentences
        )
        for sentence in sentences:
            sentence.sentence_imposed_group_id = imposed_group.sentence_imposed_group_id
        person = NormalizedStatePerson(
            state_code=self.state_code.value,
            person_id=self.person_id,
            sentences=sentences,
            sentence_imposed_groups=[imposed_group],
        )
        return assert_type(set_backedges(person), NormalizedStatePerson)

    def run_test(
        self, people: list[NormalizedStatePerson], expected_result: list[TableRow]
    ) -> None:
        self.maxDiff = None
        schemas = self.parent_schemas.copy()
        cleaned_offense_schema = schemas.pop(CLEANED_OFFENSE_ADDRESS)
        # Writes normalized_state tables, including association tables
        write_root_entities_to_emulator(
            emulator_tc=self, schema_mapping=schemas, people=people
        )
        self.create_mock_table(CLEANED_OFFENSE_ADDRESS, cleaned_offense_schema)
        self.load_rows_into_table(CLEANED_OFFENSE_ADDRESS, CLEANED_OFFENSE_DATA)
        view = self.view_builder.build()
        self.run_query_test(view.view_query, expected_result)

    def test_one_sentence_one_length(self) -> None:
        """
        The most simple case, a sentence is actively serving and has a single set of
        projected dates from imposition.
        """
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                )
            ],
            lengths=[
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.big_projected_date_min,
                    projected_completion_date_max_external=None,
                )
            ],
            statuses=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.state_code.value,
                    sentence_status_snapshot_id=114,
                    status_update_datetime=self.critical_date_1,
                    status_end_datetime=None,
                    status=StateSentenceStatus.SERVING,
                    sequence_num=1,
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": False,
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_life": False,
                "projected_completion_date_min": self.big_projected_date_min,
                "projected_completion_date_max": None,
                "sentence_length_days_min": 1096,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": None,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": None,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
            },
        ]
        self.run_test([person], expected_data)

    def test_no_sentence_lengths(self) -> None:
        """Tests that the view doesn't break when there are no StateSentenceLength entities."""
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": False,
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_life": False,
                "projected_completion_date_min": None,
                "projected_completion_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": None,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": None,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
            },
        ]
        self.run_test([person], expected_data)

    def test_sentence_lengths_same_update_datetime(self) -> None:
        """Tests that the projected date calculations work as expected when StateSentenceLength entities are updated together."""
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
            lengths=[
                # This length has the group's projected_completion_date_min
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.big_projected_date_min,
                    projected_completion_date_max_external=None,
                )
            ],
        )
        sentence_2 = self._make_sentence(
            2,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE-2",
                    charge_v2_id=hash("CHARGE-2"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
            lengths=[
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=211,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
                # This length won't be considered
                NormalizedStateSentenceLength(
                    sequence_num=2,
                    state_code=self.state_code.value,
                    sentence_length_id=212,
                    length_update_datetime=self.critical_date_2,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1, sentence_2])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": False,
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_life": False,
                "projected_completion_date_min": self.big_projected_date_min,
                "projected_completion_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": None,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": None,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
            },
        ]
        self.run_test([person], expected_data)

    def test_sentence_lengths_different_update_datetimes(self) -> None:
        """Tests that the projected date calculations work as expected when StateSentenceLength entities are updated at different times."""
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
            lengths=[
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
                # This length is not considered.
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_2,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
            ],
        )
        sentence_2 = self._make_sentence(
            2,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE-2",
                    charge_v2_id=hash("CHARGE-2"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
            lengths=[
                # This length has the group's projected_completion_date_min
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=211,
                    length_update_datetime=self.critical_date_1
                    + datetime.timedelta(days=3),
                    projected_completion_date_min_external=self.big_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
                # This length won't be considered
                NormalizedStateSentenceLength(
                    sequence_num=2,
                    state_code=self.state_code.value,
                    sentence_length_id=212,
                    length_update_datetime=self.critical_date_3,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1, sentence_2])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": False,
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_life": False,
                "projected_completion_date_min": self.big_projected_date_min,
                "projected_completion_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": None,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": None,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
            },
        ]
        self.run_test([person], expected_data)

    def test_any_is_drug(self) -> None:
        """Tests that fields named "any_is_drug" aggregate correctly"""
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="IS DRUG DESCRIPTION",
                ),
            ],
            lengths=[
                # This length has the group's projected_completion_date_min
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.big_projected_date_min,
                    projected_completion_date_max_external=None,
                )
            ],
        )
        sentence_2 = self._make_sentence(
            2,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE-2",
                    charge_v2_id=hash("CHARGE-2"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                ),
            ],
            lengths=[
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=211,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
                # This length won't be considered
                NormalizedStateSentenceLength(
                    sequence_num=2,
                    state_code=self.state_code.value,
                    sentence_length_id=212,
                    length_update_datetime=self.critical_date_2,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1, sentence_2])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": False,
                "any_is_violent": False,
                "any_is_drug": True,
                "any_is_life": False,
                "projected_completion_date_min": self.big_projected_date_min,
                "projected_completion_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "IS DRUG DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": True,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": None,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
            },
        ]
        self.run_test([person], expected_data)

    def test_all_any_is_xxx_fields(self) -> None:
        """Tests that all fields named "any_is_xxx" aggregate correctly"""
        sentence_1 = self._make_sentence(
            1,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE",
                    charge_v2_id=hash("CHARGE"),
                    status=StateChargeV2Status.CONVICTED,
                    description="IS VIOLENT DRUG DESCRIPTION",
                ),
            ],
            lengths=[
                # This length has the group's projected_completion_date_min
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=111,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.big_projected_date_min,
                    projected_completion_date_max_external=None,
                )
            ],
        )
        sentence_1.is_life = True
        sentence_2 = self._make_sentence(
            2,
            charges=[
                NormalizedStateChargeV2(
                    state_code=self.state_code.value,
                    external_id="CHARGE-2",
                    charge_v2_id=hash("CHARGE-2"),
                    status=StateChargeV2Status.CONVICTED,
                    description="DESCRIPTION",
                    is_sex_offense=True,
                ),
            ],
            lengths=[
                NormalizedStateSentenceLength(
                    sequence_num=1,
                    state_code=self.state_code.value,
                    sentence_length_id=211,
                    length_update_datetime=self.critical_date_1,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
                # This length won't be considered
                NormalizedStateSentenceLength(
                    sequence_num=2,
                    state_code=self.state_code.value,
                    sentence_length_id=212,
                    length_update_datetime=self.critical_date_2,
                    projected_completion_date_min_external=self.small_projected_date_min,
                    projected_completion_date_max_external=None,
                ),
            ],
        )
        person = self.build_person_with_sentences([sentence_1, sentence_2])
        imposed_group = person.sentence_imposed_groups[0]
        expected_data = [
            {
                "sentence_imposed_group_id": imposed_group.sentence_imposed_group_id,
                "most_severe_charge_v2_id": imposed_group.most_severe_charge_v2_id,
                "sentencing_authority": "STATE",
                "state_code": "US_XX",
                "imposed_date": imposed_group.imposed_date,
                "serving_start_date": imposed_group.serving_start_date,
                "any_is_sex_offense": True,
                "any_is_violent": True,
                "any_is_drug": True,
                "any_is_life": True,
                "projected_completion_date_min": self.big_projected_date_min,
                "projected_completion_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "most_severe_charge_classification_subtype": None,
                "most_severe_charge_classification_type": None,
                "most_severe_charge_description": "IS VIOLENT DRUG DESCRIPTION",
                "most_severe_charge_external_id": "CHARGE",
                "most_severe_charge_is_drug": None,
                "most_severe_charge_is_drug_uniform": True,
                "most_severe_charge_is_sex_offense": None,
                "most_severe_charge_is_violent": None,
                "most_severe_charge_is_violent_uniform": True,
                "most_severe_charge_ncic_category_external": None,
                "most_severe_charge_ncic_category_uniform": None,
                "most_severe_charge_ncic_code_uniform": "NCIC UNIFORM",
            },
        ]
        self.run_test([person], expected_data)
