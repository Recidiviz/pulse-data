# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests the state ingest pipeline."""
from datetime import date, datetime
from typing import List

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.pipelines.ingest.state.test_case import (
    INGEST_INTEGRATION,
    StateIngestPipelineTestCase,
)


class TestStateIngestPipeline(StateIngestPipelineTestCase):
    """Tests the state ingest pipeline all the way through using state code US_DD."""

    def setUp(self) -> None:
        super().setUp()

    def test_state_ingest_pipeline(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name=INGEST_INTEGRATION)
        expected_ingest_view_output = {
            ingest_view: self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view, test_name=INGEST_INTEGRATION
            )
            for ingest_view in self.ingest_view_manifest_collector().launchable_ingest_views(
                ingest_instance=self.ingest_instance(), is_dataflow_pipeline=True
            )
        }

        # Ingest12
        external_id_1 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID1",
            id_type="US_DD_ID_TYPE",
        )
        person_1 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
            external_ids=[external_id_1],
            full_name='{"given_names": "VALUE1", "middle_names": "", "name_suffix": "", "surname": "VALUE1"}',
        )
        external_id_1.person = person_1
        external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID2",
            id_type="US_DD_ID_TYPE",
        )
        person_2 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
            external_ids=[external_id_2],
            full_name='{"given_names": "VALUE2", "middle_names": "", "name_suffix": "", "surname": "VALUE2"}',
        )
        external_id_2.person = person_2

        # IngestMultipleChildren
        incarceration_period_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=self.region_code().value,
            external_id="IC1",
            admission_date=date(2018, 1, 1),
            release_date=date(2019, 1, 1),
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_1,
        )
        incarceration_period_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=self.region_code().value,
            external_id="IC2",
            admission_date=date(2020, 1, 1),
            release_date=date(2021, 1, 1),
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_1,
        )
        incarceration_period_3 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=self.region_code().value,
            external_id="IC3",
            admission_date=date(2022, 1, 1),
            release_date=date(2023, 1, 1),
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_1,
        )
        incarceration_period_4 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=self.region_code().value,
            external_id="IC4",
            admission_date=date(2021, 6, 1),
            release_date=date(2021, 12, 1),
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_1,
        )
        person_1.incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
            incarceration_period_4,
        ]

        # IngestMultipleRootExternalIds
        external_id_1_1 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="VALUE3",
            id_type="US_DD_ID_ANOTHER_TYPE",
            person=person_1,
        )
        person_1.external_ids.append(external_id_1_1)
        person_3 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
        )
        external_id_3_1 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID3",
            id_type="US_DD_ID_TYPE",
            person=person_3,
        )
        external_id_3_2 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="VALUE4",
            id_type="US_DD_ID_ANOTHER_TYPE",
            person=person_3,
        )
        person_3.external_ids = [external_id_3_1, external_id_3_2]

        # IngestTaskDeadline
        person_4 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
        )
        external_id_4 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="1",
            id_type="US_DD_ID_TYPE",
            person=person_4,
        )
        person_4.external_ids = [external_id_4]

        task_deadline_1 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 1),
            update_datetime=datetime(2023, 7, 1, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_2 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 2),
            update_datetime=datetime(2023, 7, 2, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_3 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 3),
            update_datetime=datetime(2023, 7, 3, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_4 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 4),
            update_datetime=datetime(2023, 7, 4, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        person_4.task_deadlines = [
            task_deadline_1,
            task_deadline_2,
            task_deadline_3,
            task_deadline_4,
        ]

        # IngestMultipleParents
        external_id_5 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID7",
            id_type="US_DD_ID_TYPE",
        )
        person_5 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
            external_ids=[external_id_5],
        )
        external_id_5.person = person_5
        charge_1 = entities.StateCharge.new_with_defaults(
            state_code=self.region_code().value,
            external_id="C1",
            status=StateChargeStatus.INTERNAL_UNKNOWN,
            person=person_5,
        )
        charge_2 = entities.StateCharge.new_with_defaults(
            state_code=self.region_code().value,
            external_id="C2",
            status=StateChargeStatus.INTERNAL_UNKNOWN,
            person=person_5,
        )
        charge_3 = entities.StateCharge.new_with_defaults(
            state_code=self.region_code().value,
            external_id="C3",
            status=StateChargeStatus.INTERNAL_UNKNOWN,
            person=person_5,
        )
        supervision_sentence_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=self.region_code().value,
            external_id="S1",
            status=StateSentenceStatus.INTERNAL_UNKNOWN,
            person=person_5,
            charges=[charge_1],
        )
        supervision_sentence_2 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=self.region_code().value,
            external_id="S2",
            status=StateSentenceStatus.INTERNAL_UNKNOWN,
            person=person_5,
            charges=[charge_1],
        )
        supervision_sentence_3 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=self.region_code().value,
            external_id="S3",
            status=StateSentenceStatus.INTERNAL_UNKNOWN,
            person=person_5,
            charges=[charge_3],
        )
        incarceration_sentence_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=self.region_code().value,
                external_id="I1",
                status=StateSentenceStatus.INTERNAL_UNKNOWN,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                person=person_5,
                charges=[charge_1],
            )
        )
        incarceration_sentence_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=self.region_code().value,
                external_id="I2",
                status=StateSentenceStatus.INTERNAL_UNKNOWN,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                person=person_5,
                charges=[charge_1],
            )
        )
        incarceration_sentence_3 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=self.region_code().value,
                external_id="I3",
                status=StateSentenceStatus.INTERNAL_UNKNOWN,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                person=person_5,
                charges=[charge_2],
            )
        )
        charge_1.incarceration_sentences = [
            incarceration_sentence_1,
            incarceration_sentence_2,
        ]
        charge_1.supervision_sentences = [
            supervision_sentence_1,
            supervision_sentence_2,
        ]
        charge_2.incarceration_sentences = [incarceration_sentence_3]
        charge_3.supervision_sentences = [supervision_sentence_3]
        person_5.incarceration_sentences = [
            incarceration_sentence_1,
            incarceration_sentence_2,
            incarceration_sentence_3,
        ]
        person_5.supervision_sentences = [
            supervision_sentence_1,
            supervision_sentence_2,
            supervision_sentence_3,
        ]

        expected_root_entities: List[RootEntity] = [
            person_1,
            person_2,
            person_3,
            person_4,
            person_5,
        ]

        self.run_test_state_pipeline(
            expected_ingest_view_output, expected_root_entities
        )

    def test_state_ingest_pipeline_ingest_view_results_only(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name=INGEST_INTEGRATION)
        expected_ingest_view_output = {
            ingest_view: self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view, test_name=INGEST_INTEGRATION
            )
            for ingest_view in self.ingest_view_manifest_collector().launchable_ingest_views(
                ingest_instance=self.ingest_instance(), is_dataflow_pipeline=True
            )
        }
        self.run_test_state_pipeline(
            expected_ingest_view_output, [], ingest_view_results_only=True
        )

    def test_state_ingest_pipeline_ingest_views_to_run_subset(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name=INGEST_INTEGRATION)
        subset_of_ingest_views = ["ingest12", "ingestTaskDeadline"]
        expected_ingest_view_output = {
            ingest_view: self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view, test_name=INGEST_INTEGRATION
            )
            if ingest_view in subset_of_ingest_views
            else []
            for ingest_view in self.ingest_view_manifest_collector().launchable_ingest_views(
                ingest_instance=self.ingest_instance(), is_dataflow_pipeline=True
            )
        }

        # Ingest12
        external_id_1 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID1",
            id_type="US_DD_ID_TYPE",
        )
        person_1 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
            external_ids=[external_id_1],
            full_name='{"given_names": "VALUE1", "middle_names": "", "name_suffix": "", "surname": "VALUE1"}',
        )
        external_id_1.person = person_1
        external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="ID2",
            id_type="US_DD_ID_TYPE",
        )
        person_2 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
            external_ids=[external_id_2],
            full_name='{"given_names": "VALUE2", "middle_names": "", "name_suffix": "", "surname": "VALUE2"}',
        )
        external_id_2.person = person_2

        # IngestTaskDeadline
        person_4 = entities.StatePerson.new_with_defaults(
            state_code=self.region_code().value,
        )
        external_id_4 = entities.StatePersonExternalId.new_with_defaults(
            state_code=self.region_code().value,
            external_id="1",
            id_type="US_DD_ID_TYPE",
            person=person_4,
        )
        person_4.external_ids = [external_id_4]

        task_deadline_1 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 1),
            update_datetime=datetime(2023, 7, 1, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_2 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 2),
            update_datetime=datetime(2023, 7, 2, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_3 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 3),
            update_datetime=datetime(2023, 7, 3, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        task_deadline_4 = entities.StateTaskDeadline.new_with_defaults(
            state_code=self.region_code().value,
            due_date=date(2023, 8, 4),
            update_datetime=datetime(2023, 7, 4, 0, 0, 0),
            task_type=StateTaskType.INTERNAL_UNKNOWN,
            person=person_4,
        )
        person_4.task_deadlines = [
            task_deadline_1,
            task_deadline_2,
            task_deadline_3,
            task_deadline_4,
        ]

        expected_root_entities: List[RootEntity] = [person_1, person_2, person_4]

        self.run_test_state_pipeline(
            expected_ingest_view_output,
            expected_root_entities,
            ingest_views_to_run=" ".join(subset_of_ingest_views),
        )

    def test_expected_pipeline_output(self) -> None:
        expected_output_entities = {
            "state_person",
            "state_person_external_id",
            "state_incarceration_sentence",
            "state_charge",
        }
        self.assertEqual(
            pipeline.get_pipeline_output_tables(expected_output_entities),
            expected_output_entities
            | {"state_charge_incarceration_sentence_association"},
        )
