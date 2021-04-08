# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=unused-import,wrong-import-order

"""Tests for incarceration/pipeline.py"""
import unittest

from freezegun import freeze_time
from typing import Optional, Set, List, Dict, Any

import apache_beam as beam
from apache_beam.pvalue import AsDict
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline

from datetime import date

from mock import patch

from recidiviz.calculator.pipeline.incarceration import pipeline
from recidiviz.calculator.pipeline.incarceration.incarceration_event import (
    IncarcerationAdmissionEvent,
    IncarcerationReleaseEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationAdmissionMetric,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    PersonMetadata,
    ExtractPersonEventsMetadata,
)
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    Gender,
    Race,
    ResidencyStatus,
    Ethnicity,
    StatePerson,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSentenceGroup,
    StateCharge,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteToBigQueryFactory,
    FakeWriteToBigQuery,
)
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    run_test_pipeline,
    test_pipeline_options,
)

_COUNTY_OF_RESIDENCE = "county_of_residence"
_STATE_CODE = "US_XX"

ALL_METRICS_INCLUSIONS_DICT = {
    IncarcerationMetricType.INCARCERATION_ADMISSION: True,
    IncarcerationMetricType.INCARCERATION_POPULATION: True,
    IncarcerationMetricType.INCARCERATION_RELEASE: True,
}

ALL_METRIC_TYPES_SET = {
    IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationMetricType.INCARCERATION_POPULATION,
    IncarcerationMetricType.INCARCERATION_RELEASE,
}

INCARCERATION_PIPELINE_PACKAGE_NAME = pipeline.__name__


class TestIncarcerationPipeline(unittest.TestCase):
    """Tests the entire incarceration pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(FakeWriteToBigQuery)

    @staticmethod
    def _default_data_dict():
        return {
            schema.StatePerson.__tablename__: [],
            schema.StatePersonRace.__tablename__: [],
            schema.StatePersonEthnicity.__tablename__: [],
            schema.StateSentenceGroup.__tablename__: [],
            schema.StateIncarcerationSentence.__tablename__: [],
            schema.StateSupervisionSentence.__tablename__: [],
            schema.StateIncarcerationPeriod.__tablename__: [],
            schema.state_incarceration_sentence_incarceration_period_association_table.name: [],
            schema.state_supervision_sentence_incarceration_period_association_table.name: [],
            schema.StatePersonExternalId.__tablename__: [],
            schema.StatePersonAlias.__tablename__: [],
            schema.StateAssessment.__tablename__: [],
            schema.StateProgramAssignment.__tablename__: [],
            schema.StateFine.__tablename__: [],
            schema.StateCharge.__tablename__: [],
            schema.StateSupervisionPeriod.__tablename__: [],
            schema.StateEarlyDischarge.__tablename__: [],
            schema.state_charge_incarceration_sentence_association_table.name: [],
            schema.state_charge_supervision_sentence_association_table.name: [],
            schema.state_incarceration_sentence_supervision_period_association_table.name: [],
            schema.state_supervision_sentence_supervision_period_association_table.name: [],
        }

    def build_incarceration_pipeline_data_dict(
        self, fake_person_id: int, state_code: str = "US_XX"
    ):
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code=state_code,
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=Race.BLACK,
            person_id=fake_person_id,
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=Race.WHITE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code=state_code,
            ethnicity=Ethnicity.HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        sentence_group = schema.StateSentenceGroup(
            sentence_group_id=98765,
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person_id=fake_person_id,
        )

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=1111,
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=sentence_group.sentence_group_id,
            incarceration_periods=[
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ],
            person_id=fake_person_id,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=123,
            state_code=state_code,
            sentence_group_id=sentence_group.sentence_group_id,
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sentence_group.incarceration_sentences = [incarceration_sentence]
        sentence_group.supervision_sentences = [supervision_sentence]

        sentence_group_data = [normalized_database_base_dict(sentence_group)]

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration),
        ]

        state_incarceration_sentence_incarceration_period_association = [
            {
                "incarceration_period_id": initial_incarceration.incarceration_period_id,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
            },
            {
                "incarceration_period_id": first_reincarceration.incarceration_period_id,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
            },
            {
                "incarceration_period_id": subsequent_reincarceration.incarceration_period_id,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
            },
        ]

        fake_person_id_to_county_query_result = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        us_mo_sentence_status_data: List[Dict[str, Any]] = [
            {
                "state_code": "US_MO",
                "person_id": fake_person_id,
                "sentence_external_id": "XXX",
                "sentence_status_external_id": "YYY",
                "status_code": "ZZZ",
                "status_date": "not_a_date",
                "status_description": "XYZ",
            }
        ]

        incarceration_period_judicial_district_association_data = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "incarceration_period_id": 123,
                "judicial_district_code": "NW",
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": state_code,
                "race_or_ethnicity": "BLACK",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        data_dict = self._default_data_dict()
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentence_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentence_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.state_incarceration_sentence_incarceration_period_association_table.name: state_incarceration_sentence_incarceration_period_association,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
        }
        data_dict.update(data_dict_overrides)
        return data_dict

    @freeze_time("2015-01-31")
    def testIncarcerationPipeline(self):
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    @freeze_time("2015-01-31")
    def testIncarcerationPipelineFilterMetrics(self):
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )
        dataset = "recidiviz-123.state"

        expected_metric_types = {IncarcerationMetricType.INCARCERATION_ADMISSION}
        metric_types_filter = {IncarcerationMetricType.INCARCERATION_ADMISSION.value}

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=expected_metric_types,
            metric_types_filter=metric_types_filter,
        )

    def testIncarcerationPipelineUsMo(self):
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id, state_code="US_MO"
        )
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            state_code="US_MO",
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    def testIncarcerationPipelineWithFilterSet(self):
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )
        dataset = "recidivz-staging.state"

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
            unifying_id_field_filter_set={fake_person_id},
        )

    def run_test_pipeline(
        self,
        state_code: str,
        dataset: str,
        data_dict: Dict[str, List[Dict]],
        expected_metric_types: Set[IncarcerationMetricType],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            )
        )
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                dataset,
                expected_output_metric_types=expected_metric_types,
            )
        )
        with patch(
            f"{INCARCERATION_PIPELINE_PACKAGE_NAME}.ReadFromBigQuery",
            read_from_bq_constructor,
        ):
            run_test_pipeline(
                pipeline_module=pipeline,
                state_code=state_code,
                dataset=dataset,
                read_from_bq_constructor=read_from_bq_constructor,
                write_to_bq_constructor=write_to_bq_constructor,
                unifying_id_field_filter_set=unifying_id_field_filter_set,
                metric_types_filter=metric_types_filter,
            )

    def build_incarceration_pipeline_data_dict_no_incarceration(
        self, fake_person_id: int
    ):
        """Builds a data_dict for a run of the pipeline where the person has no incarceration."""
        fake_person_1 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        fake_person_id_2 = 6789

        fake_person_2 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id_2,
            gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [
            normalized_database_base_dict(fake_person_1),
            normalized_database_base_dict(fake_person_2),
        ]

        sentence_group = schema.StateSentenceGroup(
            sentence_group_id=111,
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person_id=fake_person_id,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=1111,
            state_code="US_XX",
            sentence_group_id=sentence_group.sentence_group_id,
            incarceration_periods=[],
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=123,
            state_code="US_XX",
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sentence_group.incarceration_sentences = [incarceration_sentence]

        sentence_group_data = [normalized_database_base_dict(sentence_group)]

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_periods_data: Dict[str, Any] = {}

        state_incarceration_sentence_incarceration_period_association = [
            {
                "incarceration_period_id": None,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
            },
        ]

        fake_person_id_to_county_query_result = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        us_mo_sentence_status_data: List[Dict[str, Any]] = [
            {
                "state_code": "US_MO",
                "person_id": fake_person_id,
                "sentence_external_id": "XXX",
                "sentence_status_external_id": "YYY",
                "status_code": "ZZZ",
                "status_date": "not_a_date",
                "status_description": "XYZ",
            }
        ]

        incarceration_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "incarceration_period_id": None,
                "judicial_district_code": "NW",
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": "US_XX",
                "race_or_ethnicity": "BLACK",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        data_dict = self._default_data_dict()
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentence_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentence_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.state_incarceration_sentence_incarceration_period_association_table.name: state_incarceration_sentence_incarceration_period_association,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def testIncarcerationPipelineNoIncarceration(self):
        """Tests the incarceration pipeline when a person does not have any
        incarceration periods."""
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict_no_incarceration(
            fake_person_id
        )
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            _STATE_CODE, dataset, data_dict, expected_metric_types=set()
        )


class TestClassifyIncarcerationEvents(unittest.TestCase):
    """Tests the ClassifyIncarcerationEvents DoFn in the pipeline."""

    def testClassifyIncarcerationEvents(self):
        """Tests the ClassifyIncarcerationEvents DoFn."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="TX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            start_date=date(2009, 2, 9),
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    ncic_code="5699",
                    statute="30A123",
                    offense_date=date(2009, 1, 9),
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=123,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        incarceration_period.incarceration_sentences = [incarceration_sentence]

        fake_person_id_to_county_query_result = {
            "person_id": fake_person_id,
            "county_of_residence": _COUNTY_OF_RESIDENCE,
        }

        fake_incarceration_period_judicial_district_association_result = {
            "person_id": fake_person_id,
            "incarceration_period_id": 123,
            "judicial_district_code": "NW",
        }

        incarceration_events = [
            IncarcerationStayEvent(
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code="5699",
                most_serious_offense_statute="30A123",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=incarceration_period.release_reason,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
        ]

        correct_output = [(fake_person_id, (fake_person, incarceration_events))]

        test_pipeline = TestPipeline()

        person_entities = {
            "person": [fake_person],
            "sentence_groups": [sentence_group],
            "incarceration_period_judicial_district_association": [
                fake_incarceration_period_judicial_district_association_result
            ],
            "persons_to_recent_county_of_residence": [
                fake_person_id_to_county_query_result
            ],
        }

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(pipeline.ClassifyIncarcerationEvents())
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyIncarcerationEvents_NoSentenceGroups(self):
        """Tests the ClassifyIncarcerationEvents DoFn when the person has no sentence groups."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        person_periods = {
            "person": [fake_person],
            "sentence_groups": [],
            "incarceration_period_judicial_district_association": [],
            "persons_to_recent_county_of_residence": [],
        }

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person.person_id, person_periods)])
            | "Identify Incarceration Events"
            >> beam.ParDo(pipeline.ClassifyIncarcerationEvents())
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testClassifyIncarcerationEventsWithPeriodsAfterDeath(self):
        """Tests the ClassifyIncarcerationEvents DoFn for when a person has periods
        that occur after a period ending in their death."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period_with_death = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        post_mortem_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2010, 11, 23),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        post_mortem_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[
                incarceration_period_with_death,
                post_mortem_incarceration_period_1,
                post_mortem_incarceration_period_2,
            ],
            start_date=date(2009, 2, 9),
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    ncic_code="5699",
                    statute="30A123",
                    offense_date=date(2009, 1, 9),
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=123,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        incarceration_period_with_death.incarceration_sentences = [
            incarceration_sentence
        ]
        post_mortem_incarceration_period_1.incarceration_sentences = [
            incarceration_sentence
        ]
        post_mortem_incarceration_period_2.incarceration_sentences = [
            incarceration_sentence
        ]

        fake_person_id_to_county_query_result = {
            "person_id": fake_person_id,
            "county_of_residence": _COUNTY_OF_RESIDENCE,
        }

        fake_incarceration_period_judicial_district_association_result = {
            "person_id": fake_person_id,
            "incarceration_period_id": 123,
            "judicial_district_code": "NW",
        }

        incarceration_events = [
            IncarcerationStayEvent(
                admission_reason=incarceration_period_with_death.admission_reason,
                admission_reason_raw_text=incarceration_period_with_death.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.admission_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code="5699",
                most_serious_offense_statute="30A123",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationAdmissionEvent(
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.admission_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period_with_death.admission_reason,
                admission_reason_raw_text=incarceration_period_with_death.admission_reason_raw_text,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.release_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=incarceration_period_with_death.release_reason,
                admission_reason=incarceration_period_with_death.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_with_death.release_date
                    - incarceration_period_with_death.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
        ]

        correct_output = [(fake_person_id, (fake_person, incarceration_events))]

        test_pipeline = TestPipeline()

        person_entities = {
            "person": [fake_person],
            "sentence_groups": [sentence_group],
            "incarceration_period_judicial_district_association": [
                fake_incarceration_period_judicial_district_association_result
            ],
            "persons_to_recent_county_of_residence": [
                fake_person_id_to_county_query_result
            ],
        }

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(pipeline.ClassifyIncarcerationEvents())
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the ProduceIncarcerationMetrics DoFn
    in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

    def testProduceIncarcerationMetrics(self):
        """Tests the ProduceIncarcerationMetrics DoFn."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code="US_XX",
                event_date=date(2001, 3, 16),
                facility="SAN QUENTIN",
                county_of_residence="county_of_residence",
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2002, 5, 26),
                facility="SAN QUENTIN",
                county_of_residence="county_of_residence",
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            ),
        ]

        expected_metric_count = 1

        expected_metric_counts = {
            "admissions": expected_metric_count,
            "releases": expected_metric_count,
        }

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    "person_events": [(fake_person, incarceration_events)],
                    "person_metadata": [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                pipeline.ProduceIncarcerationMetrics(),
                None,
                -1,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
            )
        )

        assert_that(
            output,
            AssertMatchers.count_metrics(expected_metric_counts),
            "Assert number of metrics is expected value",
        )

        test_pipeline.run()

    def testProduceIncarcerationMetrics_NoIncarceration(self):
        """Tests the ProduceIncarcerationMetrics when there are
        no incarceration_events. This should never happen because any person
        without incarceration events is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    "person_incarceration_events": [(fake_person, [])],
                    "person_metadata": [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                pipeline.ProduceIncarcerationMetrics(),
                None,
                -1,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceIncarcerationMetrics_NoInput(self):
        """Tests the ProduceIncarcerationMetrics when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                pipeline.ProduceIncarcerationMetrics(),
                None,
                -1,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_metric_type(allow_empty: bool = False):
        def _validate_metric_type(output):
            if not allow_empty and not output:
                raise BeamAssertException("Output metrics unexpectedly empty")

            for metric in output:
                if not isinstance(metric, IncarcerationMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type" "IncarcerationMetric."
                    )

        return _validate_metric_type

    @staticmethod
    def count_metrics(expected_metric_counts):
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output):
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                metric_type = metric.metric_type

                if metric_type == IncarcerationMetricType.INCARCERATION_ADMISSION:
                    actual_combination_counts["admissions"] = (
                        actual_combination_counts["admissions"] + 1
                    )
                elif metric_type == IncarcerationMetricType.INCARCERATION_RELEASE:
                    actual_combination_counts["releases"] = (
                        actual_combination_counts["releases"] + 1
                    )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_combination_counts[key]:

                    raise BeamAssertException(
                        "Failed assert. Count does not" "match expected value."
                    )

        return _count_metrics

    @staticmethod
    def validate_pipeline_test(expected_metric_types: Set[IncarcerationMetricType]):
        """Asserts that the pipeline produced the expected types of metrics."""

        def _validate_pipeline_test(output):
            observed_metric_types: Set[IncarcerationMetricType] = set()

            for metric in output:
                if not isinstance(metric, IncarcerationMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type SupervisionMetric."
                    )

                if isinstance(metric, IncarcerationAdmissionMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_ADMISSION
                    )
                elif isinstance(metric, IncarcerationPopulationMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_POPULATION
                    )
                elif isinstance(metric, IncarcerationReleaseMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_RELEASE
                    )

            if observed_metric_types != expected_metric_types:
                raise BeamAssertException(
                    f"Failed assert. Expected metric types {expected_metric_types} does not equal"
                    f" observed metric types {observed_metric_types}."
                )

        return _validate_pipeline_test
