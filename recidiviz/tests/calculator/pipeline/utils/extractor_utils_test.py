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
"""Tests for utils/extractor_utils.py."""
# pylint: disable=protected-access
import unittest
from datetime import date
from typing import Type

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from mock import patch

from recidiviz.calculator.pipeline.utils import extractor_utils
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
    remove_relationship_properties,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQuery,
    FakeReadFromBigQueryFactory,
)
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    default_data_dict_for_root_schema_classes,
)
from recidiviz.tests.persistence.database import database_test_utils


class TestBuildRootEntity(unittest.TestCase):
    """"Tests the BuildRootEntity PTransform."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

    def testBuildRootEntity(self):
        """Tests building a root StatePerson with no related attributes."""

        fake_person = schema.StatePerson(
            person_id=12345,
            current_address="123 Street",
            full_name="Jack Smith",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="US_XX",
        )

        fake_person_data = [normalized_database_base_dict(fake_person)]

        data_dict = {schema.StatePerson.__tablename__: fake_person_data}

        fake_person_entity = StateSchemaToEntityConverter().convert(fake_person)
        dataset = "recidiviz-123.state"

        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=dataset,
                root_entity_class=entities.StatePerson,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=False,
                state_code=fake_person.state_code,
            )

            assert_that(output, equal_to([(12345, fake_person_entity)]))

            test_pipeline.run()

    def testBuildRootEntity_HydratedRelationshipProperties(self):
        """Tests the extraction of a valid StatePerson entity with cross-entity
        relationship properties hydrated."""

        fake_person_id = 12345

        fake_person = schema.StatePerson(
            person_id=fake_person_id,
            current_address="123 Street",
            full_name="Bernard Madoff",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="US_XX",
        )

        fake_person_data = [normalized_database_base_dict(fake_person)]

        ethnicity_1 = schema.StatePersonEthnicity(
            state_code="US_XX",
            ethnicity=Ethnicity.NOT_HISPANIC,
            person_id=fake_person_id,
            person_ethnicity_id=234,
        )

        ethnicities_data = [normalized_database_base_dict(ethnicity_1)]

        alias_1 = schema.StatePersonAlias(
            state_code="US_XX",
            full_name="Bernie Madoff",
            person_alias_id=18615,
            person_id=fake_person_id,
        )

        alias_data = [normalized_database_base_dict(alias_1)]

        external_id_1 = schema.StatePersonExternalId(
            person_external_id_id=999,
            external_id="888",
            state_code="US_XX",
            id_type="US_XX_TYPE",
            person_id=fake_person_id,
        )

        external_ids_data = [normalized_database_base_dict(external_id_1)]

        sentence_group_1 = schema.StateSentenceGroup(
            status=StateSentenceStatus.SERVING,
            date_imposed=date(2011, 3, 7),
            state_code="US_XX",
            min_length_days=199,
            max_length_days=500,
            sentence_group_id=213,
            person_id=fake_person_id,
        )

        sentence_group_data = [normalized_database_base_dict(sentence_group_1)]

        race_1 = schema.StatePersonRace(
            race=Race.WHITE, state_code="US_XX", person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            race=Race.BLACK, state_code="US_XX", person_id=fake_person_id
        )

        races_data = [
            normalized_database_base_dict(race_1),
            normalized_database_base_dict(race_2),
        ]

        assessment_1 = schema.StateAssessment(
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2012, 4, 1),
            state_code="US_XX",
            assessment_score=29,
            assessment_id=184672,
            person_id=fake_person_id,
        )

        assessment_data = [normalized_database_base_dict(assessment_1)]
        data_dict = default_data_dict_for_root_schema_classes([schema.StatePerson])

        data_dict_overrides = {
            schema.StatePerson.__tablename__: fake_person_data,
            schema.StatePersonEthnicity.__tablename__: ethnicities_data,
            schema.StatePersonAlias.__tablename__: alias_data,
            schema.StatePersonExternalId.__tablename__: external_ids_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StatePersonRace.__tablename__: races_data,
        }
        data_dict.update(data_dict_overrides)

        fake_person_entity = StateSchemaToEntityConverter().convert(fake_person)

        fake_person_entity.ethnicities = StateSchemaToEntityConverter().convert_all(
            [ethnicity_1]
        )

        fake_person_entity.aliases = StateSchemaToEntityConverter().convert_all(
            [alias_1]
        )

        fake_person_entity.external_ids = StateSchemaToEntityConverter().convert_all(
            [external_id_1]
        )

        fake_person_entity.sentence_groups = StateSchemaToEntityConverter().convert_all(
            [sentence_group_1]
        )

        fake_person_entity.races = StateSchemaToEntityConverter().convert_all(
            [race_1, race_2]
        )

        fake_person_entity.assessments = StateSchemaToEntityConverter().convert_all(
            [assessment_1]
        )
        dataset = "recidiviz-123.state"

        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=dataset,
                root_entity_class=entities.StatePerson,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                state_code=fake_person.state_code,
            )

            assert_that(output, equal_to([(12345, fake_person_entity)]))

            test_pipeline.run()

    def testBuildRootEntity_DoNotHydrateRelationshipProperties(self):
        """Tests the extraction of a valid StatePerson entity with cross-entity
        relationship properties that are present in the data_dict but should not
        be hydrated."""

        fake_person_id = 12345

        fake_person = schema.StatePerson(
            person_id=fake_person_id,
            current_address="123 Street",
            full_name="Bernard Madoff",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="US_XX",
        )

        fake_person_data = [normalized_database_base_dict(fake_person)]

        ethnicity_1 = schema.StatePersonEthnicity(
            state_code="US_XX",
            ethnicity=Ethnicity.NOT_HISPANIC,
            person_id=fake_person_id,
            person_ethnicity_id=234,
        )

        ethnicities_data = [normalized_database_base_dict(ethnicity_1)]

        alias_1 = schema.StatePersonAlias(
            state_code="US_XX",
            full_name="Bernie Madoff",
            person_alias_id=18615,
            person_id=fake_person_id,
        )

        alias_data = [normalized_database_base_dict(alias_1)]

        external_id_1 = schema.StatePersonExternalId(
            person_external_id_id=999,
            external_id="888",
            state_code="US_XX",
            id_type="US_XX_TYPE",
            person_id=fake_person_id,
        )

        external_ids_data = [normalized_database_base_dict(external_id_1)]

        sentence_group_1 = schema.StateSentenceGroup(
            status=StateSentenceStatus.SERVING,
            date_imposed=date(2011, 3, 7),
            state_code="US_XX",
            min_length_days=199,
            max_length_days=500,
            sentence_group_id=213,
            person_id=fake_person_id,
        )

        sentence_group_data = [normalized_database_base_dict(sentence_group_1)]

        race_1 = schema.StatePersonRace(
            race=Race.WHITE, state_code="US_XX", person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            race=Race.BLACK, state_code="US_XX", person_id=fake_person_id
        )

        races_data = [
            normalized_database_base_dict(race_1),
            normalized_database_base_dict(race_2),
        ]

        assessment_1 = schema.StateAssessment(
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2012, 4, 1),
            state_code="US_XX",
            assessment_score=29,
            assessment_id=184672,
            person_id=fake_person_id,
        )

        assessment_data = [normalized_database_base_dict(assessment_1)]

        data_dict = {
            schema.StatePerson.__tablename__: fake_person_data,
            schema.StatePersonEthnicity.__tablename__: ethnicities_data,
            schema.StatePersonAlias.__tablename__: alias_data,
            schema.StatePersonExternalId.__tablename__: external_ids_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StatePersonRace.__tablename__: races_data,
        }

        fake_person_entity = StateSchemaToEntityConverter().convert(fake_person)
        dataset = "recidiviz-123.state"

        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=dataset,
                root_entity_class=entities.StatePerson,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=False,
                state_code=fake_person.state_code,
            )

            assert_that(output, equal_to([(12345, fake_person_entity)]))

            test_pipeline.run()

    def testBuildRootEntity_NoDataSource(self):
        """Tests the BuildRootEntity PTransform when there is no valid data
        source."""
        with self.assertRaisesRegex(
            ValueError, "No valid data source passed to the pipeline"
        ):
            test_pipeline = TestPipeline()

            _ = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=None,
                root_entity_class=entities.StateIncarcerationSentence,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                state_code="US_XX",
            )

            test_pipeline.run()

    def testBuildRootEntity_EmptyEntityClass(self):
        """Tests the BuildRootEntity PTransform when the |root_entity_class|
        is None."""

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            123, []
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation = remove_relationship_properties(supervision_violation)

        data_dict = {supervision_violation.__tablename__: supervision_violation_data}

        with self.assertRaisesRegex(
            ValueError, "^BuildRootEntity: Expecting root_entity_class to be not None.$"
        ):
            dataset = "recidiviz-123.state"

            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                _ = test_pipeline | extractor_utils.BuildRootEntity(
                    dataset=dataset,
                    root_entity_class=None,
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    build_related_entities=True,
                    state_code="US_XX",
                )

                test_pipeline.run()

    def testBuildRootEntity_InvalidUnifyingIdField(self):
        """Tests the BuildRootEntity PTransform when the |unifying_id_field|
        is not a valid field on the root_schema_class."""

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            123, []
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation = remove_relationship_properties(supervision_violation)

        data_dict = {supervision_violation.__tablename__: supervision_violation_data}

        with self.assertRaisesRegex(
            ValueError,
            r"^Root entity class \[StateSupervisionViolation\] does not have unifying id field \[XX\]$",
        ):
            dataset = "recidiviz-123.state"

            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                _ = test_pipeline | extractor_utils.BuildRootEntity(
                    dataset=dataset,
                    root_entity_class=entities.StateSupervisionViolation,
                    unifying_id_field="XX",
                    build_related_entities=True,
                    state_code=supervision_violation.state_code,
                )

                test_pipeline.run()

    def testBuildRootEntity_EmptyUnifyingIdField(self):
        """Tests the BuildRootEntity PTransform when the |unifying_id_field|
        is None."""

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            123, []
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation = remove_relationship_properties(supervision_violation)

        data_dict = {supervision_violation.__tablename__: supervision_violation_data}

        dataset = "recidiviz-123.state"
        with self.assertRaisesRegex(
            ValueError, "No valid unifying_id_field passed to the pipeline."
        ):
            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                _ = test_pipeline | extractor_utils.BuildRootEntity(
                    dataset=dataset,
                    root_entity_class=entities.StateSupervisionViolation,
                    unifying_id_field=None,
                    build_related_entities=True,
                    state_code="US_XX",
                )

                test_pipeline.run()

    def testBuildRootEntity_HydratedRelationships_InvalidUnifyingIdField(self):
        """Tests the BuildRootEntity PTransform when the the |unifying_id_field|
        is a valid field on the root_schema_class, but is not a valid field
        on one of the relationship entities.

        In this case, schema.StateSupervisionViolation has a field
        'supervision_period_id', but schema.StateSupervisionViolationResponse
        does not have this field. This means you will not be able to properly
        connect this related entity.

        We expect that we do not hydrate StateSupervisionViolationResponses in this case.
        """

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(123)
        )

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            123, [supervision_violation_response]
        )

        supervision_violation.supervision_period_id = 444

        supervision_violation.supervision_violation_responses = [
            supervision_violation_response
        ]

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        data_dict = {
            supervision_violation.__tablename__: supervision_violation_data,
            supervision_violation_response.__tablename__: supervision_violation_response_data,
        }

        with patch("logging.Logger.warning") as mock:
            dataset = "recidiviz-123.state"
            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                output = test_pipeline | extractor_utils.BuildRootEntity(
                    dataset=dataset,
                    root_entity_class=entities.StateSupervisionViolation,
                    unifying_id_field="supervision_period_id",
                    build_related_entities=True,
                    state_code=supervision_violation.state_code,
                )

                output_violation_entity = StateSchemaToEntityConverter().convert(
                    supervision_violation
                )

                output_violation_entity.supervision_violation_responses = []
                output_violation_entity.supervision_violation_types = []
                output_violation_entity.supervision_violated_conditions = []

                assert_that(
                    output,
                    equal_to(
                        [
                            (
                                supervision_violation.supervision_period_id,
                                output_violation_entity,
                            )
                        ]
                    ),
                )

                test_pipeline.run()

            mock.assert_not_called()

    def testBuildRootEntity_HydratedRelationshipProperties_StateCodeFilter_Mismatch(
        self,
    ):
        """Tests the extraction of a valid StatePerson entity with cross-entity relationship properties hydrated,
        where the state_codes on the hydrated entities do not match the state_code filter."""

        fake_person_id = 12345

        fake_person = schema.StatePerson(
            person_id=fake_person_id,
            current_address="123 Street",
            full_name="Bernard Madoff",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="FL",
        )

        fake_person_data = [normalized_database_base_dict(fake_person)]

        ethnicity_1 = schema.StatePersonEthnicity(
            state_code="CA",
            ethnicity=Ethnicity.NOT_HISPANIC,
            person_id=fake_person_id,
            person_ethnicity_id=234,
        )

        ethnicities_data = [normalized_database_base_dict(ethnicity_1)]

        alias_1 = schema.StatePersonAlias(
            state_code="NY",
            full_name="Bernie Madoff",
            person_alias_id=18615,
            person_id=fake_person_id,
        )

        alias_data = [normalized_database_base_dict(alias_1)]

        external_id_1 = schema.StatePersonExternalId(
            person_external_id_id=999,
            external_id="888",
            state_code="CA",
            id_type="US_XX_TYPE",
            person_id=fake_person_id,
        )

        external_ids_data = [normalized_database_base_dict(external_id_1)]

        sentence_group_1 = schema.StateSentenceGroup(
            status=StateSentenceStatus.SERVING,
            date_imposed=date(2011, 3, 7),
            state_code="CA",
            min_length_days=199,
            max_length_days=500,
            sentence_group_id=213,
            person_id=fake_person_id,
        )

        sentence_group_data = [normalized_database_base_dict(sentence_group_1)]

        race_1 = schema.StatePersonRace(
            race=Race.WHITE, state_code="CA", person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            race=Race.BLACK, state_code="CA", person_id=fake_person_id
        )

        races_data = [
            normalized_database_base_dict(race_1),
            normalized_database_base_dict(race_2),
        ]

        assessment_1 = schema.StateAssessment(
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2012, 4, 1),
            state_code="CA",
            assessment_score=29,
            assessment_id=184672,
            person_id=fake_person_id,
        )

        assessment_data = [normalized_database_base_dict(assessment_1)]

        data_dict = default_data_dict_for_root_schema_classes([schema.StatePerson])

        data_dict_overrides = {
            schema.StatePerson.__tablename__: fake_person_data,
            schema.StatePersonEthnicity.__tablename__: ethnicities_data,
            schema.StatePersonAlias.__tablename__: alias_data,
            schema.StatePersonExternalId.__tablename__: external_ids_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StateProgramAssignment.__tablename__: [],
        }
        data_dict.update(data_dict_overrides)

        fake_person_entity = StateSchemaToEntityConverter().convert(fake_person)

        fake_person_entity.ethnicities = StateSchemaToEntityConverter().convert_all(
            [ethnicity_1]
        )

        fake_person_entity.aliases = StateSchemaToEntityConverter().convert_all(
            [alias_1]
        )

        fake_person_entity.external_ids = StateSchemaToEntityConverter().convert_all(
            [external_id_1]
        )

        fake_person_entity.sentence_groups = StateSchemaToEntityConverter().convert_all(
            [sentence_group_1]
        )

        fake_person_entity.races = StateSchemaToEntityConverter().convert_all(
            [race_1, race_2]
        )

        fake_person_entity.assessments = StateSchemaToEntityConverter().convert_all(
            [assessment_1]
        )

        empty_person = schema.StatePerson(
            person_id=fake_person_id,
            current_address="123 Street",
            full_name="Bernard Madoff",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="FL",
        )

        empty_person_entity = StateSchemaToEntityConverter().convert(empty_person)

        dataset = "recidiviz-123.state"

        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=dataset,
                root_entity_class=entities.StatePerson,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                state_code="FL",
            )

            assert_that(output, equal_to([(12345, empty_person_entity)]))

            test_pipeline.run()

    def testBuildRootEntity_HydratedRelationshipProperties_StateCodeFilter_Match(self):
        """Tests the extraction of a valid StatePerson entity with cross-entity relationship properties hydrated,
        where the state_codes on the hydrated entities match the state_code filter."""

        fake_person_id = 12345

        fake_person = schema.StatePerson(
            person_id=fake_person_id,
            current_address="123 Street",
            full_name="Bernard Madoff",
            birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT,
            state_code="US_XX",
        )

        fake_person_data = [normalized_database_base_dict(fake_person)]

        ethnicity_1 = schema.StatePersonEthnicity(
            state_code="US_XX",
            ethnicity=Ethnicity.NOT_HISPANIC,
            person_id=fake_person_id,
            person_ethnicity_id=234,
        )

        ethnicities_data = [normalized_database_base_dict(ethnicity_1)]

        alias_1 = schema.StatePersonAlias(
            state_code="US_XX",
            full_name="Bernie Madoff",
            person_alias_id=18615,
            person_id=fake_person_id,
        )

        alias_data = [normalized_database_base_dict(alias_1)]

        external_id_1 = schema.StatePersonExternalId(
            person_external_id_id=999,
            external_id="888",
            state_code="US_XX",
            id_type="US_XX_TYPE",
            person_id=fake_person_id,
        )

        external_ids_data = [normalized_database_base_dict(external_id_1)]

        sentence_group_1 = schema.StateSentenceGroup(
            status=StateSentenceStatus.SERVING,
            date_imposed=date(2011, 3, 7),
            state_code="US_XX",
            min_length_days=199,
            max_length_days=500,
            sentence_group_id=213,
            person_id=fake_person_id,
        )

        sentence_group_data = [normalized_database_base_dict(sentence_group_1)]

        race_1 = schema.StatePersonRace(
            race=Race.WHITE, state_code="US_XX", person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            race=Race.BLACK, state_code="US_XX", person_id=fake_person_id
        )

        races_data = [
            normalized_database_base_dict(race_1),
            normalized_database_base_dict(race_2),
        ]

        assessment_1 = schema.StateAssessment(
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2012, 4, 1),
            state_code="US_XX",
            assessment_score=29,
            assessment_id=184672,
            person_id=fake_person_id,
        )

        assessment_data = [normalized_database_base_dict(assessment_1)]

        data_dict = default_data_dict_for_root_schema_classes([schema.StatePerson])

        data_dict_overrides = {
            schema.StatePerson.__tablename__: fake_person_data,
            schema.StatePersonEthnicity.__tablename__: ethnicities_data,
            schema.StatePersonAlias.__tablename__: alias_data,
            schema.StatePersonExternalId.__tablename__: external_ids_data,
            schema.StateSentenceGroup.__tablename__: sentence_group_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StatePersonRace.__tablename__: races_data,
        }
        data_dict.update(data_dict_overrides)

        fake_person_entity = StateSchemaToEntityConverter().convert(fake_person)

        fake_person_entity.ethnicities = StateSchemaToEntityConverter().convert_all(
            [ethnicity_1]
        )

        fake_person_entity.aliases = StateSchemaToEntityConverter().convert_all(
            [alias_1]
        )

        fake_person_entity.external_ids = StateSchemaToEntityConverter().convert_all(
            [external_id_1]
        )

        fake_person_entity.sentence_groups = StateSchemaToEntityConverter().convert_all(
            [sentence_group_1]
        )

        fake_person_entity.races = StateSchemaToEntityConverter().convert_all(
            [race_1, race_2]
        )

        fake_person_entity.assessments = StateSchemaToEntityConverter().convert_all(
            [assessment_1]
        )

        dataset = "recidiviz-123.state"

        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | extractor_utils.BuildRootEntity(
                dataset=dataset,
                root_entity_class=entities.StatePerson,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                state_code="US_XX",
            )

            assert_that(output, equal_to([(12345, fake_person_entity)]))

            test_pipeline.run()


class TestExtractEntity(unittest.TestCase):
    """Tests the ExtractEntity PTransform."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

    def testExtractEntity(self):
        person = remove_relationship_properties(
            database_test_utils.generate_test_person(123, "US_XX", [], None, [])
        )

        person_data = [normalized_database_base_dict(person)]

        data_dict = {person.__tablename__: person_data}

        output_person_entity = StateSchemaToEntityConverter().convert(person)

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StatePerson"
        )

        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = (
                test_pipeline
                | "Extract StatePerson Entity"
                >> extractor_utils._ExtractEntity(
                    dataset=dataset,
                    entity_class=entity_class,
                    unifying_id_field=entity_class.get_class_id_name(),
                    parent_id_field=None,
                    unifying_id_field_filter_set=None,
                    state_code=person.state_code,
                )
            )

            assert_that(
                output,
                equal_to([(output_person_entity.get_id(), output_person_entity)]),
            )

            test_pipeline.run()

    def testExtractEntity_InvalidUnifyingIdField(self):
        person = remove_relationship_properties(
            database_test_utils.generate_test_person(123, "US_XX", [], None, [])
        )

        person_data = [normalized_database_base_dict(person)]

        data_dict = {person.__tablename__: person_data}

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StatePerson"
        )

        with patch("logging.Logger.warning") as mock:
            dataset = "recidiviz-123.state"
            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                output = (
                    test_pipeline
                    | "Extract StatePerson Entity"
                    >> extractor_utils._ExtractEntity(
                        dataset=dataset,
                        entity_class=entity_class,
                        unifying_id_field="XX",
                        parent_id_field="person_id",
                        unifying_id_field_filter_set=None,
                        state_code=None,
                    )
                )

                assert_that(output, equal_to([]))

                test_pipeline.run()

            mock.assert_not_called()

    def testExtractEntity_InvalidRootIdField(self):
        incarceration_period = remove_relationship_properties(
            database_test_utils.generate_test_incarceration_period(123, [])
        )

        incarceration_period_data = [
            normalized_database_base_dict(incarceration_period)
        ]

        data_dict = {incarceration_period.__tablename__: incarceration_period_data}

        with patch("logging.Logger.warning") as mock:
            dataset = "recidiviz-123.state"
            with patch(
                "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
                self.fake_bq_source_factory.create_fake_bq_source_constructor(
                    dataset, data_dict
                ),
            ):
                test_pipeline = TestPipeline()

                _ = (
                    test_pipeline
                    | "Extract StatePerson Entity"
                    >> extractor_utils._ExtractEntity(
                        dataset=dataset,
                        entity_class=entities.StateIncarcerationPeriod,
                        unifying_id_field=entities.StatePerson.get_class_id_name(),
                        parent_id_field="AAA",
                        unifying_id_field_filter_set=None,
                        state_code=incarceration_period.state_code,
                    )
                )

                test_pipeline.run()

            mock.assert_called_with(
                "Invalid inner_connection_id_field: %s." "Dropping this entity.", "AAA"
            )


class TestExtractRelationshipPropertyEntities(unittest.TestCase):
    """Tests the ExtractRelationshipPropertyEntities PTransform."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

    def testExtractRelationshipPropertyEntities_With1ToMany(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        are 1-to-many relationships to be hydrated."""
        person = database_test_utils.generate_test_person(123, "US_XX", [], None, [])

        assessment = database_test_utils.generate_test_assessment(
            person_id=person.person_id
        )

        data_dict = default_data_dict_for_root_schema_classes([schema.StatePerson])

        data_dict_overrides = {
            person.__tablename__: normalized_database_base_dict_list([person]),
            assessment.__tablename__: normalized_database_base_dict_list([assessment]),
        }
        data_dict.update(data_dict_overrides)

        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "StatePerson"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StatePerson,
                    parent_id_field="person_id",
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=person.state_code,
                )
            )

            # Assert it has the property fields we expect
            self.assertEqual(
                properties_dict.keys(),
                {
                    "aliases",
                    "ethnicities",
                    "external_ids",
                    "races",
                    "assessments",
                    "program_assignments",
                    "incarceration_incidents",
                    "sentence_groups",
                    "supervising_officer",
                },
            )

            output_assessments = properties_dict.get("assessments")

            assert_that(
                output_assessments,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=person.person_id,
                    inner_connection_id=person.person_id,
                    class_type=entities.StateAssessment,
                ),
                label="Validate assessments output",
            )

            test_pipeline.run()

    def testExtractRelationshipPropertyEntities_WithManyToMany(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        are many-to-many relationships to be hydrated."""
        incarceration_sentence = (
            database_test_utils.generate_test_incarceration_sentence(123, [], [])
        )

        supervision_period = database_test_utils.generate_test_supervision_period(
            123, [], [], []
        )

        # Build association table for many-to-many relationship
        incarceration_sentence_supervision_period_association_table = [
            {
                "supervision_period_id": supervision_period.supervision_period_id,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
            }
        ]

        data_dict = {
            supervision_period.__tablename__: normalized_database_base_dict_list(
                [supervision_period]
            ),
            incarceration_sentence.__tablename__: normalized_database_base_dict_list(
                [incarceration_sentence]
            ),
            schema.state_incarceration_sentence_supervision_period_association_table.name: incarceration_sentence_supervision_period_association_table,
            schema.StateCharge.__tablename__: [],
            schema.StateIncarcerationPeriod.__tablename__: [],
            schema.state_incarceration_sentence_incarceration_period_association_table.name: [],
            schema.state_charge_incarceration_sentence_association_table.name: [],
            schema.StateEarlyDischarge.__tablename__: [],
        }
        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "StateIncarcerationSentence"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StateIncarcerationSentence,
                    parent_id_field="incarceration_sentence_id",
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=incarceration_sentence.state_code,
                )
            )

            # Assert it has the property fields we expect
            self.assertEqual(
                properties_dict.keys(),
                {
                    "charges",
                    "incarceration_periods",
                    "supervision_periods",
                    "early_discharges",
                },
            )

            output_supervision_periods = properties_dict.get("supervision_periods")

            assert_that(
                output_supervision_periods,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=incarceration_sentence.person_id,
                    inner_connection_id=incarceration_sentence.incarceration_sentence_id,
                    class_type=entities.StateSupervisionPeriod,
                ),
                label="Validate supervision_period output",
            )

            test_pipeline.run()

    def testExtractRelationshipPropertyEntities_With1To1(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        is a 1-to-1 relationship to be hydrated (from the point of view of the
        root schema object).
        """
        court_case = database_test_utils.generate_test_court_case(person_id=123)
        charge = database_test_utils.generate_test_charge(person_id=123, charge_id=345)

        # 1 to 1 relationship
        charge.court_case_id = court_case.court_case_id

        data_dict = {
            charge.__tablename__: normalized_database_base_dict_list([charge]),
            court_case.__tablename__: normalized_database_base_dict_list([court_case]),
        }
        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "IncarcerationIncident"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StateCharge,
                    parent_id_field=charge.get_class_id_name(),
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=court_case.state_code,
                )
            )

            # Assert it has the property fields we expect
            self.assertEqual(properties_dict.keys(), {"court_case"})

            output_court_case = properties_dict.get("court_case")

            assert_that(
                output_court_case,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=charge.person_id,
                    inner_connection_id=charge.charge_id,
                    class_type=entities.StateCourtCase,
                ),
                label="Validate state_agent output",
            )

            test_pipeline.run()

    def testExtractRelationshipPropertyEntities_With1To1NoUnifyingId(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        is a 1-to-1 relationship to be hydrated and the child object does not have
        the unifying id. In this case we expect not to hydrate the child.
        """

        incarceration_incident = (
            database_test_utils.generate_test_incarceration_incident(123, [])
        )
        responding_officer = database_test_utils.generate_test_assessment_agent()
        responding_officer.person_id = 123

        # 1 to 1 relationship
        incarceration_incident.responding_officer_id = responding_officer.agent_id

        data_dict = {
            incarceration_incident.__tablename__: normalized_database_base_dict_list(
                [incarceration_incident]
            ),
            responding_officer.__tablename__: normalized_database_base_dict_list(
                [responding_officer]
            ),
            schema.StateIncarcerationIncidentOutcome.__tablename__: [],
        }
        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):

            test_pipeline = TestPipeline()

            properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "IncarcerationIncident"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StateIncarcerationIncident,
                    parent_id_field="incarceration_incident_id",
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=incarceration_incident.state_code,
                )
            )

            # Assert it has the property fields we expect
            self.assertEqual(
                properties_dict.keys(),
                {"responding_officer", "incarceration_incident_outcomes"},
            )

            output_responding_officer = properties_dict.get("responding_officer")

            assert_that(output_responding_officer, equal_to([]))

            test_pipeline.run()

    def testExtractRelationshipPropertyEntities_Optional1to1(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        is a 1-to-1 relationship to be hydrated (from the point of view of the
        root schema object), the relationship field on the root entity is
        optional, and the field is not set on the root entity ."""
        person_id = 123
        court_case_1 = database_test_utils.generate_test_court_case(person_id)

        charge_1 = database_test_utils.generate_test_charge(
            person_id,
            charge_id=345,
            court_case=court_case_1,
        )

        charge_2 = database_test_utils.generate_test_charge(
            person_id,
            charge_id=567,
            # Relationship field unset
            court_case=None,
        )

        data_dict = {
            schema.StateCourtCase.__tablename__: normalized_database_base_dict_list(
                [court_case_1]
            ),
            schema.StateCharge.__tablename__: normalized_database_base_dict_list(
                [charge_1, charge_2]
            ),
        }

        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output_properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "StateCharge"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StateCharge,
                    parent_id_field="charge_id",
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=charge_1.state_code,
                )
            )

            output_court_case = output_properties_dict.get("court_case")

            assert_that(
                output_court_case,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=charge_1.person_id,
                    inner_connection_id=charge_1.charge_id,
                    class_type=entities.StateCourtCase,
                ),
                label="Validate state_charge relationship output",
            )

            test_pipeline.run()

    def testExtractRelationshipPropertyEntities_OrphanedChild(self):
        """Tests the ExtractRelationshipPropertyEntities PTransform when there
        is a 1-to-many relationship to be hydrated, the relationship field on the root
        entity is optional, and there is an orphaned relationship entity.

        The expected result is that the orphaned entity is dropped, because it
        is not associated with any of the root entities we are hydrating.
        """
        person_id = 123
        incident_outcome_1 = (
            database_test_utils.generate_test_incarceration_incident_outcome(person_id)
        )

        incarceration_incident = (
            database_test_utils.generate_test_incarceration_incident(
                person_id, incarceration_incident_outcomes=[incident_outcome_1]
            )
        )

        incident_outcome_1.incarceration_incident_id = (
            incarceration_incident.incarceration_incident_id
        )

        incident_outcome_2 = schema.StateIncarcerationIncidentOutcome(
            incarceration_incident_outcome_id=789,
            state_code="US_XX",
            person_id=person_id,
        )

        data_dict = {
            schema.StateIncarcerationIncident.__tablename__: normalized_database_base_dict_list(
                [incarceration_incident]
            ),
            schema.StateIncarcerationIncidentOutcome.__tablename__: normalized_database_base_dict_list(
                [incident_outcome_1, incident_outcome_2]
            ),
        }

        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output_properties_dict = (
                test_pipeline | "Extract relationship properties for the "
                "StateIncarcerationIncident"
                >> extractor_utils._ExtractRelationshipPropertyEntities(
                    dataset=dataset,
                    parent_schema_class=schema.StateIncarcerationIncident,
                    parent_id_field="incarceration_incident_id",
                    unifying_id_field=entities.StatePerson.get_class_id_name(),
                    unifying_id_field_filter_set=None,
                    state_code=incarceration_incident.state_code,
                )
            )

            output_incident = output_properties_dict.get(
                "incarceration_incident_outcomes"
            )

            assert_that(
                output_incident,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=incarceration_incident.person_id,
                    inner_connection_id=incarceration_incident.incarceration_incident_id,
                    class_type=entities.StateIncarcerationIncidentOutcome,
                ),
                label="Validate incident outcome relationship output",
            )

            test_pipeline.run()


class TestExtractEntityWithAssociationTable(unittest.TestCase):
    """Tests the ExtractEntityWithAssociationTable DoFn."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

    def testExtractEntityWithAssociationTable(self):
        """Tests extracting an entity that requires an association table for
        extraction."""

        charge = database_test_utils.generate_test_charge(person_id=123, charge_id=345)
        incarceration_sentence = (
            database_test_utils.generate_test_incarceration_sentence(person_id=123)
        )

        incarceration_sentence.charges = [charge]

        # Build association table for many-to-many relationship
        charge_sentence_association_table = [
            {
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
                "charge_id": charge.charge_id,
            }
        ]

        association_table_name = (
            schema.state_charge_incarceration_sentence_association_table.name
        )

        data_dict = {
            incarceration_sentence.__tablename__: [
                normalized_database_base_dict(incarceration_sentence)
            ],
            charge.__tablename__: [normalized_database_base_dict(charge)],
            association_table_name: charge_sentence_association_table,
        }

        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):

            test_pipeline = TestPipeline()

            output = test_pipeline | "Extract association table entities" >> extractor_utils._ExtractEntityWithAssociationTable(
                dataset=dataset,
                entity_class=entities.StateCharge,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                parent_id_field=entities.StateIncarcerationSentence.get_class_id_name(),
                association_table=association_table_name,
                association_table_parent_id_field=entities.StateIncarcerationSentence.get_class_id_name(),
                association_table_entity_id_field=entities.StateCharge.get_class_id_name(),
                unifying_id_field_filter_set=None,
                state_code=charge.state_code,
            )

            assert_that(
                output,
                ExtractAssertMatchers.validate_extract_relationship_property_entities(
                    outer_connection_id=incarceration_sentence.person_id,
                    inner_connection_id=incarceration_sentence.incarceration_sentence_id,
                    class_type=entities.StateCharge,
                ),
                label="Validate StateCharge output",
            )

            test_pipeline.run()

    def testExtractEntityWithAssociationTableNoUnifyingId(self):
        """Tests extracting an entity that requires an association table for
        extraction where the associated entity has no unifying id - will return an empty PCollection.
        """
        parole_decision = database_test_utils.generate_test_parole_decision(123)

        agent = schema.StateAgent(
            agent_id=1010,
            external_id="ASSAGENT1234",
            agent_type=entities.StateAgentType.PAROLE_BOARD_MEMBER,
            state_code="US_XX",
            full_name="JOHN SMITH",
        )

        parole_decision.decision_agents = [agent]

        # Build association table for many-to-many relationship
        decision_agent_association_table = [
            {
                "parole_decision_id": parole_decision.parole_decision_id,
                "agent_id": agent.agent_id,
            }
        ]

        association_table_name = (
            schema.state_parole_decision_decision_agent_association_table.name
        )

        data_dict = {
            parole_decision.__tablename__: [
                normalized_database_base_dict(parole_decision)
            ],
            agent.__tablename__: [normalized_database_base_dict(agent)],
            association_table_name: decision_agent_association_table,
        }
        dataset = "recidiviz-123.state"
        with patch(
            "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            ),
        ):
            test_pipeline = TestPipeline()

            output = test_pipeline | "Extract association table entities" >> extractor_utils._ExtractEntityWithAssociationTable(
                dataset=dataset,
                entity_class=entities.StateAgent,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                parent_id_field=entities.StateParoleDecision.get_class_id_name(),
                association_table=association_table_name,
                association_table_parent_id_field=entities.StateParoleDecision.get_class_id_name(),
                association_table_entity_id_field=entities.StateAgent.get_class_id_name(),
                unifying_id_field_filter_set=None,
                state_code=None,
            )

            assert_that(output, equal_to([]))

            test_pipeline.run()


class TestHydrateRootEntity(unittest.TestCase):
    """Tests the HydrateRootEntity DoFn."""

    def testHydrateRootEntity(self):
        """Tests hydrating a StateSupervisionViolation entity as the root
        entity."""
        supervision_violation = database_test_utils.generate_test_supervision_violation(
            123, []
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation = remove_relationship_properties(supervision_violation)

        output_violation_entity = StateSchemaToEntityConverter().convert(
            supervision_violation
        )

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StateSupervisionViolation"
        )

        test_pipeline = TestPipeline()

        # Read entities from data_dict
        entities_raw = (
            test_pipeline
            | "Read supervision violation data from dict"
            >> FakeReadFromBigQuery(table_values=supervision_violation_data)
        )

        hydrate_kwargs = {
            "entity_class": entity_class,
            "unifying_id_field": entity_class.get_class_id_name(),
        }

        output = entities_raw | f"Hydrate {entity_class} instances" >> beam.ParDo(
            extractor_utils._HydrateRootEntity(), **hydrate_kwargs
        )

        assert_that(
            output,
            equal_to(
                [
                    (
                        supervision_violation.supervision_violation_id,
                        output_violation_entity,
                    )
                ]
            ),
        )

        test_pipeline.run()


class TestHydrateEntity(unittest.TestCase):
    """Tests the HydrateEntity DoFn."""

    def testHydrateEntity(self):
        charge = schema.StateCharge(
            charge_id=6666,
            person_id=111,
            state_code="US_XX",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case_id=222,
        )

        charge_data = [normalized_database_base_dict(charge)]

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StateCharge"
        )

        output_charge_entity = StateSchemaToEntityConverter().convert(charge)

        test_pipeline = TestPipeline()

        # Read entities from data_dict
        entities_raw = (
            test_pipeline
            | "Read charge data from dict"
            >> FakeReadFromBigQuery(table_values=charge_data)
        )

        hydrate_kwargs = {
            "entity_class": entity_class,
            "outer_connection_id_field": "person_id",
            "inner_connection_id_field": "court_case_id",
        }

        output = entities_raw | f"Hydrate {entity_class} instances" >> beam.ParDo(
            extractor_utils._HydrateEntity(), **hydrate_kwargs
        )

        assert_that(
            output,
            equal_to(
                [(charge.person_id, (charge.court_case_id, output_charge_entity))]
            ),
        )

        test_pipeline.run()

    def testHydrateEntity_EntityPrimaryId(self):
        charge = schema.StateCharge(
            charge_id=6666,
            person_id=111,
            state_code="US_XX",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case_id=222,
        )

        charge_data = [normalized_database_base_dict(charge)]

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StateCharge"
        )

        output_charge_entity = StateSchemaToEntityConverter().convert(charge)

        test_pipeline = TestPipeline()

        # Read entities from data_dict
        entities_raw = (
            test_pipeline
            | "Read charge data from dict"
            >> FakeReadFromBigQuery(table_values=charge_data)
        )

        hydrate_kwargs = {
            "entity_class": entity_class,
            "outer_connection_id_field": entity_class.get_class_id_name(),
            "inner_connection_id_field": "court_case_id",
        }

        output = entities_raw | f"Hydrate {entity_class} instances" >> beam.ParDo(
            extractor_utils._HydrateEntity(), **hydrate_kwargs
        )

        assert_that(
            output,
            equal_to(
                [
                    (
                        output_charge_entity.get_id(),
                        (charge.court_case_id, output_charge_entity),
                    )
                ]
            ),
        )

        test_pipeline.run()

    def testHydrateEntity_InvalidId(self):
        charge = schema.StateCharge(
            charge_id=6666,
            person_id=111,
            state_code="US_XX",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case_id=222,
        )

        charge_data = [normalized_database_base_dict(charge)]

        entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities, "StateCharge"
        )

        with patch("logging.Logger.warning") as mock:
            test_pipeline = TestPipeline()

            # Read entities from data_dict
            entities_raw = (
                test_pipeline
                | "Read charge data from dict"
                >> FakeReadFromBigQuery(table_values=charge_data)
            )

            hydrate_kwargs = {
                "entity_class": entity_class,
                "outer_connection_id_field": "XX",
                "inner_connection_id_field": "court_case_id",
            }

            _ = entities_raw | f"Hydrate {entity_class} instances" >> beam.ParDo(
                extractor_utils._HydrateEntity(), **hydrate_kwargs
            )

            test_pipeline.run()

            mock.assert_called_with(
                "Invalid outer_connection_id_field: %s." "Dropping this entity.", "XX"
            )


class TestHydrateRootEntityWithRelationshipPropertyEntities(unittest.TestCase):
    """Tests the HydrateRootEntitiesWithRelationshipPropertyEntities DoFn."""

    def testHydrateRelationshipsOnEntities(self):
        # incarceration sentence is the root entity
        incarceration_sentence = remove_relationship_properties(
            database_test_utils.generate_test_incarceration_sentence(123)
        )
        sentence_group = remove_relationship_properties(
            database_test_utils.generate_test_sentence_group(123, [], [])
        )
        charge1 = remove_relationship_properties(
            database_test_utils.generate_test_charge(123, 6666, None)
        )
        charge2 = remove_relationship_properties(
            database_test_utils.generate_test_charge(123, 7777, None)
        )

        incarceration_sentence_entity = StateSchemaToEntityConverter().convert(
            incarceration_sentence
        )
        sentence_group_entity = StateSchemaToEntityConverter().convert(sentence_group)
        charge1_entity = StateSchemaToEntityConverter().convert(charge1)
        charge2_entity = StateSchemaToEntityConverter().convert(charge2)

        element = [
            (
                incarceration_sentence.person_id,
                {
                    incarceration_sentence.__tablename__: [
                        incarceration_sentence_entity
                    ],
                    "sentence_group": [
                        (
                            incarceration_sentence.incarceration_sentence_id,
                            sentence_group_entity,
                        )
                    ],
                    "charges": [
                        (
                            incarceration_sentence.incarceration_sentence_id,
                            charge1_entity,
                        ),
                        (
                            incarceration_sentence.incarceration_sentence_id,
                            charge2_entity,
                        ),
                    ],
                },
            )
        ]

        output_incarceration_sentence = (
            entities.StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=2222,
                status=entities.StateSentenceStatus.SUSPENDED,
                state_code="US_XX",
                is_capital_punishment=False,
            )
        )

        output_incarceration_sentence.sentence_group = sentence_group_entity
        output_incarceration_sentence.charges = [charge1_entity, charge2_entity]

        schema_class = schema.StateIncarcerationSentence

        hydrate_kwargs = {"schema_class": schema_class}

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | "Convert to PCollection" >> beam.Create(element)
            | "Hydrate incarceration sentence with relationship property entities"
            >> beam.ParDo(
                extractor_utils._HydrateRootEntitiesWithRelationshipPropertyEntities(),
                **hydrate_kwargs,
            )
        )

        assert_that(
            output,
            equal_to(
                [(incarceration_sentence.person_id, output_incarceration_sentence)]
            ),
        )

        test_pipeline.run()

    def testHydrateRelationshipsOnEntities_MultipleRoots(self):
        person_id = 143

        # incarceration_sentence is the root entity
        incarceration_sentence_1 = remove_relationship_properties(
            database_test_utils.generate_test_incarceration_sentence(person_id)
        )
        sentence_group_1 = remove_relationship_properties(
            database_test_utils.generate_test_sentence_group(person_id, [], [])
        )
        charge_1_1 = remove_relationship_properties(
            database_test_utils.generate_test_charge(person_id, 6666, None)
        )
        charge_1_2 = remove_relationship_properties(
            database_test_utils.generate_test_charge(person_id, 7777, None)
        )

        incarceration_sentence_2 = schema.StateIncarcerationSentence(
            incarceration_sentence_id=9999,
            status=entities.StateSentenceStatus.COMPLETED,
            state_code="US_XX",
            person_id=person_id,
        )

        sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=7895,
            status=StateSentenceStatus.SUSPENDED.value,
            state_code="US_XX",
            person_id=person_id,
        )

        charge_2_1 = schema.StateCharge(
            charge_id=1209,
            person_id=person_id,
            status=entities.ChargeStatus.PENDING,
            state_code="US_XX",
        )

        incarceration_sentence_entity_1 = StateSchemaToEntityConverter().convert(
            incarceration_sentence_1
        )
        sentence_group_entity_1 = StateSchemaToEntityConverter().convert(
            sentence_group_1
        )
        charge_1_1_entity = StateSchemaToEntityConverter().convert(charge_1_1)
        charge_1_2_entity = StateSchemaToEntityConverter().convert(charge_1_2)

        incarceration_sentence_entity_2 = StateSchemaToEntityConverter().convert(
            incarceration_sentence_2
        )
        sentence_group_entity_2 = StateSchemaToEntityConverter().convert(
            sentence_group_2
        )
        charge_2_1_entity = StateSchemaToEntityConverter().convert(charge_2_1)

        element = [
            (
                person_id,
                {
                    incarceration_sentence_1.__tablename__: [
                        incarceration_sentence_entity_1
                    ],
                    "sentence_group": [
                        (
                            incarceration_sentence_1.incarceration_sentence_id,
                            sentence_group_entity_1,
                        )
                    ],
                    "charges": [
                        (
                            incarceration_sentence_1.incarceration_sentence_id,
                            charge_1_1_entity,
                        ),
                        (
                            incarceration_sentence_1.incarceration_sentence_id,
                            charge_1_2_entity,
                        ),
                    ],
                },
            ),
            (
                person_id,
                {
                    incarceration_sentence_2.__tablename__: [
                        incarceration_sentence_entity_2
                    ],
                    "sentence_group": [
                        (
                            incarceration_sentence_2.incarceration_sentence_id,
                            sentence_group_entity_2,
                        )
                    ],
                    "charges": [
                        (
                            incarceration_sentence_2.incarceration_sentence_id,
                            charge_2_1_entity,
                        )
                    ],
                },
            ),
        ]

        output_incarceration_sentence_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=2222,
                status=entities.StateSentenceStatus.SUSPENDED,
                state_code="US_XX",
                is_capital_punishment=False,
            )
        )

        output_incarceration_sentence_1.sentence_group = sentence_group_entity_1
        output_incarceration_sentence_1.charges = [charge_1_1_entity, charge_1_2_entity]

        output_incarceration_sentence_2 = entities.StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_2.incarceration_sentence_id,
            status=entities.StateSentenceStatus.COMPLETED,
            state_code="US_XX",
        )

        output_incarceration_sentence_2.sentence_group = sentence_group_entity_2
        output_incarceration_sentence_2.charges = [charge_2_1_entity]

        schema_class = schema.StateIncarcerationSentence

        hydrate_kwargs = {"schema_class": schema_class}

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | "Convert to PCollection" >> beam.Create(element)
            | "Hydrate incarceration_sentence with relationship property entities"
            >> beam.ParDo(
                extractor_utils._HydrateRootEntitiesWithRelationshipPropertyEntities(),
                **hydrate_kwargs,
            )
        )

        assert_that(
            output,
            equal_to(
                [
                    (
                        incarceration_sentence_1.person_id,
                        output_incarceration_sentence_1,
                    ),
                    (
                        incarceration_sentence_2.person_id,
                        output_incarceration_sentence_2,
                    ),
                ]
            ),
        )

        test_pipeline.run()


class TestRepackageUnifyingIdParentIdStructure(unittest.TestCase):
    """Tests the RepackageUnifyingIdParentIdStructure DoFn."""

    def testRepackageUnifyingIdParentIdStructure(self):
        incarceration_sentence = entities.StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=444,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        element = [
            (
                1234,
                {
                    "child_entity_with_unifying_id": [(1234, incarceration_sentence)],
                    "parent_entity_ids": [111, 222],
                },
            )
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | "Convert to PCollection" >> beam.Create(element)
            | "Repackage structure"
            >> beam.ParDo(extractor_utils._RepackageUnifyingIParentIdStructure())
        )

        assert_that(
            output,
            equal_to(
                [
                    (1234, (111, incarceration_sentence)),
                    (1234, (222, incarceration_sentence)),
                ]
            ),
        )

        test_pipeline.run()

    def testRepackageUnifyingIdParentIdStructure_NoRootIds(self):
        incarceration_sentence = entities.StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=444,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        element = [
            (
                999,
                {
                    "child_entity_with_unifying_id": [(1234, incarceration_sentence)],
                    "parent_entity_ids": [],
                },
            )
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | "Convert to PCollection" >> beam.Create(element)
            | "Repackage structure"
            >> beam.ParDo(extractor_utils._RepackageUnifyingIParentIdStructure())
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class ExtractAssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_build_root_entities(
        unifying_id: int, class_type: Type[entities.Entity]
    ):
        """Validates that the output of ExtractRelationshipPropertyEntities
        matches the expected format:

            (unifying_id, Entity)

        where the Entity is of the given |class_type|.
        """

        def _validate_build_root_entities(output):
            for item in output:
                id_value, entity = item

                assert id_value == unifying_id

                assert issubclass(entity.__class__, class_type)

        return _validate_build_root_entities

    @staticmethod
    def validate_extract_relationship_property_entities(
        outer_connection_id: int,
        inner_connection_id: int,
        class_type: Type[entities.Entity],
    ):
        """Validates that the output of ExtractRelationshipPropertyEntities
        matches the expected format:

            (primary_id, (secondary_id, Entity))

        where the Entity is of the given |class_type|.
        """

        def _validate_extract_relationship_property_entities(output):
            empty = True
            for item in output:
                print("VALIDATING")
                print(item)
                empty = False
                first_id, id_entity = item
                assert first_id == outer_connection_id

                second_id, entity = id_entity
                print(
                    f"second_id={second_id}, inner_connection_id={inner_connection_id}"
                )
                assert second_id == inner_connection_id

                assert issubclass(entity.__class__, class_type)
            assert not empty

        return _validate_extract_relationship_property_entities
