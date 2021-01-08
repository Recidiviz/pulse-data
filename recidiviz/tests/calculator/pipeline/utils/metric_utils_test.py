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
"""Tests the functions in the metric_utils file."""
import unittest
import pytest
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationAdmissionMetric, IncarcerationMetricType, \
    IncarcerationPopulationMetric, IncarcerationReleaseMetric
from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType, ProgramReferralMetric, \
    ProgramParticipationMetric
from recidiviz.calculator.pipeline.recidivism.metrics import ReincarcerationRecidivismMetricType, \
    ReincarcerationRecidivismCountMetric, ReincarcerationRecidivismRateMetric
from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType, SupervisionPopulationMetric, \
    SupervisionTerminationMetric, SupervisionRevocationMetric, \
    SupervisionRevocationAnalysisMetric, SupervisionSuccessMetric, \
    SuccessfulSupervisionSentenceDaysServedMetric, SupervisionCaseComplianceMetric, SupervisionDowngradeMetric, \
    SupervisionStartMetric, SupervisionOutOfStatePopulationMetric
from recidiviz.calculator.pipeline.utils.metric_utils import MetricMethodologyType, json_serializable_metric_key, \
    RecidivizMetric
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity


class TestJsonSerializableMetricKey(unittest.TestCase):
    """Tests the json_serializable_metric_key function."""
    def test_json_serializable_metric_key(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK, Race.WHITE],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK,WHITE',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_OneRace(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_RaceEthnicity(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK],
                      'ethnicity': [Ethnicity.HISPANIC, Ethnicity.EXTERNAL_UNKNOWN],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK',
                           'ethnicity': 'EXTERNAL_UNKNOWN,HISPANIC',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_RaceEthnicityNone(self):
        # This should never happen due to the way this dictionary is constructed.
        metric_key = {'gender': Gender.MALE,
                      'race': [None],
                      'ethnicity': [None],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_ViolationTypeFrequencyCounter(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK, Race.WHITE],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA',
                      'violation_type_frequency_counter': [['TECHNICAL'], ['ASC', 'EMP', 'TECHNICAL']]
                      }

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK,WHITE',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA',
                           'violation_type_frequency_counter': '[ASC, EMP, TECHNICAL],[TECHNICAL]'
                           }

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_InvalidList(self):
        metric_key = {'invalid_list_key': ['list', 'values']}

        with pytest.raises(ValueError) as e:
            json_serializable_metric_key(metric_key)

            self.assertEqual(e, "Unexpected list in metric_key for key: invalid_list_key")


class TestBQSchemaForMetricTable(unittest.TestCase):
    """Tests the bq_schema_for_metric_table function."""
    def test_bq_schema_for_metric_table(self):
        schema_fields = RecidivizMetric.bq_schema_for_metric_table()

        expected_output = [
            SchemaField('metric_type', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('job_id', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('state_code', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('methodology', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('age_bucket', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('race', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('ethnicity', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('prioritized_race_or_ethnicity', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('gender', bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField('created_on', bigquery.enums.SqlTypeNames.DATE.value),
            SchemaField('updated_on', bigquery.enums.SqlTypeNames.DATE.value),
        ]

        self.assertEqual(expected_output, schema_fields)

    def test_bq_schema_for_metric_table_incarceration(self):
        incarceration_metrics_for_type = {
            IncarcerationMetricType.INCARCERATION_ADMISSION: IncarcerationAdmissionMetric,
            IncarcerationMetricType.INCARCERATION_POPULATION: IncarcerationPopulationMetric,
            IncarcerationMetricType.INCARCERATION_RELEASE: IncarcerationReleaseMetric
        }

        for metric_type in IncarcerationMetricType:
            # Assert that all IncarcerationMetricTypes are covered
            assert metric_type in incarceration_metrics_for_type.keys()

            # If there's no error, then all attribute types are handled
            _ = incarceration_metrics_for_type.get(metric_type).bq_schema_for_metric_table()

    def test_bq_schema_for_metric_table_program(self):
        program_metrics_for_type = {
            ProgramMetricType.PROGRAM_REFERRAL: ProgramReferralMetric,
            ProgramMetricType.PROGRAM_PARTICIPATION: ProgramParticipationMetric
        }

        for metric_type in ProgramMetricType:
            # Assert that all ProgramMetricTypes are covered
            assert metric_type in program_metrics_for_type.keys()

            # If there's no error, then all attribute types are handled
            _ = program_metrics_for_type.get(metric_type).bq_schema_for_metric_table()

    def test_bq_schema_for_metric_table_recidivism(self):
        recidivism_metrics_for_type = {
            ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT: ReincarcerationRecidivismCountMetric,
            ReincarcerationRecidivismMetricType.REINCARCERATION_RATE: ReincarcerationRecidivismRateMetric,
        }

        for metric_type in ReincarcerationRecidivismMetricType:
            # Assert that all ReincarcerationRecidivismMetricTypes are covered
            assert metric_type in recidivism_metrics_for_type.keys()

            # If there's no error, then all attribute types are handled
            _ = recidivism_metrics_for_type.get(metric_type).bq_schema_for_metric_table()

    def test_bq_schema_for_metric_table_supervision(self):
        supervision_metrics_for_type = {
            SupervisionMetricType.SUPERVISION_TERMINATION: SupervisionTerminationMetric,
            SupervisionMetricType.SUPERVISION_COMPLIANCE: SupervisionCaseComplianceMetric,
            SupervisionMetricType.SUPERVISION_POPULATION: SupervisionPopulationMetric,
            SupervisionMetricType.SUPERVISION_REVOCATION: SupervisionRevocationMetric,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: SupervisionRevocationAnalysisMetric,
            SupervisionMetricType.SUPERVISION_START: SupervisionStartMetric,
            SupervisionMetricType.SUPERVISION_SUCCESS: SupervisionSuccessMetric,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
                SuccessfulSupervisionSentenceDaysServedMetric,
            SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION: SupervisionOutOfStatePopulationMetric,
            SupervisionMetricType.SUPERVISION_DOWNGRADE: SupervisionDowngradeMetric
        }

        for metric_type in SupervisionMetricType:
            # Assert that all SupervisionMetricTypes are covered
            assert metric_type in supervision_metrics_for_type.keys()

            # If there's no error, then all attribute types are handled
            _ = supervision_metrics_for_type.get(metric_type).bq_schema_for_metric_table()


class TestRecidivizMetricType(unittest.TestCase):
    """Tests required characteristics of various RecidivizMetricTypes."""
    def test_unique_metric_type_values(self):
        all_metric_type_values = [
            metric_class.build_from_dictionary({
                'job_id': 'xxx',
                'state_code': 'US_XX'
            }).metric_type.value for metric_class in DATAFLOW_METRICS_TO_TABLES
        ]

        # Assert that all metric type values are unique
        self.assertEqual(len(set(all_metric_type_values)), len(all_metric_type_values))
