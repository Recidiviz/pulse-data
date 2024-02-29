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
"""This class implements tests for the OutliersQuerier class"""
import csv
import json
import os
from datetime import date, datetime
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy.exc import IntegrityError, NoResultFound

from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS_AND_INFERRED,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    VIOLATIONS,
)
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import (
    ConfigurationStatus,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
    UserInfo,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    Configuration,
    MetricBenchmark,
    OutliersBase,
    SupervisionClientEvent,
    SupervisionClients,
    SupervisionDistrict,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerOutlierStatus,
    SupervisionOfficerSupervisor,
    UserMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.outliers import fixtures
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


def load_model_fixture(
    model: OutliersBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            for k, v in row.items():
                if k == "full_name":
                    row["full_name"] = json.loads(row["full_name"])
                if k == "client_name":
                    row["client_name"] = json.loads(row["client_name"])
                if k == "attributes" and v != "":
                    row["attributes"] = json.loads(row["attributes"])
                if k == "has_seen_onboarding" and v != "":
                    row["has_seen_onboarding"] = json.loads(row["has_seen_onboarding"])
                if v == "":
                    row[k] = None
            results.append(row)

    return results


TEST_END_DATE = date(year=2023, month=5, day=1)
TEST_PREV_END_DATE = date(year=2023, month=4, day=1)

TEST_METRIC_1 = OutliersMetricConfig.build_from_metric(
    metric=INCARCERATION_STARTS_AND_INFERRED,
    title_display_name="Incarceration Rate (CPVs & TPVs)",
    body_display_name="incarceration rate",
    event_name="incarcerations",
    event_name_singular="incarceration",
    event_name_past_tense="were incarcerated",
    description_markdown="""Incarceration rate description

<br />
Incarceration rate denominator description""",
)

TEST_METRIC_2 = OutliersMetricConfig.build_from_metric(
    metric=TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    title_display_name="Limited Supervision Unit Transfer Rate",
    body_display_name="Limited Supervision Unit transfer rate(s)",
    event_name="LSU transfers",
    event_name_singular="LSU transfer",
    event_name_past_tense="were transferred to LSU",
)

TEST_METRIC_3 = OutliersMetricConfig.build_from_metric(
    metric=ABSCONSIONS_BENCH_WARRANTS,
    title_display_name="Absconsion Rate",
    body_display_name="absconsion rate",
    event_name="absconsions",
    event_name_singular="absconsion",
    event_name_past_tense="absconded",
)

TEST_CLIENT_EVENT_1 = OutliersClientEventConfig.build(
    event=VIOLATIONS, display_name="violations"
)


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class TestOutliersQuerier(TestCase):
    """Implements tests for the OutliersQuerier."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, db_name="us_pa")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            session.execute("""ALTER SEQUENCE configurations_id_seq RESTART WITH 1;""")

            for officer in load_model_fixture(SupervisionOfficer):
                session.add(SupervisionOfficer(**officer))
            for status in load_model_fixture(SupervisionOfficerOutlierStatus):
                session.add(SupervisionOfficerOutlierStatus(**status))
            for supervisor in load_model_fixture(SupervisionOfficerSupervisor):
                session.add(SupervisionOfficerSupervisor(**supervisor))
            for district in load_model_fixture(SupervisionDistrict):
                session.add(SupervisionDistrict(**district))
            for manager in load_model_fixture(SupervisionDistrictManager):
                session.add(SupervisionDistrictManager(**manager))
            for benchmark in load_model_fixture(MetricBenchmark):
                session.add(MetricBenchmark(**benchmark))
            for event in load_model_fixture(SupervisionClientEvent):
                session.add(SupervisionClientEvent(**event))
            for client in load_model_fixture(SupervisionClients):
                session.add(SupervisionClients(**client))
            for metadata in load_model_fixture(UserMetadata):
                session.add(UserMetadata(**metadata))
            for config in load_model_fixture(Configuration):
                session.add(Configuration(**config))

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    @patch("recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_config")
    def test_get_officer_level_report_data_by_supervisor(
        self, mock_config: MagicMock
    ) -> None:
        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_1, TEST_METRIC_2],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
        )

        actual = OutliersQuerier(
            StateCode.US_PA
        ).get_officer_level_report_data_for_all_officer_supervisors(
            end_date=TEST_END_DATE
        )

        self.snapshot.assert_match(actual, name="test_get_officer_level_report_data_by_supervisor")  # type: ignore[attr-defined]

        actual_json = {
            supervisor_id: supervisor_data.to_dict()
            for supervisor_id, supervisor_data in actual.items()
        }
        self.snapshot.assert_match(actual_json, name="test_get_officer_level_report_data_by_supervisor_json")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entities(
        self,
    ) -> None:
        actual = OutliersQuerier(
            StateCode.US_PA
        ).get_supervision_officer_supervisor_entities()

        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entities")  # type: ignore[attr-defined]

    def test_get_officers_for_supervisor(self) -> None:
        actual = OutliersQuerier(StateCode.US_PA).get_officers_for_supervisor(
            supervisor_external_id="102", num_lookback_periods=5
        )
        self.snapshot.assert_match(actual, name="test_get_officers_for_supervisor")  # type: ignore[attr-defined]

    def test_get_supervisor_from_pseudonymized_id_no_match(self) -> None:
        # If matching supervisor doesn't exist, return None
        actual = OutliersQuerier(
            StateCode.US_PA
        ).get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id="invalidhash"
        )
        self.assertIsNone(actual)

    def test_get_supervisor_from_pseudonymized_id_found_match(self) -> None:
        # Return matching supervisor
        with SessionFactory.using_database(self.database_key) as session:
            expected = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            actual = OutliersQuerier(
                StateCode.US_PA
            ).get_supervisor_entity_from_pseudonymized_id(
                supervisor_pseudonymized_id="hash1"
            )

            self.assertEqual(expected.external_id, actual.external_id)  # type: ignore[union-attr]

    def test_get_benchmarks(self) -> None:
        actual = OutliersQuerier(StateCode.US_PA).get_benchmarks(4)
        self.snapshot.assert_match(actual, name="test_get_benchmarks")  # type: ignore[attr-defined]

    def test_get_events_by_officer(self) -> None:
        # Return matching event
        with SessionFactory.using_database(self.database_key) as session:
            metric_id = TEST_METRIC_3.name
            expected = (
                session.query(SupervisionClientEvent)
                .filter(SupervisionClientEvent.event_date == "2023-04-01")
                .first()
            )

            actual = OutliersQuerier(StateCode.US_PA).get_events_by_officer(
                "officerhash3", [metric_id]
            )
            self.assertEqual(len(actual), 1)
            self.assertEqual(expected.event_date, actual[0].event_date)
            self.assertEqual(expected.client_id, actual[0].client_id)
            self.assertEqual(metric_id, actual[0].metric_id)

    def test_get_events_by_officer_none_found(self) -> None:
        actual = OutliersQuerier(StateCode.US_PA).get_events_by_officer(
            "officerhash1", ["absconsions_bench_warrants"]
        )
        self.assertEqual(actual, [])

    def test_get_supervision_officer_entity_found_match(self) -> None:
        # Return matching supervision officer entity
        actual = OutliersQuerier(StateCode.US_PA).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3", num_lookback_periods=0
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_found_match")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_no_match(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(StateCode.US_PA).get_supervision_officer_entity(
            pseudonymized_officer_id="invalidhash", num_lookback_periods=0
        )

        self.assertIsNone(actual)

    def test_get_supervisor_from_external_id_no_match(self) -> None:
        # If matching supervisor doesn't exist, return None
        actual = OutliersQuerier(StateCode.US_PA).get_supervisor_from_external_id(
            external_id="invalidid"
        )
        self.assertIsNone(actual)

    def test_get_supervision_officer_entity_no_metrics(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(StateCode.US_PA).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash9",
            num_lookback_periods=0,
            period_end_date=TEST_PREV_END_DATE,
        )

        self.assertIsNone(actual)

    def test_get_supervisor_from_external_id_found_match(self) -> None:
        # Return matching supervisor
        actual = OutliersQuerier(StateCode.US_PA).get_supervisor_from_external_id(
            external_id="101"
        )

        self.assertEqual("101", actual.external_id)  # type: ignore[union-attr]

    def test_get_events_by_client(self) -> None:
        # Return matching event
        with SessionFactory.using_database(self.database_key) as session:
            metric_id = TEST_METRIC_3.name
            expected = (
                session.query(SupervisionClientEvent)
                .filter(SupervisionClientEvent.event_date == "2023-05-01")
                .first()
            )

            actual = OutliersQuerier(StateCode.US_PA).get_events_by_client(
                "clienthash1", [metric_id], TEST_END_DATE
            )
            self.assertEqual(len(actual), 1)
            self.assertEqual(expected.event_date, actual[0].event_date)
            self.assertEqual(expected.client_id, actual[0].client_id)
            self.assertEqual(metric_id, actual[0].metric_id)

    def test_get_events_by_client_none_found(self) -> None:
        actual = OutliersQuerier(StateCode.US_PA).get_events_by_client(
            "randomhash", ["absconsions_bench_warrants"], TEST_END_DATE
        )
        self.assertEqual(actual, [])

    def test_get_supervision_client_no_match(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(StateCode.US_PA).get_client_from_pseudonymized_id(
            pseudonymized_id="randomhash",
        )

        self.assertIsNone(actual)

    def test_get_supervision_client_success(self) -> None:
        # Return matching supervisor
        actual = OutliersQuerier(StateCode.US_PA).get_client_from_pseudonymized_id(
            pseudonymized_id="clienthash1"
        )

        self.assertEqual("111", actual.client_id)  # type: ignore[union-attr]

    def test_get_user_metadata_empty(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)

        self.assertIsNone(querier.get_user_metadata(pseudonymized_id="hash3"))

    def test_get_user_metadata_defaults(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(StateCode.US_PA)
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore

    def test_get_user_metadata_success(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(StateCode.US_PA)
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertTrue(metadata.has_seen_onboarding)  # type: ignore

    def test_update_user_metadata_new_entity(self) -> None:
        pid = "hash3"
        querier = OutliersQuerier(StateCode.US_PA)
        querier.update_user_metadata(pid, True)
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertTrue(metadata.has_seen_onboarding)  # type: ignore

    def test_update_user_metadata_existing_entity(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(StateCode.US_PA)
        querier.update_user_metadata(pid, False)
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore

    def test_get_user_info_success(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(StateCode.US_PA)
        result = querier.get_user_info(pid)

        self.assertIsNotNone(result.entity)
        self.assertEqual(result.entity.external_id, "101")  # type: ignore
        self.assertEqual(result.role, "supervision_officer_supervisor")
        self.assertTrue(result.has_seen_onboarding)

    def test_get_user_info_default_onboarding(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(StateCode.US_PA)
        result = querier.get_user_info(pid)

        self.assertIsNotNone(result.entity)
        self.assertEqual(result.entity.external_id, "102")  # type: ignore
        self.assertEqual(result.role, "supervision_officer_supervisor")
        self.assertFalse(result.has_seen_onboarding)

    def test_get_user_info_non_supervisor(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        result = querier.get_user_info("leadership-hash")

        self.assertEqual(
            result, UserInfo(entity=None, role=None, has_seen_onboarding=True)
        )

    def test_get_configuration_for_user_no_fv(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        user_context = None
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 1)

    def test_get_configuration_for_user_fv(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        user_context = UserContext(
            state_code_str="US_PA",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            feature_variants={"fv1": {}, "random": {}},
        )
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 3)

    def test_get_configuration_for_user_no_fv_match(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        user_context = UserContext(
            state_code_str="US_PA",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            feature_variants={"randomfv": {}},
        )
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 1)

    def test_get_configuration_for_user_multiple_fv(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        user_context = UserContext(
            state_code_str="US_PA",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            feature_variants={"fv1": {}, "fv2": {}},
        )

        with patch("logging.Logger.error") as mock_logger:
            result = querier.get_configuration_for_user(user_context)

            self.assertEqual(result.id, 1)
            mock_logger.assert_has_calls(
                [
                    call(
                        "Multiple configurations and feature variants may apply to this user, however only one should apply. The default configuration will be shown instead."
                    )
                ]
            )

    def test_get_configurations(self) -> None:
        querier = OutliersQuerier(StateCode.US_PA)
        result = querier.get_configurations()

        self.assertListEqual([config.id for config in result], [1, 4, 3, 2])

    def test_add_configuration_no_fv(self) -> None:
        new_datetime = datetime(2024, 2, 1)
        config_to_add = {
            "updated_at": new_datetime,
            "feature_variant": None,
            "updated_by": "alexa@recidiviz.org",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
            "none_are_outliers_label": "are outliers",
            "worse_than_rate_label": "Far worse than statewide rate",
            "slightly_worse_than_rate_label": "slightly worse than statewide rate",
            "at_or_below_rate_label": "At or below statewide rate",
            "exclusion_reason_description": None,
        }

        querier = OutliersQuerier(StateCode.US_PA)
        querier.add_configuration(config_to_add)

        results = querier.get_configurations()
        self.assertEqual(results[0].feature_variant, None)
        self.assertEqual(results[0].updated_by, "alexa@recidiviz.org")
        # Assumes that the first non-header row in configurations.csv has feature_variant=None
        self.assertEqual(
            querier.get_configuration(1).status, ConfigurationStatus.INACTIVE.value
        )

    def test_add_configuration_fv(self) -> None:
        config_to_add = {
            "updated_at": datetime(2024, 2, 1),
            "feature_variant": "fv1",
            "updated_by": "alexa@recidiviz.org",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
            "none_are_outliers_label": "are outliers",
            "worse_than_rate_label": "Far worse than statewide rate",
            "slightly_worse_than_rate_label": "slightly worse than statewide rate",
            "at_or_below_rate_label": "At or below statewide rate",
            "exclusion_reason_description": None,
        }

        querier = OutliersQuerier(StateCode.US_PA)
        querier.add_configuration(config_to_add)

        results = querier.get_configurations()
        self.assertEqual(results[0].feature_variant, "fv1")
        self.assertEqual(results[0].updated_by, "alexa@recidiviz.org")
        # Assumes that the third non-header row in configurations.csv has feature_variant=fv1
        self.assertEqual(
            querier.get_configuration(3).status, ConfigurationStatus.INACTIVE.value
        )

    def test_add_configuration_error(self) -> None:
        config_to_add = {
            "updated_at": datetime(2024, 2, 1),
            "feature_variant": "fv1",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
        }

        with self.assertRaises(IntegrityError):
            querier = OutliersQuerier(StateCode.US_PA)
            querier.add_configuration(config_to_add)
            # Assert the latest entity is the first non-header row in configurations.csv
            self.assertEqual(querier.get_configurations()[0].id, 1)

    def test_deactivate_configuration_no_matching_id(self) -> None:
        with self.assertRaises(NoResultFound):
            querier = OutliersQuerier(StateCode.US_PA)
            querier.deactivate_configuration(1001)

    def test_deactivate_configuration_inactive(self) -> None:
        with patch("logging.Logger.error") as mock_logger:
            querier = OutliersQuerier(StateCode.US_PA)
            # Assumes that the second non-header row in configurations.csv has status=INACTIVE
            querier.deactivate_configuration(2)
            mock_logger.assert_has_calls(
                [call("Configuration %s is already inactive", 2)]
            )

    def test_deactivate_configuration_default_config(self) -> None:
        with self.assertRaises(ValueError):
            querier = OutliersQuerier(StateCode.US_PA)
            # Assumes that the first non-header row in configurations.csv is the only
            # row with feature_variant=None and status=ACTIVE
            querier.deactivate_configuration(1)

    def test_deactivate_configuration_success(self) -> None:
        # Assumes that the third non-header row in configurations.csv has
        # feature_variant != None and status=ACTIVE
        config_id_to_deactivate = 3
        querier = OutliersQuerier(StateCode.US_PA)
        querier.deactivate_configuration(config_id_to_deactivate)

        self.assertEqual(
            querier.get_configuration(config_id_to_deactivate).status,
            ConfigurationStatus.INACTIVE.value,
        )
