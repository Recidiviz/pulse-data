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
from datetime import date
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import pytest

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
    OfficerMetricEntity,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerSupervisorEntity,
    TargetStatus,
    TargetStatusStrategy,
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

        expected = {
            "103": OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[TEST_METRIC_1, TEST_METRIC_2],
                recipient_email_address="manager3@recidiviz.org",
                additional_recipients=["manager2@recidiviz.org"],
            ),
            "102": OfficerSupervisorReportData(
                metrics=[
                    OutlierMetricInfo(
                        metric=TEST_METRIC_2,
                        target=0.008,
                        other_officers={
                            TargetStatus.FAR: [],
                            TargetStatus.MET: [
                                0.27,
                                0.11,
                                0.039,
                                0.184,
                                0.126,
                                0.171,
                                0.333,
                            ],
                            TargetStatus.NEAR: [],
                        },
                        highlighted_officers=[
                            OfficerMetricEntity(
                                name=PersonName(
                                    given_names="Officer",
                                    surname="4",
                                    middle_names=None,
                                    name_suffix=None,
                                ),
                                rate=0.0,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.0,
                                supervisor_external_id="102",
                                supervision_district="2",
                                prev_target_status=None,
                            )
                        ],
                        target_status_strategy=TargetStatusStrategy.ZERO_RATE,
                    )
                ],
                metrics_without_outliers=[TEST_METRIC_1],
                recipient_email_address="supervisor2@recidiviz.org",
                additional_recipients=[
                    "manager2@recidiviz.org",
                    "manager3@recidiviz.org",
                ],
            ),
            "101": OfficerSupervisorReportData(
                metrics=[
                    OutlierMetricInfo(
                        metric=TEST_METRIC_1,
                        target=0.13,
                        other_officers={
                            TargetStatus.FAR: [],
                            TargetStatus.MET: [0.11, 0.04, 0.0, 0.12],
                            TargetStatus.NEAR: [0.184, 0.17],
                        },
                        highlighted_officers=[
                            OfficerMetricEntity(
                                name=PersonName(
                                    given_names="Officer",
                                    surname="1",
                                    middle_names=None,
                                    name_suffix=None,
                                ),
                                rate=0.26,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.32,
                                supervisor_external_id="101",
                                supervision_district="1",
                                prev_target_status=TargetStatus.NEAR,
                            ),
                            OfficerMetricEntity(
                                name=PersonName(
                                    given_names="Officer",
                                    surname="8",
                                    middle_names=None,
                                    name_suffix=None,
                                ),
                                rate=0.333,
                                target_status=TargetStatus.FAR,
                                prev_rate=None,
                                supervisor_external_id="101",
                                supervision_district="1",
                                prev_target_status=None,
                            ),
                        ],
                        target_status_strategy=TargetStatusStrategy.IQR_THRESHOLD,
                    )
                ],
                metrics_without_outliers=[TEST_METRIC_2],
                recipient_email_address="supervisor1@recidiviz.org",
                additional_recipients=[],
            ),
        }
        expected_json = {
            "103": {
                "metrics": [],
                "metrics_without_outliers": [
                    {
                        "name": "incarceration_starts_and_inferred",
                        "outcome_type": "ADVERSE",
                        "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                        "body_display_name": "incarceration rate",
                        "event_name": "incarcerations",
                        "event_name_singular": "incarceration",
                        "event_name_past_tense": "were incarcerated",
                        "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                    },
                    {
                        "name": "task_completions_transfer_to_limited_supervision",
                        "outcome_type": "FAVORABLE",
                        "title_display_name": "Limited Supervision Unit Transfer Rate",
                        "body_display_name": "Limited Supervision Unit transfer rate(s)",
                        "event_name": "LSU transfers",
                        "event_name_singular": "LSU transfer",
                        "event_name_past_tense": "were transferred to LSU",
                        "description_markdown": None,
                    },
                ],
                "recipient_email_address": "manager3@recidiviz.org",
                "additional_recipients": ["manager2@recidiviz.org"],
            },
            "102": {
                "metrics": [
                    {
                        "metric": {
                            "name": "task_completions_transfer_to_limited_supervision",
                            "outcome_type": "FAVORABLE",
                            "title_display_name": "Limited Supervision Unit Transfer Rate",
                            "body_display_name": "Limited Supervision Unit transfer rate(s)",
                            "event_name": "LSU transfers",
                            "event_name_singular": "LSU transfer",
                            "event_name_past_tense": "were transferred to LSU",
                            "description_markdown": None,
                        },
                        "target": 0.008,
                        "other_officers": {
                            "FAR": [],
                            "MET": [0.27, 0.11, 0.039, 0.184, 0.126, 0.171, 0.333],
                            "NEAR": [],
                        },
                        "highlighted_officers": [
                            {
                                "name": {
                                    "given_names": "Officer",
                                    "surname": "4",
                                    "middle_names": None,
                                    "name_suffix": None,
                                },
                                "rate": 0.0,
                                "target_status": "FAR",
                                "prev_rate": 0.0,
                                "supervisor_external_id": "102",
                                "supervision_district": "2",
                                "prev_target_status": None,
                            }
                        ],
                        "target_status_strategy": "ZERO_RATE",
                    }
                ],
                "metrics_without_outliers": [
                    {
                        "name": "incarceration_starts_and_inferred",
                        "outcome_type": "ADVERSE",
                        "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                        "body_display_name": "incarceration rate",
                        "event_name": "incarcerations",
                        "event_name_singular": "incarceration",
                        "event_name_past_tense": "were incarcerated",
                        "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                    }
                ],
                "recipient_email_address": "supervisor2@recidiviz.org",
                "additional_recipients": [
                    "manager2@recidiviz.org",
                    "manager3@recidiviz.org",
                ],
            },
            "101": {
                "metrics": [
                    {
                        "metric": {
                            "name": "incarceration_starts_and_inferred",
                            "outcome_type": "ADVERSE",
                            "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                            "body_display_name": "incarceration rate",
                            "event_name": "incarcerations",
                            "event_name_singular": "incarceration",
                            "event_name_past_tense": "were incarcerated",
                            "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                        },
                        "target": 0.13,
                        "other_officers": {
                            "FAR": [],
                            "MET": [0.11, 0.04, 0.0, 0.12],
                            "NEAR": [0.184, 0.17],
                        },
                        "highlighted_officers": [
                            {
                                "name": {
                                    "given_names": "Officer",
                                    "surname": "1",
                                    "middle_names": None,
                                    "name_suffix": None,
                                },
                                "rate": 0.26,
                                "target_status": "FAR",
                                "prev_rate": 0.32,
                                "supervisor_external_id": "101",
                                "supervision_district": "1",
                                "prev_target_status": "NEAR",
                            },
                            {
                                "name": {
                                    "given_names": "Officer",
                                    "surname": "8",
                                    "middle_names": None,
                                    "name_suffix": None,
                                },
                                "rate": 0.333,
                                "target_status": "FAR",
                                "prev_rate": None,
                                "supervisor_external_id": "101",
                                "supervision_district": "1",
                                "prev_target_status": None,
                            },
                        ],
                        "target_status_strategy": "IQR_THRESHOLD",
                    }
                ],
                "metrics_without_outliers": [
                    {
                        "name": "task_completions_transfer_to_limited_supervision",
                        "outcome_type": "FAVORABLE",
                        "title_display_name": "Limited Supervision Unit Transfer Rate",
                        "body_display_name": "Limited Supervision Unit transfer rate(s)",
                        "event_name": "LSU transfers",
                        "event_name_singular": "LSU transfer",
                        "event_name_past_tense": "were transferred to LSU",
                        "description_markdown": None,
                    }
                ],
                "recipient_email_address": "supervisor1@recidiviz.org",
                "additional_recipients": [],
            },
        }
        self.assertEqual(expected, actual)

        actual_json = {
            supervisor_id: supervisor_data.to_dict()
            for supervisor_id, supervisor_data in actual.items()
        }
        self.assertEqual(expected_json, actual_json)

    def test_get_supervision_officer_entities(
        self,
    ) -> None:
        expected = [
            SupervisionOfficerSupervisorEntity(
                full_name=PersonName(
                    given_names="Supervisor",
                    surname="1",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="101",
                pseudonymized_id="hash1",
                supervision_district=None,
                email="supervisor1@recidiviz.org",
                has_outliers=True,
            ),
            SupervisionOfficerSupervisorEntity(
                full_name=PersonName(
                    given_names="Supervisor",
                    surname="2",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="102",
                pseudonymized_id="hash2",
                supervision_district="2",
                email="supervisor2@recidiviz.org",
                has_outliers=True,
            ),
            SupervisionOfficerSupervisorEntity(
                full_name=PersonName(
                    given_names="Supervisor",
                    surname="3",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="103",
                pseudonymized_id="hash3",
                supervision_district="2",
                email="manager3@recidiviz.org",
                has_outliers=False,
            ),
        ]

        actual = OutliersQuerier(
            StateCode.US_PA
        ).get_supervision_officer_supervisor_entities()

        self.assertEqual(expected, actual)

    def test_get_officers_for_supervisor(self) -> None:
        actual = OutliersQuerier(StateCode.US_PA).get_officers_for_supervisor(
            supervisor_external_id="102", num_lookback_periods=5
        )

        expected = [
            SupervisionOfficerEntity(
                full_name=PersonName(
                    given_names="Officer",
                    surname="3",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="03",
                pseudonymized_id="officerhash3",
                supervisor_external_id="102",
                district="2",
                caseload_type=None,
                outlier_metrics=[
                    {
                        "metric_id": "absconsions_bench_warrants",
                        "statuses_over_time": [
                            {
                                "status": "FAR",
                                "end_date": "2023-05-01",
                                "metric_rate": 0.8,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2023-04-01",
                                "metric_rate": 0.8,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2023-03-01",
                                "metric_rate": 0.8,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2023-02-01",
                                "metric_rate": 0.8,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2023-01-01",
                                "metric_rate": 0.8,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2022-12-01",
                                "metric_rate": 0.8,
                            },
                        ],
                    }
                ],
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    given_names="Officer",
                    surname="4",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="04",
                pseudonymized_id="officerhash4",
                supervisor_external_id="102",
                district="2",
                caseload_type=None,
                outlier_metrics=[
                    {
                        "metric_id": "task_completions_transfer_to_limited_supervision",
                        "statuses_over_time": [
                            {
                                "status": "FAR",
                                "end_date": "2023-05-01",
                                "metric_rate": 0,
                            },
                            {
                                "status": "FAR",
                                "end_date": "2023-04-01",
                                "metric_rate": 0,
                            },
                        ],
                    }
                ],
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    given_names="Officer",
                    surname="6",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="06",
                pseudonymized_id="officerhash6",
                supervisor_external_id="102",
                district="2",
                caseload_type=None,
                outlier_metrics=[],
            ),
        ]

        self.assertEqual(actual, expected)

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

        expected = [
            {
                "metric_id": "absconsions_bench_warrants",
                "caseload_type": "ALL",
                "benchmarks": [
                    {"target": 0.14, "end_date": "2023-05-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-04-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-03-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-02-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-01-01", "threshold": 0.21},
                ],
                "latest_period_values": {
                    "far": [0.8],
                    "near": [0.32],
                    "met": [0.1, 0.1],
                },
            },
            {
                "metric_id": "incarceration_starts_and_inferred",
                "caseload_type": "ALL",
                "benchmarks": [
                    {"target": 0.13, "end_date": "2023-05-01", "threshold": 0.2},
                    {"target": 0.14, "end_date": "2023-04-01", "threshold": 0.21},
                ],
                "latest_period_values": {
                    "far": [0.26, 0.333],
                    "near": [0.17, 0.184],
                    "met": [0.0, 0.04, 0.11, 0.12],
                },
            },
            {
                "metric_id": "task_completions_transfer_to_limited_supervision",
                "caseload_type": "ALL",
                "benchmarks": [
                    {"target": 0.008, "end_date": "2023-05-01", "threshold": 0.1},
                    {"target": 0.008, "end_date": "2023-04-01", "threshold": 0.1},
                ],
                "latest_period_values": {
                    "far": [0.0],
                    "near": [],
                    "met": [0.039, 0.11, 0.126, 0.171, 0.184, 0.27, 0.333],
                },
            },
        ]

        self.assertEqual(expected, actual)

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

        expected = SupervisionOfficerEntity(
            full_name=PersonName(
                given_names="Officer",
                surname="3",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="03",
            pseudonymized_id="officerhash3",
            supervisor_external_id="102",
            district="2",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "status": "FAR",
                            "end_date": "2023-05-01",
                            "metric_rate": 0.8,
                        },
                    ],
                }
            ],
        )

        self.assertEqual(expected, actual)

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
        print(result)

        self.assertListEqual([config.id for config in result], [1, 4, 3, 2])
