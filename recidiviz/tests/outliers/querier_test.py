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
import os
from datetime import date
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    INCARCERATION_STARTS_AND_INFERRED,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
)
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import (
    OfficerMetricEntity,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersConfig,
    OutliersMetricConfig,
    SupervisionDistrictReportData,
    SupervisionOfficerSupervisorMetricEntity,
    SupervisionOfficerSupervisorMetricInfo,
    TargetStatus,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    OutliersBase,
    SupervisionDistrict,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionOfficerSupervisor,
    SupervisionStateMetric,
    SupervisionUnit,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.outliers import fixtures
from recidiviz.tools.postgres import local_postgres_helpers


def load_model_fixture(
    model: OutliersBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            results.append(row)

    return results


TEST_END_DATE = date(year=2023, month=5, day=1)
TEST_PREV_END_DATE = date(year=2023, month=4, day=1)

TEST_METRIC_1 = OutliersMetricConfig.build_from_metric(
    metric=INCARCERATION_STARTS_AND_INFERRED,
    title_display_name="Incarceration Rate (CPVs & TPVs)",
    body_display_name="incarceration rate",
    event_name="incarcerations",
)

TEST_METRIC_2 = OutliersMetricConfig.build_from_metric(
    metric=TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    title_display_name="Limited Supervision Unit Transfer Rate",
    body_display_name="Limited Supervision Unit transfer rate(s)",
    event_name="LSU transfers",
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
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, db_name="us_xx")
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            for unit in load_model_fixture(SupervisionUnit):
                session.add(SupervisionUnit(**unit))
            for officer in load_model_fixture(SupervisionOfficer):
                session.add(SupervisionOfficer(**officer))
            for metric in load_model_fixture(SupervisionStateMetric):
                session.add(SupervisionStateMetric(**metric))
            for metric in load_model_fixture(SupervisionOfficerMetric):
                session.add(SupervisionOfficerMetric(**metric))
            for supervisor in load_model_fixture(SupervisionOfficerSupervisor):
                session.add(SupervisionOfficerSupervisor(**supervisor))
            for district in load_model_fixture(SupervisionDistrict):
                session.add(SupervisionDistrict(**district))
            for manager in load_model_fixture(SupervisionDistrictManager):
                session.add(SupervisionDistrictManager(**manager))

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    @patch("recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_config")
    def test_get_officer_level_report_data_by_supervisor(
        self, mock_config: MagicMock
    ) -> None:
        mock_config.return_value = OutliersConfig(
            metrics=[TEST_METRIC_1, TEST_METRIC_2],
            supervision_officer_label="officer",
        )

        actual = (
            OutliersQuerier().get_officer_level_report_data_for_all_officer_supervisors(
                state_code=StateCode.US_XX,
                end_date=TEST_END_DATE,
            )
        )

        expected = {
            "101": OfficerSupervisorReportData(
                metrics=[
                    OutlierMetricInfo(
                        metric=TEST_METRIC_1,
                        target=0.13887506249377812,
                        other_officers={
                            TargetStatus.FAR: [],
                            TargetStatus.MET: [
                                0.12645777715329493,
                                0.0,
                                0.03996003996003996,
                                0.111000111000111,
                            ],
                            TargetStatus.NEAR: [
                                0.18409086725207563,
                                0.17053206002728513,
                            ],
                        },
                        highlighted_officers=[
                            OfficerMetricEntity(
                                name="Officer 1",
                                rate=0.26688907422852376,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.31938677738741617,
                                prev_target_status=TargetStatus.FAR,
                                supervisor_external_id="101",
                            ),
                            OfficerMetricEntity(
                                name="Officer 8",
                                rate=0.3333333333333333,
                                target_status=TargetStatus.FAR,
                                prev_rate=None,
                                prev_target_status=None,
                                supervisor_external_id="101",
                            ),
                        ],
                        target_status_strategy=TargetStatusStrategy.IQR_THRESHOLD,
                    )
                ],
                metrics_without_outliers=[TEST_METRIC_2],
                recipient_email_address="supervisor1@recidiviz.org",
            ),
            "102": OfficerSupervisorReportData(
                metrics=[
                    OutlierMetricInfo(
                        metric=TEST_METRIC_2,
                        target=0.008800003960001782,
                        other_officers={
                            TargetStatus.FAR: [],
                            TargetStatus.MET: [
                                0.26688907422852376,
                                0.12645777715329493,
                                0.18409086725207563,
                                0.03996003996003996,
                                0.111000111000111,
                                0.17053206002728513,
                                0.3333333333333333,
                            ],
                            TargetStatus.NEAR: [],
                        },
                        highlighted_officers=[
                            OfficerMetricEntity(
                                name="Officer 4",
                                rate=0.0,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.0,
                                supervisor_external_id="102",
                                prev_target_status=None,
                            )
                        ],
                        target_status_strategy=TargetStatusStrategy.ZERO_RATE,
                    )
                ],
                metrics_without_outliers=[TEST_METRIC_1],
                recipient_email_address="supervisor2@recidiviz.org",
            ),
            "103": OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[
                    TEST_METRIC_1,
                    TEST_METRIC_2,
                ],
                recipient_email_address="supervisor3@recidiviz.org",
            ),
        }

        expected_json = {
            "101": {
                "metrics": [
                    {
                        "metric": {
                            "name": "incarceration_starts_and_inferred",
                            "outcome_type": "ADVERSE",
                            "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                            "body_display_name": "incarceration rate",
                            "event_name": "incarcerations",
                        },
                        "target": 0.13887506249377812,
                        "other_officers": {
                            "FAR": [],
                            "MET": [
                                0.12645777715329493,
                                0.0,
                                0.03996003996003996,
                                0.111000111000111,
                            ],
                            "NEAR": [0.18409086725207563, 0.17053206002728513],
                        },
                        "highlighted_officers": [
                            {
                                "name": "Officer 1",
                                "rate": 0.26688907422852376,
                                "target_status": "FAR",
                                "prev_rate": 0.31938677738741617,
                                "prev_target_status": "FAR",
                                "supervisor_external_id": "101",
                            },
                            {
                                "name": "Officer 8",
                                "rate": 0.3333333333333333,
                                "target_status": "FAR",
                                "prev_rate": None,
                                "prev_target_status": None,
                                "supervisor_external_id": "101",
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
                    }
                ],
                "recipient_email_address": "supervisor1@recidiviz.org",
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
                        },
                        "target": 0.008800003960001782,
                        "other_officers": {
                            "FAR": [],
                            "MET": [
                                0.26688907422852376,
                                0.12645777715329493,
                                0.18409086725207563,
                                0.03996003996003996,
                                0.111000111000111,
                                0.17053206002728513,
                                0.3333333333333333,
                            ],
                            "NEAR": [],
                        },
                        "highlighted_officers": [
                            {
                                "name": "Officer 4",
                                "rate": 0.0,
                                "target_status": "FAR",
                                "prev_rate": 0.0,
                                "supervisor_external_id": "102",
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
                    }
                ],
                "recipient_email_address": "supervisor2@recidiviz.org",
            },
            "103": {
                "metrics": [],
                "metrics_without_outliers": [
                    {
                        "name": "incarceration_starts_and_inferred",
                        "outcome_type": "ADVERSE",
                        "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                        "body_display_name": "incarceration rate",
                        "event_name": "incarcerations",
                    },
                    {
                        "name": "task_completions_transfer_to_limited_supervision",
                        "outcome_type": "FAVORABLE",
                        "title_display_name": "Limited Supervision Unit Transfer Rate",
                        "body_display_name": "Limited Supervision Unit transfer rate(s)",
                        "event_name": "LSU transfers",
                    },
                ],
                "recipient_email_address": "supervisor3@recidiviz.org",
            },
        }
        self.assertEqual(actual, expected)

        actual_json = {
            supervisor_id: supervisor_data.to_json()
            for supervisor_id, supervisor_data in actual.items()
        }
        self.assertEqual(expected_json, actual_json)

    @patch("recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_config")
    def test_get_supervision_district_report_data_by_district(
        self,
        mock_config: MagicMock,
    ) -> None:
        mock_config.return_value = OutliersConfig(
            metrics=[TEST_METRIC_1],
            supervision_officer_label="officer",
        )

        actual = OutliersQuerier().get_supervision_district_report_data_by_district(
            state_code=StateCode.US_XX,
            end_date=TEST_END_DATE,
        )

        expected = {
            "1001": SupervisionDistrictReportData(
                recipient_name="Manager 1",
                recipient_email="manager1@recidiviz.org",
                entities=[
                    SupervisionOfficerSupervisorMetricEntity(
                        supervisor_name="Supervisor 1",
                        metrics=[
                            SupervisionOfficerSupervisorMetricInfo(
                                metric=TEST_METRIC_1,
                                officers_far_pct=0.4,
                                prev_officers_far_pct=0.25,
                                officer_rates={
                                    TargetStatus.MET: [
                                        0.12645777715329493,
                                        0.03996003996003996,
                                    ],
                                    TargetStatus.NEAR: [0.17053206002728513],
                                    TargetStatus.FAR: [
                                        0.26688907422852376,
                                        0.3333333333333333,
                                    ],
                                },
                            )
                        ],
                    )
                ],
                officer_label="officer",
            ),
            "1002": SupervisionDistrictReportData(
                recipient_name="Manager 2",
                recipient_email="manager2@recidiviz.org",
                entities=[
                    SupervisionOfficerSupervisorMetricEntity(
                        supervisor_name="Supervisor 2",
                        metrics=[
                            SupervisionOfficerSupervisorMetricInfo(
                                metric=TEST_METRIC_1,
                                officers_far_pct=0.0,
                                prev_officers_far_pct=0.0,
                                officer_rates={
                                    TargetStatus.MET: [0.0, 0.111000111000111],
                                    TargetStatus.NEAR: [0.18409086725207563],
                                    TargetStatus.FAR: [],
                                },
                            )
                        ],
                    ),
                    SupervisionOfficerSupervisorMetricEntity(
                        supervisor_name="Supervisor 3",
                        metrics=[
                            SupervisionOfficerSupervisorMetricInfo(
                                metric=TEST_METRIC_1,
                                officers_far_pct=0,
                                prev_officers_far_pct=0,
                                officer_rates={
                                    TargetStatus.MET: [],
                                    TargetStatus.NEAR: [],
                                    TargetStatus.FAR: [],
                                },
                            )
                        ],
                    ),
                ],
                officer_label="officer",
            ),
        }

        expected_json = {
            "1001": {
                "recipient_name": "Manager 1",
                "recipient_email": "manager1@recidiviz.org",
                "entities": [
                    {
                        "supervisor_name": "Supervisor 1",
                        "metrics": [
                            {
                                "metric": {
                                    "name": "incarceration_starts_and_inferred",
                                    "outcome_type": "ADVERSE",
                                    "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                                    "body_display_name": "incarceration rate",
                                    "event_name": None,
                                },
                                "officers_far_pct": 0.4,
                                "prev_officers_far_pct": 0.25,
                                "officer_rates": {
                                    "MET": [0.12645777715329493, 0.03996003996003996],
                                    "NEAR": [0.17053206002728513],
                                    "FAR": [0.26688907422852376, 0.3333333333333333],
                                },
                            }
                        ],
                    }
                ],
                "officer_label": "officer",
            },
            "1002": {
                "recipient_name": "Manager 2",
                "recipient_email": "manager2@recidiviz.org",
                "entities": [
                    {
                        "supervisor_name": "Supervisor 2",
                        "metrics": [
                            {
                                "metric": {
                                    "name": "incarceration_starts_and_inferred",
                                    "outcome_type": "ADVERSE",
                                    "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                                    "body_display_name": "incarceration rate",
                                    "event_name": None,
                                },
                                "officers_far_pct": 0.0,
                                "prev_officers_far_pct": 0.0,
                                "officer_rates": {
                                    "MET": [0.0, 0.111000111000111],
                                    "NEAR": [0.18409086725207563],
                                    "FAR": [],
                                },
                            }
                        ],
                    },
                    {
                        "supervisor_name": "Supervisor 3",
                        "metrics": [
                            {
                                "metric": {
                                    "name": "incarceration_starts_and_inferred",
                                    "outcome_type": "ADVERSE",
                                    "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                                    "body_display_name": "incarceration rate",
                                    "event_name": None,
                                },
                                "officers_far_pct": 0,
                                "prev_officers_far_pct": 0,
                                "officer_rates": {"MET": [], "NEAR": [], "FAR": []},
                            }
                        ],
                    },
                ],
                "officer_label": "officer",
            },
        }

        self.assertEqual(actual, expected)

        actual_json = {
            manager_id: manager_data.to_json()
            for manager_id, manager_data in actual.items()
        }
        print(actual_json)
        self.assertEqual(actual_json, expected_json)
