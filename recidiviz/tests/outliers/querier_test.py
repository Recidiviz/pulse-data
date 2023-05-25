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
"""This class implmenets tests for the OutliersQuerier class"""
import csv
import os
from datetime import date
from typing import Dict, List, Optional
from unittest import TestCase

import pytest

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.querier.querier import (
    Entity,
    MetricInfo,
    OutliersQuerier,
    OutliersReportData,
    TargetStatus,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    OutliersBase,
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

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_get_officer_level_report_data_by_unit(self) -> None:
        actual = OutliersQuerier().get_officer_level_report_data_for_all_units(
            state_code=StateCode.US_PA,
            db_key=self.database_key,
            end_date=TEST_END_DATE,
        )
        expected = {
            "A": OutliersReportData(
                metrics={
                    "incarceration_starts_and_inferred": MetricInfo(
                        context={
                            "target": 0.13887506249377812,
                            "min": 0.0,
                            "max": 0.31938677738741617,
                        },
                        entities=[
                            Entity(
                                name="Officer 1",
                                rate=0.26688907422852376,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.31938677738741617,
                                prev_target_status=TargetStatus.FAR,
                            ),
                            Entity(
                                name="Officer 2",
                                rate=0.12645777715329493,
                                target_status=TargetStatus.MET,
                                prev_rate=0.12858428700012858,
                                prev_target_status=TargetStatus.MET,
                            ),
                            Entity(
                                name="Officer 5",
                                rate=0.03996003996003996,
                                target_status=TargetStatus.MET,
                                prev_rate=0.07616146230007616,
                                prev_target_status=TargetStatus.MET,
                            ),
                            Entity(
                                name="Officer 7",
                                rate=0.17053206002728513,
                                target_status=TargetStatus.NEAR,
                                prev_rate=0.135013501350135,
                                prev_target_status=TargetStatus.MET,
                            ),
                        ],
                    )
                },
                entity_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            ),
            "B": OutliersReportData(
                metrics={
                    "incarceration_starts_and_inferred": MetricInfo(
                        context={
                            "target": 0.13887506249377812,
                            "min": 0.0,
                            "max": 0.31938677738741617,
                        },
                        entities=[
                            Entity(
                                name="Officer 3",
                                rate=0.18409086725207563,
                                target_status=TargetStatus.NEAR,
                                prev_rate=0.18505667360629194,
                                prev_target_status=TargetStatus.NEAR,
                            ),
                            Entity(
                                name="Officer 4",
                                rate=0.2898550724637681,
                                target_status=TargetStatus.FAR,
                                prev_rate=0.0,
                                prev_target_status=TargetStatus.MET,
                            ),
                            Entity(
                                name="Officer 6",
                                rate=0.111000111000111,
                                target_status=TargetStatus.MET,
                                prev_rate=0.12001200120012002,
                                prev_target_status=TargetStatus.MET,
                            ),
                        ],
                    )
                },
                entity_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            ),
        }

        self.assertEqual(actual, expected)
