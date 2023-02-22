# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This class implements tests for the Justice Counts SpreadsheetInterface."""
import datetime
from pathlib import Path

import pandas as pd
from freezegun import freeze_time

from recidiviz.justice_counts.metrics import supervision
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.spreadsheet import SpreadsheetInterface
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)


class TestSpreadsheetInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the SpreadsheetInterface."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.bulk_upload_test_files = Path(
            "recidiviz/tests/justice_counts/bulk_upload/bulk_upload_fixtures"
        )

    def test_ingest_spreadsheet(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_E
            session.add_all([user, agency])
            session.commit()
            session.refresh(user)
            session.refresh(agency)
            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.LAW_ENFORCEMENT,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add(spreadsheet)
            update_datetime = datetime.datetime(
                2022, 2, 1, 1, 0, 0, 0, datetime.timezone.utc
            )
            with freeze_time(update_datetime):
                file = (self.bulk_upload_test_files / "success.xlsx").open("rb")
                SpreadsheetInterface.ingest_spreadsheet(
                    session=session,
                    xls=pd.ExcelFile(file),
                    spreadsheet=spreadsheet,
                    auth0_user_id=user.auth0_user_id,
                    agency_id=agency.id,
                    metric_key_to_agency_datapoints={},
                )

                spreadsheet = session.query(schema.Spreadsheet).one()
                self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.INGESTED)
                self.assertEqual(spreadsheet.ingested_by, user.auth0_user_id)
                self.assertEqual(spreadsheet.ingested_at, update_datetime)

    def test_ingest_spreadsheet_failure(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_A
            session.add_all([user, agency])
            session.commit()
            session.refresh(user)
            session.refresh(agency)
            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.SUPERVISION,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add(spreadsheet)
            # invalid.xlsx has two rows for the same time period. This will cause the
            # Too Many Rows error to be raised during ingest.
            file = (self.bulk_upload_test_files / "invalid.xlsx").open("rb")
            SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )

            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)
            self.assertEqual(spreadsheet.ingested_by, None)
            self.assertEqual(spreadsheet.ingested_at, None)

    def test_get_ingest_spreadsheet_json(self) -> None:
        # Tests that spreadsheet jsons will include the right metrics. If
        # a metric is disaggregated by supervision subsystems, only the metrics
        # for the subsystems should be in the response. If the metric is not disaggregated,
        # then the response should only return results for the supverision (combined) values
        # for that metric.
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_E
            session.add_all([user, agency])
            session.commit()
            session.refresh(user)
            session.refresh(agency)
            # Agency datapoint that makes the supervision funding metric be
            # disaggregated by subsystem
            disaggregation_datapoint = schema.Datapoint(
                metric_definition_key=supervision.funding.key,
                source=agency,
                context_key=DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
                dimension_identifier_to_member=None,
                value=str(True),
            )
            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.SUPERVISION,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add_all([disaggregation_datapoint, spreadsheet])
            metric_key_to_agency_datapoints = {
                supervision.funding.key: [disaggregation_datapoint],
            }
            file = (self.bulk_upload_test_files / "invalid.xlsx").open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )

            metric_definitions = MetricInterface.get_metric_definitions_for_systems(
                systems={
                    schema.System[system]
                    for system in agency.systems or []
                    if schema.System[system] in schema.System.supervision_subsystems()
                    or schema.System[system] == schema.System.SUPERVISION
                }
            )

            json = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_errors=metric_key_to_errors,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_definitions=metric_definitions,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )
            metric_key_to_json = {m["key"]: m for m in json["metrics"]}
            for definition in metric_definitions:
                if definition.key == supervision.funding.key:
                    # Funding metric is disaggregated, it should not be included in the response json.
                    self.assertIsNone(metric_key_to_json.get(supervision.funding.key))
                elif definition.system in schema.System.supervision_subsystems():
                    if metric_key_to_json.get(definition.key) is not None:
                        # Funding metric should be aggregated by the supervision subsystems.
                        self.assertTrue("BUDGET" in definition.key)
                else:
                    # Every other metric should be represented in the json.
                    self.assertIsNotNone(metric_key_to_json.get(definition.key))
