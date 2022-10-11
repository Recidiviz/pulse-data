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
from pathlib import Path

import pandas as pd

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.spreadsheet import SpreadsheetInterface
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

    def test_ingest_json_response_arrests_errors(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_A
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
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/arrests_metric_errors.xlsx"
            ).open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                metric_definitions=MetricInterface.get_metric_definitions(
                    systems={schema.System.LAW_ENFORCEMENT}
                ),
            )
            filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE["LAW_ENFORCEMENT"]
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.total_arrests.key:
                    self.assertEqual(
                        metric["display_name"],
                        law_enforcement.total_arrests.display_name,
                    )
                    self.assertEqual(len(metric["metric_errors"]), 2)
                    for sheet in metric["metric_errors"]:
                        if (
                            sheet["display_name"]
                            == filename_to_metricfile["arrests_by_race"].display_name
                        ):

                            self.assertEqual(sheet["sheet_name"], "arrests_by_race")
                            self.assertTrue(
                                {
                                    "title": "Missing Column",
                                    "subtitle": None,
                                    "type": "ERROR",
                                }.items()
                                <= sheet["messages"][0].items()
                            )
                        elif (
                            sheet["display_name"]
                            == filename_to_metricfile["arrests"].display_name
                        ):
                            self.assertEqual(sheet["sheet_name"], "arrests")
                            self.assertTrue(
                                {
                                    "title": "Too Many Rows",
                                    "subtitle": "6/2021",
                                    "type": "ERROR",
                                }.items()
                                <= sheet["messages"][0].items()
                            )
                        elif (
                            sheet["display_name"]
                            == filename_to_metricfile["arrests_by_type"].display_name
                        ):
                            self.assertEqual(sheet["sheet_name"], "arrests_by_type")
                            self.assertTrue(
                                {
                                    "title": "Missing Total Value",
                                    "subtitle": None,
                                    "type": "WARNING",
                                }.items()
                                <= sheet["messages"][0].items()
                            )

                    # 24 total datapoints. 2 for aggregate total (May and June), 10 for gender breakdowns (May-June),
                    # 10 for arrest_by_type breakdowns (May - June).
                    self.assertEqual(len(metric["datapoints"]), 22)
                else:
                    metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                    self.assertEqual(len(metric["metric_errors"]), 1)
                    self.assertEqual(len(metric["metric_errors"][0]["messages"]), 1)
                    message = metric["metric_errors"][0]["messages"][0]
                    self.assertEqual(message["title"], "Missing Metric")
                    self.assertEqual(message["type"], "ERROR")
                    self.assertTrue(
                        f"No data for the {metric_definition.display_name} metric was provided. "
                    )
            self.assertEqual(len(json_response["non_metric_errors"]), 0)
            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)

    def test_ingest_json_response_annual_budget_missing_metric(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_A
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
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/annual_budget_metric.xlsx"
            ).open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                metric_definitions=MetricInterface.get_metric_definitions(
                    systems={schema.System.LAW_ENFORCEMENT}
                ),
            )
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.annual_budget.key:
                    self.assertEqual(
                        metric["display_name"],
                        law_enforcement.annual_budget.display_name,
                    )
                    self.assertEqual(metric["metric_errors"], [])
                    # 3 total datapoints, for each year (2020-2022) annual budget.
                    self.assertEqual(len(metric["datapoints"]), 3)
                    for datapoint in metric["datapoints"]:
                        self.assertIsNone(datapoint["old_value"])
                elif len(metric["metric_errors"]) != 0:
                    metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                    self.assertEqual(len(metric["metric_errors"]), 1)
                    self.assertEqual(len(metric["metric_errors"][0]["messages"]), 1)
                    message = metric["metric_errors"][0]["messages"][0]
                    self.assertEqual(message["title"], "Missing Metric")
                    self.assertEqual(message["type"], "ERROR")
                    self.assertTrue(
                        f"No data for the {metric_definition.display_name} metric was provided. "
                        in message["description"]
                    )

            self.assertEqual(len(json_response["non_metric_errors"]), 0)
            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)
            # Uploading the spreadsheet with changes will result in datapoints
            # with non-None old_value values.
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/annual_budget_metric_update.xlsx"
            ).open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                metric_definitions=MetricInterface.get_metric_definitions(
                    systems={schema.System.LAW_ENFORCEMENT}
                ),
            )
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.annual_budget.key:
                    # 3 total datapoints, for each year (2020-2022) annual budget.
                    self.assertEqual(len(metric["datapoints"]), 3)
                    for datapoint in metric["datapoints"]:
                        self.assertIsNotNone(datapoint["old_value"])
            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)

    def test_ingest_json_response_arrests_wrong_system(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_A
            session.add_all([user, agency])
            session.commit()
            session.refresh(user)
            session.refresh(agency)
            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.PROSECUTION,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add(spreadsheet)
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/arrests_metric_errors.xlsx"
            ).open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                metric_definitions=MetricInterface.get_metric_definitions(
                    systems={schema.System.PROSECUTION}
                ),
            )
            # There should be a missing metric error for each metric in prosecution.
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                self.assertEqual(len(metric["metric_errors"]), 1)
                self.assertEqual(len(metric["metric_errors"][0]["messages"]), 1)
                message = metric["metric_errors"][0]["messages"][0]
                self.assertEqual(message["title"], "Missing Metric")
                self.assertEqual(message["type"], "ERROR")
                self.assertTrue(
                    f"No data for the {metric_definition.display_name} metric was provided. "
                    in message["description"]
                )

            # 1 non_metric error for invalid sheet names.
            self.assertEqual(len(json_response["non_metric_errors"]), 1)
            error = json_response["non_metric_errors"][0]
            self.assertEqual(error["type"], "ERROR")
            self.assertEqual(error["title"], "Invalid Sheet Names")
            self.assertTrue(
                "The following sheet names do not correspond to a metric for"
                in error["description"]
            )
            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)

    def test_ingest_json_response_success(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_A
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
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/law_enforcement_success.xlsx"
            ).open("rb")
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
                metric_key_to_agency_datapoints={},
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                metric_definitions=MetricInterface.get_metric_definitions(
                    systems={schema.System.LAW_ENFORCEMENT}
                ),
            )
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.annual_budget.key:
                    self.assertEqual(metric["metric_errors"], [])
                    self.assertTrue(len(metric["datapoints"]) > 0)

            self.assertEqual(len(json_response["non_metric_errors"]), 0)
            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.INGESTED)
