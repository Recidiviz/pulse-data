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
                datapoint_json_list,
                sheet_to_error,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                datapoint_json_list=datapoint_json_list,
                sheet_to_error=sheet_to_error,
                system="LAW_ENFORCEMENT",
            )
            filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE["LAW_ENFORCEMENT"]
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.total_arrests.key:
                    self.assertEqual(
                        metric["display_name"],
                        law_enforcement.total_arrests.display_name,
                    )
                    self.assertEqual(len(metric["sheets"]), 2)
                    for sheet in metric["sheets"]:
                        if (
                            sheet["display_name"]
                            == filename_to_metricfile["arrests_by_race"].display_name
                        ):

                            self.assertEqual(sheet["sheet_name"], "arrests_by_race")
                            self.assertEqual(
                                sheet["messages"],
                                [
                                    {
                                        "title": "Missing Column",
                                        "subtitle": "race/ethnicity",
                                        "description": "We expected the following column: 'race/ethnicity'. Only the following column names were found in the sheet: year, month, value.",
                                        "type": "ERROR",
                                    },
                                ],
                            )
                        elif (
                            sheet["display_name"]
                            == filename_to_metricfile["arrests"].display_name
                        ):
                            self.assertEqual(sheet["sheet_name"], "arrests")
                            self.assertEqual(
                                sheet["messages"],
                                [
                                    {
                                        "title": "Too Many Rows",
                                        "subtitle": "6/2021",
                                        "description": "There should only be a single row containing data for arrests in 6/2021.",
                                        "type": "ERROR",
                                    },
                                ],
                            )
                    # 24 total datapoints. 2 for aggregate total (May and June), 10 for gender breakdowns (May-June),
                    # 10 for arrest_by_type breakdowns (May - June).
                    self.assertEqual(len(metric["datapoints"]), 24)
                else:
                    metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                    self.assertEqual(len(metric["sheets"]), 1)
                    self.assertEqual(
                        metric["sheets"][0]["messages"],
                        [
                            {
                                "title": "Missing Metric",
                                "subtitle": None,
                                "description": f"No sheets for the '{metric_definition.display_name}' metric were provided.",
                                "type": "WARNING",
                            },
                        ],
                    )
            self.assertEqual(len(json_response["pre_ingest_errors"]), 0)

    def test_ingest_json_response_annual_budget_no_errors(self) -> None:
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
                datapoint_json_list,
                sheet_to_error,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                datapoint_json_list=datapoint_json_list,
                sheet_to_error=sheet_to_error,
                system="LAW_ENFORCEMENT",
            )
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.annual_budget.key:
                    self.assertEqual(
                        metric["display_name"],
                        law_enforcement.annual_budget.display_name,
                    )
                    self.assertEqual(metric["sheets"], [])
                    # 3 total datapoints, for each year (2020-2022) annual budget.
                    self.assertEqual(len(metric["datapoints"]), 3)
                    for datapoint in metric["datapoints"]:
                        self.assertIsNone(datapoint["old_value"])
                elif len(metric["sheets"]) != 0:
                    metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                    self.assertEqual(len(metric["sheets"]), 1)
                    self.assertEqual(
                        metric["sheets"][0]["messages"],
                        [
                            {
                                "title": "Missing Metric",
                                "subtitle": None,
                                "description": f"No sheets for the '{metric_definition.display_name}' metric were provided.",
                                "type": "WARNING",
                            },
                        ],
                    )

            self.assertEqual(len(json_response["pre_ingest_errors"]), 0)
            # Uploading the spreadsheet with changes will result in datapoints
            # with non-None old_value values.
            file = (
                self.bulk_upload_test_files
                / "law_enforcement/annual_budget_metric_update.xlsx"
            ).open("rb")
            (
                datapoint_json_list,
                sheet_to_error,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                datapoint_json_list=datapoint_json_list,
                sheet_to_error=sheet_to_error,
                system="LAW_ENFORCEMENT",
            )
            for metric in json_response["metrics"]:
                if metric["key"] == law_enforcement.annual_budget.key:
                    # 3 total datapoints, for each year (2020-2022) annual budget.
                    self.assertEqual(len(metric["datapoints"]), 3)
                    for datapoint in metric["datapoints"]:
                        self.assertIsNotNone(datapoint["old_value"])

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
                datapoint_json_list,
                sheet_to_error,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                xls=pd.ExcelFile(file),
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            json_response = SpreadsheetInterface.get_ingest_spreadsheet_json(
                datapoint_json_list=datapoint_json_list,
                sheet_to_error=sheet_to_error,
                system="LAW_ENFORCEMENT",
            )
            # There should be a metric error for the arrest metric because it does
            # not belong to the prosecution metrics and a warning for the annual_budget
            # metric because no sheet was provided. The rest of the warnings regarding
            # missing metrics are in pre_ingest. Annual budget is an exception because
            # all systems have an annual budget metric, and as a result, an annual_budget
            # sheet name.
            self.assertEqual(len(json_response["metrics"]), 7)
            for metric in json_response["metrics"]:
                metric_definition = METRIC_KEY_TO_METRIC[metric["key"]]
                if metric["key"] == law_enforcement.annual_budget.key:
                    self.assertEqual(len(metric["sheets"]), 1)
                    self.assertEqual(
                        metric["sheets"][0]["messages"],
                        [
                            {
                                "title": "Missing Metric",
                                "subtitle": None,
                                "description": f"No sheets for the '{metric_definition.display_name}' metric were provided.",
                                "type": "WARNING",
                            },
                        ],
                    )
                elif len(metric["sheets"]) != 0:
                    self.assertEqual(len(metric["sheets"]), 4)
                    for sheet in metric["sheets"]:
                        self.assertEqual(
                            sheet["messages"][0]["title"], "Metric Not Found"
                        )
                        self.assertEqual(sheet["messages"][0]["type"], "ERROR")
                        self.assertTrue(
                            "No metric corresponds to the filename"
                            in sheet["messages"][0]["description"]
                        )

            # 6 pre-ingest warnings because no data was provided for the 6 prosecution metrics.
            self.assertEqual(len(json_response["pre_ingest_errors"]), 6)
            for error in json_response["pre_ingest_errors"]:
                self.assertEqual(error["type"], "WARNING")
                self.assertEqual(error["title"], "Missing Metric")
                self.assertTrue("No sheets for the" in error["description"])
