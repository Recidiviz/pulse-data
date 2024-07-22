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
import itertools
import os
import tempfile
from io import BytesIO
from pathlib import Path

import pandas as pd
from freezegun import freeze_time

from recidiviz.justice_counts.bulk_upload.template_generator import (
    generate_bulk_upload_template,
)
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics import prisons, supervision
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.spreadsheet import SpreadsheetInterface
from recidiviz.justice_counts.utils.constants import UploadMethod
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.spreadsheet_helpers import create_excel_file
from recidiviz.tests.justice_counts.utils.utils import (
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
            file_name = "test_ingest_spreadsheet.xlsx"
            with freeze_time(update_datetime):
                file_path = create_excel_file(
                    system=schema.System.LAW_ENFORCEMENT,
                    file_name=file_name,
                )
                content = open(  # pylint:disable=consider-using-with
                    Path(file_path), "rb"
                ).read()
                file = BytesIO(content)
                SpreadsheetInterface.ingest_spreadsheet(
                    session=session,
                    file=file,
                    file_name=file_name,
                    spreadsheet=spreadsheet,
                    auth0_user_id=user.auth0_user_id,
                    agency=agency,
                    metric_key_to_metric_interface={},
                    upload_method=UploadMethod.BULK_UPLOAD,
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
                system=schema.System.LAW_ENFORCEMENT,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            file_name = "test_ingest_spreadsheet_failure.xlsx"
            session.add(spreadsheet)
            # Excel workbook will have an invalid sheet.
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                file_name=file_name,
                add_invalid_sheet_name=True,
            )
            content = open(  # pylint:disable=consider-using-with
                Path(file_path), "rb"
            ).read()
            file = BytesIO(content)
            SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                file=file,
                file_name=file_name,
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency=agency,
                metric_key_to_metric_interface={},
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.ERRORED)
            self.assertEqual(spreadsheet.ingested_by, None)
            self.assertEqual(spreadsheet.ingested_at, None)

    def test_get_ingest_spreadsheet_json(self) -> None:
        # Tests that spreadsheet jsons will include the right metrics.
        # The response should contain whatever was included in the spreadsheet,
        # even if it does not exactly match how the metrics are configured.
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_agency_E
            session.add_all([user, agency])
            session.commit()
            session.refresh(user)
            session.refresh(agency)

            # Add a supervision funding metric that is disaggregated by subsystem.
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=MetricInterface(
                    key=supervision.funding.key,
                    disaggregated_by_supervision_subsystems=True,
                ),
            )

            # Add a supervision expenses metric that is enabled.
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=MetricInterface(
                    key=supervision.expenses.key,
                    is_metric_enabled=True,
                ),
            )

            # Add a supervision discharges metric that is disabled.
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=agency,
                agency_metric_updates=MetricInterface(
                    key=supervision.discharges.key,
                    is_metric_enabled=False,
                ),
            )

            metric_key_to_metric_interface = (
                MetricSettingInterface.get_metric_key_to_metric_interface(
                    session=session, agency=agency
                )
            )

            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.SUPERVISION,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add(spreadsheet)
            file_name = "test_get_ingest_spreadsheet_json.xlsx"
            # Excel workbook will have an invalid sheet.
            file_path = create_excel_file(
                system=schema.System.SUPERVISION,
                metric_key_to_subsystems={
                    supervision.funding.key: [
                        schema.System.PAROLE,
                        schema.System.PROBATION,
                    ]
                },
                file_name=file_name,
            )
            content = open(  # pylint:disable=consider-using-with
                Path(file_path), "rb"
            ).read()
            file = BytesIO(content)
            file.filename = os.path.basename(file_path)  # type: ignore[attr-defined]
            ingest_result = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                file=file,
                file_name=file_name,  # type: ignore[arg-type]
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency=agency,
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            metric_definitions = (
                SpreadsheetInterface.get_metric_definitions_for_workbook(
                    system=schema.System.SUPERVISION, agency=agency
                )
            )

            json = SpreadsheetInterface.get_ingest_spreadsheet_json(
                metric_definitions=metric_definitions,
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                updated_report_jsons=[],
                new_report_jsons=[],
                unchanged_report_jsons=[],
                ingest_result=ingest_result,
            )

            metric_key_to_json = {m["key"]: m for m in json["metrics"]}
            for definition in metric_definitions:
                if definition.key == supervision.funding.key:
                    # Funding metric is disaggregated, it should not be included in the response json.
                    self.assertIsNone(metric_key_to_json.get(supervision.funding.key))
                elif definition.system in schema.System.supervision_subsystems():
                    if metric_key_to_json.get(definition.key) is not None:
                        # Funding metric should be aggregated by the supervision subsystems.
                        self.assertTrue("FUNDING" in definition.key)
                elif definition.key == supervision.expenses.key:
                    self.assertEqual(
                        metric_key_to_json[supervision.expenses.key]["enabled"], True
                    )
                elif definition.key == supervision.discharges.key:
                    self.assertEqual(
                        metric_key_to_json[supervision.discharges.key]["enabled"], False
                    )
                else:
                    # Every other metric should be represented in the json.
                    self.assertIsNotNone(metric_key_to_json.get(definition.key))
                    self.assertEqual(
                        metric_key_to_json[definition.key]["enabled"], None
                    )

    def test_template_generator(self) -> None:
        # Testing that a spreadsheet will not include sheets for metric(s) or
        # dissaggregations that have been disabled.
        with SessionFactory.using_database(self.database_key) as session:
            prison_super_agency = self.test_schema_objects.test_prison_super_agency
            prison_child_agency_A = self.test_schema_objects.test_prison_child_agency_A
            prison_child_agency_B = self.test_schema_objects.test_prison_child_agency_B
            test_user_A = self.test_schema_objects.test_user_A
            session.add_all(
                [
                    prison_super_agency,
                    prison_child_agency_A,
                    prison_child_agency_B,
                    test_user_A,
                ]
            )
            session.commit()
            prison_child_agency_A.super_agency_id = prison_super_agency.id
            prison_child_agency_B.super_agency_id = prison_super_agency.id
            session.refresh(prison_super_agency)

            # Write test interfaces to the Metrics Setting table.
            super_agency_metric_interfaces = (
                self.test_schema_objects.get_test_metric_interfaces()
            )
            for metric_interface in super_agency_metric_interfaces:
                MetricSettingInterface.add_or_update_agency_metric_setting(
                    session=session,
                    agency=prison_super_agency,
                    agency_metric_updates=metric_interface,
                )
            # Modify prison admissions metric setting so that it is enabled with a
            # custom reporting frequency.
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=session,
                agency=prison_super_agency,
                agency_metric_updates=MetricInterface(
                    key=prisons.admissions.key,
                    is_metric_enabled=True,
                    custom_reporting_frequency=CustomReportingFrequency(
                        frequency=schema.ReportingFrequency.ANNUAL, starting_month=2
                    ),
                ),
            )

            session.commit()
            system = "PRISONS"
            system_enum = schema.System.PRISONS
            metricfiles = SYSTEM_TO_METRICFILES[system_enum]
            all_sheet_names_set = [
                metricfile.canonical_filename for metricfile in metricfiles
            ]

            with tempfile.TemporaryDirectory() as tempbulkdir:
                # Standard Template
                file_path = os.path.join(tempbulkdir, str(system) + ".xlsx")
                generate_bulk_upload_template(
                    system_enum, file_path, session, prison_super_agency
                )
                xls = pd.ExcelFile(file_path)
                sheet_names_set = xls.sheet_names
                sheet_name_to_df = pd.read_excel(xls, sheet_name=None)
                for sheet_name in all_sheet_names_set:
                    if sheet_name not in ("admissions",):
                        # Un-configured / disabled metrics are not included in the workbook
                        self.assertFalse(sheet_name in sheet_names_set)
                    else:
                        self.assertTrue(sheet_name in sheet_names_set)
                        df = sheet_name_to_df[sheet_name]
                        rows = df.to_dict("records")
                        for row in rows:
                            self.assertTrue(
                                "month" not in row
                            )  # Custom Reporting Frequency is set to ANNUAL
                            self.assertTrue("agency" in row)
                            self.assertTrue(
                                row["agency"]
                                in {
                                    prison_child_agency_A.name,
                                    prison_child_agency_B.name,
                                }
                            )

                # Single-page template
                file_path = os.path.join(tempbulkdir, str(system) + ".xlsx")
                generate_bulk_upload_template(
                    system_enum,
                    file_path,
                    session,
                    prison_super_agency,
                    is_single_page_template=True,
                )
                xls = pd.ExcelFile(file_path)
                self.assertEqual(len(xls.sheet_names), 1)
                df = pd.read_excel(xls, sheet_name="Sheet 1")
                metrics_in_sheet = df["metric"].unique()
                self.assertFalse({"admissions"} == (metrics_in_sheet))
                agencies = df["agency"].unique()
                self.assertTrue(prison_child_agency_A.name in agencies)
                self.assertTrue(prison_child_agency_B.name in agencies)

    def test_custom_child_agency_name(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = self.test_schema_objects.test_user_A
            agency = self.test_schema_objects.test_prison_super_agency
            child_agency = self.test_schema_objects.test_prison_child_agency_A
            child_agency.custom_child_agency_name = "foobar"
            session.add_all([user, agency, child_agency])
            session.commit()
            child_agency.super_agency_id = agency.id
            session.refresh(agency)
            session.refresh(user)

            spreadsheet = self.test_schema_objects.get_test_spreadsheet(
                system=schema.System.PRISONS,
                user_id=user.auth0_user_id,
                agency_id=agency.id,
            )
            session.add(spreadsheet)
            # Excel workbook will not have any warnings or errors.
            # create_excel_file is populating the agency name with the custom_child_agency_name.
            file_name = "test_custom_child_agency_name.xlsx"
            file_path = create_excel_file(
                system=schema.System.PRISONS,
                file_name=file_name,
                child_agencies=[child_agency],
            )
            content = open(  # pylint:disable=consider-using-with
                Path(file_path), "rb"
            ).read()
            file = BytesIO(content)
            ingest_result = SpreadsheetInterface.ingest_spreadsheet(
                session=session,
                file=file,
                file_name=file_name,
                spreadsheet=spreadsheet,
                auth0_user_id=user.auth0_user_id,
                agency=agency,
                metric_key_to_metric_interface={},
                upload_method=UploadMethod.BULK_UPLOAD,
            )

            spreadsheet = session.query(schema.Spreadsheet).one()
            self.assertEqual(spreadsheet.status, schema.SpreadsheetStatus.INGESTED)
            self.assertEqual(spreadsheet.ingested_by, user.auth0_user_id)

            # Confirm that datapoints were ingested for the child agency
            child_agency_datapoints = []
            for (
                datapoint_json_list
            ) in ingest_result.metric_key_to_datapoint_jsons.values():
                child_agency_datapoints += [
                    datapoint_json
                    for datapoint_json in datapoint_json_list
                    if datapoint_json["agency_name"] == child_agency.name
                ]

            self.assertTrue(
                len(
                    list(
                        itertools.chain(
                            *ingest_result.metric_key_to_datapoint_jsons.values()
                        )
                    )
                )
                > 0
            )
