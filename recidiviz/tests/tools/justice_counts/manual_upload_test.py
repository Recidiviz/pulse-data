# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for the Justice Counts manual_upload script."""

import datetime
import decimal
import os
import unittest
from typing import Dict, Optional

import pytest
from dateutil.relativedelta import relativedelta
from sqlalchemy import sql

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta, EnumParsingError
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.tools.justice_counts import test_utils
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.tools.postgres import local_postgres_helpers


def manifest_filepath(report_id: str):
    return os.path.join(os.path.dirname(__file__), 'fixtures', report_id, 'manifest.yaml')


class FakeType(manual_upload.Dimension, EntityEnum, metaclass=EntityEnumMeta):
    A = 'A'
    B = 'B'
    C = 'C'

    @classmethod
    def get(cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None) -> 'FakeType':
        return manual_upload.parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def dimension_identifier(cls) -> str:
        return 'global/fake_type'

    @property
    def dimension_value(self) -> str:
        return self.value

    @classmethod
    def _get_default_map(cls) -> Dict[str, 'FakeType']:
        return {
            'A': cls.A,
            'B': cls.B,
            'C': cls.C,
        }


class FakeSubtype(manual_upload.Dimension, EntityEnum, metaclass=EntityEnumMeta):
    B_1 = 'B_1'
    B_2 = 'B_2'

    @classmethod
    def get(cls, dimension_cell_value: str, enum_overrides: Optional[EnumOverrides] = None) -> 'FakeSubtype':
        return manual_upload.parse_entity_enum(cls, dimension_cell_value, enum_overrides)

    @classmethod
    def dimension_identifier(cls) -> str:
        return 'global/fake_subtype'

    @property
    def dimension_value(self) -> str:
        return self.value

    @classmethod
    def _get_default_map(cls) -> Dict[str, 'FakeSubtype']:
        return {
            'B 1': cls.B_1,
            'B 2': cls.B_2,
        }


@pytest.mark.uses_db
class ManualUploadTest(unittest.TestCase):
    """Tests that the manual upload tool works as expected"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(JusticeCountsBase)
        self.fs = FakeGCSFileSystem()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(JusticeCountsBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def test_ingestReport_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report1')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [source] = session.query(schema.Source).all()
        self.assertEqual('Colorado Department of Corrections', source.name)

        [report] = session.query(schema.Report).all()
        self.assertEqual(source, report.source)
        self.assertEqual('Dashboard Measures', report.type)
        self.assertEqual('2020-10-05', report.instance)
        self.assertEqual(datetime.date(2020, 10, 5), report.publish_date)
        self.assertEqual('https://www.colorado.gov/pacific/cdoc/departmental-reports-and-statistics', report.url)
        self.assertEqual(schema.AcquisitionMethod.MANUALLY_ENTERED, report.acquisition_method)
        self.assertEqual('Solange Knowles', report.acquired_by)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(schema.System.CORRECTIONS, table_definition.system)
        self.assertEqual(schema.MetricType.POPULATION, table_definition.metric_type)
        self.assertEqual(['global/location/state', 'metric/population/type'], table_definition.filtered_dimensions)
        self.assertEqual(['US_CO', 'PRISON'], table_definition.filtered_dimension_values)
        self.assertEqual(['global/facility/raw'], table_definition.aggregated_dimensions)

        tables = session.query(schema.ReportTableInstance).order_by(schema.ReportTableInstance.time_window_start).all()
        # Ensure all the tables have the correct source and definition
        for table in tables:
            self.assertEqual(report, table.report)
            self.assertEqual(table_definition, table.report_table_definition)
        [table1, table2, table3] = tables

        # Look at the first table in detail
        self.assertEqual(datetime.date(2020, 6, 30), table1.time_window_start)
        self.assertEqual(datetime.date(2020, 7, 1), table1.time_window_end)
        table1_cells = session.query(schema.Cell).filter(schema.Cell.report_table_instance == table1).all()
        # Sort in Python, as postgres sort is platform dependent (case sensitivity)
        table1_cells.sort(key=lambda cell: cell.aggregated_dimension_values)
        summarized_cells = [(cell.aggregated_dimension_values[0], int(cell.value)) for cell in table1_cells]
        self.assertListEqual(
            [('Awaiting Transfer - Federal Tracking', 0),
             ('Community Furlough - COVID-19', 10),
             ('Escapee In Custody', 19),
             ('Fugitive', 159),
             ('ISP/Community - Hospital', 0),
             ('Intensive Supervision Program-Inmate', 316),
             ('Private prisons', 2842),
             ('Residential Transition Inmates', 747),
             ('State prisons', 12793),
             ('TPVs Awaiting Hearing/County Jail', 555)], summarized_cells)

        # Ensure the sums of the cells for all of the tables are correct
        table1_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table1).scalar()
        self.assertEqual(17441, table1_sum)
        table2_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table2).scalar()
        self.assertEqual(17157, table2_sum)
        table3_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table3).scalar()
        self.assertEqual(16908, table3_sum)

        session.close()

    def test_ingestAndUpdateReport_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report1')))
        # This contains a new table for Jul20, with the state prisons population increased by 100.
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report1_updated')))

        # Note: If a report is published monthly, but has data for the last six months, we will always re-ingest all of
        # it each time

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        # There should still only be a single source, report, and table definition
        [source] = session.query(schema.Source).all()
        self.assertEqual('Colorado Department of Corrections', source.name)
        [report] = session.query(schema.Report).all()
        self.assertEqual(source, report.source)
        self.assertEqual('Dashboard Measures', report.type)
        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(schema.System.CORRECTIONS, table_definition.system)
        self.assertEqual(schema.MetricType.POPULATION, table_definition.metric_type)

        # There should still only be three tables, the second one with a new sum.
        tables = session.query(schema.ReportTableInstance).order_by(schema.ReportTableInstance.time_window_start).all()
        for table in tables:
            self.assertEqual(report, table.report)
            self.assertEqual(table_definition, table.report_table_definition)
        [table1, table2, table3] = tables
        table1_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table1).scalar()
        self.assertEqual(17441, table1_sum)
        table2_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table2).scalar()
        # This has increased by 100
        self.assertEqual(17257, table2_sum)
        table3_sum = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == table3).scalar()
        self.assertEqual(16908, table3_sum)

        # TODO(#4476): Add a case where a row is dropped to ensure that is reflected.

    def test_ingestMultiDimensionReport_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report2_multidimension')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [facility_totals_definition, facility_demographics_definition] = session.query(schema.ReportTableDefinition) \
            .order_by(sql.func.array_length(schema.ReportTableDefinition.aggregated_dimensions, 1)).all()
        self.assertEqual(['global/facility/raw'], facility_totals_definition.aggregated_dimensions)
        self.assertEqual(['global/facility/raw', 'global/gender/raw', 'global/race/raw'],
                         facility_demographics_definition.aggregated_dimensions)

        [facility_totals_table] = session.query(schema.ReportTableInstance) \
            .filter(schema.ReportTableInstance.report_table_definition == facility_totals_definition).all()
        [facility_demographics_table] = session.query(schema.ReportTableInstance) \
            .filter(schema.ReportTableInstance.report_table_definition == facility_demographics_definition).all()

        # Sort in Python, as postgres sort is platform dependent (case sensitivity)
        facility_demographics = sorted([
            (tuple(cell.aggregated_dimension_values), int(cell.value)) for cell in
            session.query(schema.Cell)
            .filter(schema.Cell.report_table_instance == facility_demographics_table).all()])
        # There are 180 cells in the `facility_with_demographics` csv
        self.assertEqual(180, len(facility_demographics))
        self.assertEqual((('CMCF', 'Female', 'Asian'), 0), facility_demographics[0])
        self.assertEqual((('Youthful Offender Facility', 'Male', 'White'), 2), facility_demographics[-1])

        facility_totals = {cell.aggregated_dimension_values[0]: int(cell.value) for cell in
                           session.query(schema.Cell)
                               .filter(schema.Cell.report_table_instance == facility_totals_table).all()}
        facility_totals_from_demographics = {result[0]: int(result[1]) for result in
                                             session.query(
            schema.Cell.aggregated_dimension_values[1], sql.func.sum(
                schema.Cell.value))
            .filter(schema.Cell.report_table_instance == facility_demographics_table)
            .group_by(schema.Cell.aggregated_dimension_values[1]).all()}

        EXPECTED_TOTALS = {
            'MSP': 2027,
            'CMCF': 3125,
            'SMCI': 2403,
            'County Jails (approved)': 835,
            'County Jails (unapproved)': 729,
            'Youthful Offender Facility': 13,
            'Private Prisons': 3489,
            'Regional Correctional Facilities': 3946,
            'Community Work Centers': 318,
            'Community Trusties': 0,
            'TVC': 134,
            'Transitional Housing': 12,
            'Pending File Review': 115,
            'RRP': 26,
            'Court Order': 141,
        }
        self.assertEqual(EXPECTED_TOTALS, facility_totals)
        # The report itself has an inconsistency, the totals column has 729 for unapproved county jails, but summing
        # across the demographics column yields 728. The ingest process simply persists the data provided but does not
        # attempt to resolve inconsistencies.
        EXPECTED_TOTALS['County Jails (unapproved)'] = 728
        self.assertEqual(EXPECTED_TOTALS, facility_totals_from_demographics)

        session.close()

    def test_ingestReport_dynamicDateSnapshot(self):
        self._test_ingestReport_dynamicSnapshot('report3_date_snapshot')

    def test_ingestReport_dynamicLastDayOfMonthSnapshot(self):
        self._test_ingestReport_dynamicSnapshot('report3_last_day_of_month_snapshot')

    def _test_ingestReport_dynamicSnapshot(self, report_id):
        """Ingests a report with a dynamic snapshot time window and verifies the output."""
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath(report_id)))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [source] = session.query(schema.Source).all()
        self.assertEqual('Colorado Department of Corrections', source.name)
        [report] = session.query(schema.Report).all()
        self.assertEqual(source, report.source)
        self.assertEqual('Dashboard Measures', report.type)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(schema.System.CORRECTIONS, table_definition.system)
        self.assertEqual(schema.MetricType.POPULATION, table_definition.metric_type)
        self.assertEqual([], table_definition.aggregated_dimensions)

        tables = session.query(schema.ReportTableInstance).order_by(schema.ReportTableInstance.time_window_start).all()
        self.assertEqual(13, len(tables))
        self.assertListEqual([datetime.date(2019, 9, 1) + (n + 1) * relativedelta(months=1) - relativedelta(days=1)
                              for n in range(13)],
                             [table.time_window_start for table in tables])
        for table in tables:
            self.assertEqual(report, table.report)
            self.assertEqual(table_definition, table.report_table_definition)

        results = session.query(schema.ReportTableInstance.time_window_start,
                                schema.ReportTableInstance.time_window_end,
                                schema.Cell.value) \
            .join(schema.Cell) \
            .order_by(schema.ReportTableInstance.time_window_start).all()

        EXPECTED = [
            (datetime.date(2019, 9, 30), datetime.date(2019, 10, 1), decimal.Decimal(19748)),
            (datetime.date(2019, 10, 31), datetime.date(2019, 11, 1), decimal.Decimal(19690)),
            (datetime.date(2019, 11, 30), datetime.date(2019, 12, 1), decimal.Decimal(19738)),
            (datetime.date(2019, 12, 31), datetime.date(2020, 1, 1), decimal.Decimal(19714)),
            (datetime.date(2020, 1, 31), datetime.date(2020, 2, 1), decimal.Decimal(19668)),
            (datetime.date(2020, 2, 29), datetime.date(2020, 3, 1), decimal.Decimal(19586)),
            (datetime.date(2020, 3, 31), datetime.date(2020, 4, 1), decimal.Decimal(19357)),
            (datetime.date(2020, 4, 30), datetime.date(2020, 5, 1), decimal.Decimal(18419)),
            (datetime.date(2020, 5, 31), datetime.date(2020, 6, 1), decimal.Decimal(17808)),
            (datetime.date(2020, 6, 30), datetime.date(2020, 7, 1), decimal.Decimal(17441)),
            (datetime.date(2020, 7, 31), datetime.date(2020, 8, 1), decimal.Decimal(17157)),
            (datetime.date(2020, 8, 31), datetime.date(2020, 9, 1), decimal.Decimal(16908)),
            (datetime.date(2020, 9, 30), datetime.date(2020, 10, 1), decimal.Decimal(16673)),
        ]
        self.assertEqual(EXPECTED, results)

    def test_ingestReport_dynamicCustomRange(self):
        self._test_ingestReport_dynamicDateRange('report4_custom_range')

    def test_ingestReport_dynamicMonthRange(self):
        self._test_ingestReport_dynamicDateRange('report4_month_range')

    # TODO(#4483): This doesn't actually make sense for Population, we should change this to Admission or a different
    # metric once supported.
    def _test_ingestReport_dynamicDateRange(self, report_id):
        """Ingests a report with a dynamic range time window and verifies the output."""
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath(report_id)))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [source] = session.query(schema.Source).all()
        self.assertEqual('Colorado Department of Corrections', source.name)
        [report] = session.query(schema.Report).all()
        self.assertEqual(source, report.source)
        self.assertEqual('Dashboard Measures', report.type)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(schema.System.CORRECTIONS, table_definition.system)
        self.assertEqual(schema.MetricType.POPULATION, table_definition.metric_type)
        self.assertEqual([], table_definition.aggregated_dimensions)

        tables = session.query(schema.ReportTableInstance).order_by(schema.ReportTableInstance.time_window_start).all()
        self.assertEqual(13, len(tables))
        self.assertListEqual(
            [(datetime.date(2019, 9, 1) + n * relativedelta(months=1),
              datetime.date(2019, 9, 1) + (n + 1) * relativedelta(months=1))
             for n in range(13)],
            [(table.time_window_start, table.time_window_end) for table in tables])
        for table in tables:
            self.assertEqual(report, table.report)
            self.assertEqual(table_definition, table.report_table_definition)

        results = session.query(schema.ReportTableInstance.time_window_start,
                                schema.ReportTableInstance.time_window_end,
                                schema.Cell.value) \
            .join(schema.Cell) \
            .order_by(schema.ReportTableInstance.time_window_start).all()

        EXPECTED = [
            (datetime.date(2019, 9, 1), datetime.date(2019, 10, 1), decimal.Decimal(19748)),
            (datetime.date(2019, 10, 1), datetime.date(2019, 11, 1), decimal.Decimal(19690)),
            (datetime.date(2019, 11, 1), datetime.date(2019, 12, 1), decimal.Decimal(19738)),
            (datetime.date(2019, 12, 1), datetime.date(2020, 1, 1), decimal.Decimal(19714)),
            (datetime.date(2020, 1, 1), datetime.date(2020, 2, 1), decimal.Decimal(19668)),
            (datetime.date(2020, 2, 1), datetime.date(2020, 3, 1), decimal.Decimal(19586)),
            (datetime.date(2020, 3, 1), datetime.date(2020, 4, 1), decimal.Decimal(19357)),
            (datetime.date(2020, 4, 1), datetime.date(2020, 5, 1), decimal.Decimal(18419)),
            (datetime.date(2020, 5, 1), datetime.date(2020, 6, 1), decimal.Decimal(17808)),
            (datetime.date(2020, 6, 1), datetime.date(2020, 7, 1), decimal.Decimal(17441)),
            (datetime.date(2020, 7, 1), datetime.date(2020, 8, 1), decimal.Decimal(17157)),
            (datetime.date(2020, 8, 1), datetime.date(2020, 9, 1), decimal.Decimal(16908)),
            (datetime.date(2020, 9, 1), datetime.date(2020, 10, 1), decimal.Decimal(16673)),
        ]
        self.assertEqual(EXPECTED, results)

    def test_ingestSubtypeNotStrict_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report5_subtype')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(['global/fake_subtype', 'global/fake_subtype/raw', 'global/fake_type', 'global/fake_type/raw'],
                         table_definition.aggregated_dimensions)

        cells = session.query(schema.Cell).all()
        self.assertEqual([
            ([None, 'A', 'A', 'A'], decimal.Decimal(111)),
            (['B_1', 'B_1', 'B', 'B_1'], decimal.Decimal(222)),
            (['B_2', 'B_2', 'B', 'B_2'], decimal.Decimal(333)),
            ([None, 'C', 'C', 'C'], decimal.Decimal(444)),
        ], [(cell.aggregated_dimension_values, cell.value) for cell in cells])

    def test_ingestSubtypeStrict_isNotPersisted(self):
        # Act
        with self.assertRaises(EnumParsingError):
            manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report5_subtype_fail')))

    def test_ingestAdditionalFilters_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report6_filters')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(['global/facility/raw', 'global/location/state', 'metric/population/type'],
                         table_definition.filtered_dimensions)
        self.assertEqual(['MSP', 'US_CO', 'PRISON'], table_definition.filtered_dimension_values)
        self.assertEqual(['global/gender/raw', 'global/race/raw'], table_definition.aggregated_dimensions)

        cells = session.query(schema.Cell).all()
        self.assertEqual([
            (['Male', 'Black'], decimal.Decimal(1370)),
            (['Female', 'Black'], decimal.Decimal(0)),
            (['Male', 'White'], decimal.Decimal(638)),
            (['Female', 'White'], decimal.Decimal(0)),
            (['Male', 'Hispanic'], decimal.Decimal(15)),
            (['Female', 'Hispanic'], decimal.Decimal(0)),
            (['Male', 'Native American'], decimal.Decimal(0)),
            (['Female', 'Native American'], decimal.Decimal(0)),
            (['Male', 'Asian'], decimal.Decimal(4)),
            (['Female', 'Asian'], decimal.Decimal(0)),
            (['Male', 'Data Unavailable'], decimal.Decimal(0)),
            (['Female', 'Data Unavailable'], decimal.Decimal(0)),
        ], [(cell.aggregated_dimension_values, cell.value) for cell in cells])

    def test_supportCommaNumbers_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report6_commas')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        cells = session.query(schema.Cell).all()

        assertion_values = [
            (['Male', 'Black'], decimal.Decimal(1370)),
            (['Female', 'Black'], decimal.Decimal(0)),
            (['Male', 'White'], decimal.Decimal(6384123)),
            (['Female', 'White'], decimal.Decimal(0)),
            (['Male', 'Hispanic'], decimal.Decimal(15)),
            (['Female', 'Hispanic'], decimal.Decimal(0)),
            (['Male', 'Native American'], decimal.Decimal(0)),
            (['Female', 'Native American'], decimal.Decimal(0)),
            (['Male', 'Asian'], decimal.Decimal(4)),
            (['Female', 'Asian'], decimal.Decimal(0)),
            (['Male', 'Data Unavailable'], decimal.Decimal(0)),
            (['Female', 'Data Unavailable'], decimal.Decimal(0)),
        ]
        actual_values = [(cell.aggregated_dimension_values, cell.value) for cell in cells]

        for assertion_value in assertion_values:
            self.assertIn(assertion_value, actual_values)

    def test_ingestReport_populationTypeDimension(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report7_population_types')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        cells = session.query(schema.Cell).all()
        self.assertEqual([
            (['PRISON', 'Inmates', None, 'Inmates'], decimal.Decimal(1489)),
            (['SUPERVISION', 'Parolees', 'PAROLE', 'Parolees'], decimal.Decimal(5592)),
            (['SUPERVISION', 'Probationeers', 'PROBATION', 'Probationeers'], decimal.Decimal(200784)),
        ], [(cell.aggregated_dimension_values, cell.value) for cell in cells])

    def test_ingestReport_parolePopulation(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report_parole_population')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(['global/location/state', 'metric/population/type', 'metric/supervision/type'],
                         table_definition.filtered_dimensions)
        self.assertEqual(['US_MS', 'SUPERVISION', 'PAROLE'], table_definition.filtered_dimension_values)
        self.assertEqual([], table_definition.aggregated_dimensions)

        [cell] = session.query(schema.Cell).all()
        self.assertEqual([], cell.aggregated_dimension_values)
        self.assertEqual(decimal.Decimal(5592), cell.value)

    def test_raiseError_noPopulationTypeDimensionOrMetric(self):
        # Act
        with pytest.raises(AttributeError) as exception_info:
            manual_upload.ingest(self.fs,
                                 test_utils.prepare_files(self.fs,
                                                          manifest_filepath('report8_no_population_types_fail')))

        # Assert
        assert "metric and dimension column not specified" in str(exception_info.value)

    def test_raiseError_hasBothPopulationTypeDimensionAndMetric(self):
        # Act
        with pytest.raises(AttributeError) as exception_info:
            manual_upload.ingest(self.fs,
                                 test_utils.prepare_files(self.fs,
                                                          manifest_filepath('report9_both_population_types_fail')))

        # Assert
        assert "metric and dimension column specified" in str(exception_info.value)

    def test_admissionMetric_isPersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report_admissions')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [type_definition, total_definition] = session.query(schema.ReportTableDefinition) \
            .order_by(sql.func.array_length(schema.ReportTableDefinition.aggregated_dimensions, 1)).all()
        self.assertEqual(['metric/admission/type', 'metric/admission/type/raw',
                          'metric/supervision/type', 'metric/supervision/type/raw'],
                         type_definition.aggregated_dimensions)
        self.assertEqual([], total_definition.aggregated_dimensions)

        self.assertEqual(schema.MeasurementType.DELTA, type_definition.measurement_type)
        self.assertEqual(schema.MeasurementType.DELTA, total_definition.measurement_type)

        [total_table] = session.query(schema.ReportTableInstance) \
            .filter(schema.ReportTableInstance.report_table_definition == total_definition).all()
        [type_table] = session.query(schema.ReportTableInstance) \
            .filter(schema.ReportTableInstance.report_table_definition == type_definition).all()

        raw_type_values = {tuple(cell.aggregated_dimension_values): int(cell.value) for cell in
                           session.query(schema.Cell)
                           .filter(schema.Cell.report_table_instance == type_table).all()}

        EXPECTED_TOTALS = {
            ('FROM_SUPERVISION', 'Probation Revocations', 'PROBATION', 'Probation Revocations'): 244,
            ('NEW_COMMITMENT', 'New Commitments', None, 'New Commitments'): 125,
            ('OTHER', 'Other', None, 'Other'): 27,
            ('FROM_SUPERVISION', 'Parole Re-Admissions', 'PAROLE', 'Parole Re-Admissions'): 128,
            ('OTHER', 'Returned Escapees', None, 'Returned Escapees'): 53,
            ('NEW_COMMITMENT', 'Split Sentence', None, 'Split Sentence'): 166,
        }
        self.assertEqual(EXPECTED_TOTALS, raw_type_values)

        type_values = {
            (result[0], result[1]): int(result[2]) for result in
            session.query(schema.Cell.aggregated_dimension_values[1], schema.Cell.aggregated_dimension_values[3],
                          sql.func.sum(schema.Cell.value))
            .filter(schema.Cell.report_table_instance == type_table)
            .group_by(schema.Cell.aggregated_dimension_values[1], schema.Cell.aggregated_dimension_values[3])
            .all()}
        EXPECTED_TOTALS = {
            ('FROM_SUPERVISION', 'PAROLE'): 128,
            ('FROM_SUPERVISION', 'PROBATION'): 244,
            ('NEW_COMMITMENT', None): 291,
            ('OTHER', None): 80,
        }
        self.assertEqual(EXPECTED_TOTALS, type_values)

        [[total_from_types]] = session.query(sql.func.sum(schema.Cell.value)) \
            .filter(schema.Cell.report_table_instance == type_table).group_by().all()
        self.assertEqual(decimal.Decimal(743), total_from_types)
        [[total]] = session.query(schema.Cell.value).filter(schema.Cell.report_table_instance == total_table).all()
        self.assertEqual(decimal.Decimal(743), total)

        session.close()

    def test_reincarcerationsWithViolationType_arePersisted(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report8_reincarcerations')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(['global/location/state', 'metric/admission/type', 'metric/supervision/type'],
                         table_definition.filtered_dimensions)
        self.assertEqual(['US_MI', 'FROM_SUPERVISION', 'PAROLE'], table_definition.filtered_dimension_values)
        self.assertEqual(['global/gender/raw', 'metric/supervision_violation/type',
                          'metric/supervision_violation/type/raw'], table_definition.aggregated_dimensions)

        self.assertEqual(schema.MeasurementType.DELTA, table_definition.measurement_type)

        violation_type_values = {
            result[0]: int(result[1]) for result in
            session.query(schema.Cell.aggregated_dimension_values[2], sql.func.sum(schema.Cell.value))
            .group_by(schema.Cell.aggregated_dimension_values[2])
            .all()}
        EXPECTED_TOTALS = {
            'TECHNICAL': 19_926,
            'NEW_CRIME': 13_625,
        }
        self.assertEqual(EXPECTED_TOTALS, violation_type_values)
        session.close()

    def test_ingestReport_fixed_range_month(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report6_fixed_month_range')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        report_table = session.query(schema.ReportTableInstance).all()
        self.assertEqual([
            (datetime.date(2020, 9, 1), datetime.date(2020, 10, 1)),
        ], [(row.time_window_start, row.time_window_end) for row in report_table])

    def test_ingestReport_fixed_range_year(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report6_fixed_year_range')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        report_table = session.query(schema.ReportTableInstance).all()
        self.assertEqual([
            (datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)),
        ], [(row.time_window_start, row.time_window_end) for row in report_table])

    def test_ingestReport_synthetic_column(self):
        # Act
        manual_upload.ingest(self.fs, test_utils.prepare_files(self.fs, manifest_filepath('report_synthetic_column')))

        # Assert
        session = SessionFactory.for_schema_base(JusticeCountsBase)

        cells = session.query(schema.Cell).all()
        self.assertEqual([
            (['PRISON', 'Inmates', 'test1'], decimal.Decimal(1489)),
            (['SUPERVISION', 'Parolees', 'test2'], decimal.Decimal(5592)),
            (['SUPERVISION', 'Probationeers', 'test3'], decimal.Decimal(200784)),
        ], [(cell.aggregated_dimension_values, cell.value) for cell in cells])

        [table_definition] = session.query(schema.ReportTableDefinition).all()
        self.assertEqual(['metric/population/type', 'metric/population/type/raw',
                          'source/colorado_department_of_corrections/population_subtype/raw'],
                         table_definition.aggregated_dimensions)
