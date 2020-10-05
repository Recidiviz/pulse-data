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
# ============================================================================

"""Tests for justice counts schema"""

from datetime import datetime
from unittest.case import TestCase

from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.utils import fakes


class TestSchema(TestCase):
    """Test the schema can be written to and read from successfully"""

    @classmethod
    def setUpClass(cls) -> None:
        # We must use postgres because sqlite does not support array type columns
        fakes.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        fakes.use_on_disk_postgresql_database(JusticeCountsBase)

    def tearDown(self) -> None:
        fakes.teardown_on_disk_postgresql_database(JusticeCountsBase)

    @classmethod
    def tearDownClass(cls) -> None:
        fakes.stop_and_clear_on_disk_postgresql_database()

    def testSchema_insertRows_returnedInQuery(self):
        # Create an object of each type with proper relationships
        act_session = SessionFactory.for_schema_base(JusticeCountsBase)

        source = schema.Source(name='Test Source')
        act_session.add(source)

        report = schema.Report(source=source,
                               type='Monthly Prison Report',
                               instance='September 2020',
                               publish_date=datetime(2020, 10, 1),
                               acquisition_method=schema.AcquisitionMethod.SCRAPED)
        act_session.add(report)

        table_definition = schema.ReportTableDefinition(metric=schema.Metric.POPULATION,
                                                        measurement_type=schema.MeasurementType.INSTANT,
                                                        filtered_dimensions=['global/state', 'global/population_type'],
                                                        filtered_dimension_values=['US_XX', 'prison'],
                                                        aggregated_dimensions=['global/gender'])
        act_session.add(table_definition)

        table_instance = schema.ReportTableInstance(source=source,
                                                    report=report,
                                                    table_definition=table_definition,
                                                    time_window_start=datetime(2020, 9, 30),
                                                    time_window_end=datetime(2020, 9, 30),
                                                    methodology='Some methodological description')
        act_session.add(table_instance)

        cell = schema.Cell(table_instance=table_instance,
                           aggregated_dimension_values=['female'],
                           value=123)
        act_session.add(cell)

        act_session.commit()
        act_session.close()

        # Query the cell and trace the relationships back up
        assert_session = SessionFactory.for_schema_base(JusticeCountsBase)

        [cell] = assert_session.query(schema.Cell).all()
        self.assertEqual(123, cell.value)
        table_instance = cell.table_instance
        self.assertEqual(datetime(2020, 9, 30), table_instance.time_window_start)
        table_definition = table_instance.table_definition
        self.assertEqual(schema.Metric.POPULATION, table_definition.metric)
        report = table_instance.report
        self.assertEqual('Monthly Prison Report', report.type)
        source = table_instance.source
        self.assertEqual('Test Source', source.name)

        assert_session.close()
