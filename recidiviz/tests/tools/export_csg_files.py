#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for tools/export_csg_files.py"""
from datetime import date
from unittest import TestCase

from recidiviz.tools.export_csg_files import (
    generate_metric_export_configs,
    generate_sessions_export_configs,
    generate_state_specific_export_configs,
)


class TestExportCsgFiles(TestCase):
    """Tests for the export_csg_files script."""

    def test_generate_metric_export_configs(self) -> None:
        metric_configs = generate_metric_export_configs(
            "recidiviz-456",
            "US_XX",
            date.fromisoformat("1900-01-01"),
            "gs://output_bucket",
        )

        self.assertEqual(len(metric_configs), 2)
        self.assertIn(
            "recidiviz-456.dataflow_metrics_materialized.most_recent_supervision_population_metrics_materialized",
            metric_configs[0].query,
        )
        self.assertIn(
            "recidiviz-456.dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population",
            metric_configs[1].query,
        )

    def test_generate_sessions_export_configs(self) -> None:
        sessions_configs = generate_sessions_export_configs(
            "recidiviz-456",
            "US_XX",
            "gs://output_bucket",
            ["incarceration_sessions", "supervision_sessions"],
            {"incarceration_sessions": ["charges", "violations"]},
            {"incarceration_sessions": ["violation_dates"]},
        )

        self.assertEqual(
            len(sessions_configs),
            3,
            "There should be one more export than number of sessions",
        )
        self.assertIn(
            "recidiviz-456.state.state_person_external_id", sessions_configs[0].query
        )
        self.assertIn(
            "recidiviz-456.sessions.incarceration_sessions_materialized",
            sessions_configs[1].query,
        )
        self.assertIn(
            "recidiviz-456.sessions.supervision_sessions_materialized",
            sessions_configs[2].query,
        )
        self.assertIn(
            "SELECT * EXCEPT(charges, violations,violation_dates)",
            sessions_configs[1].query,
        )
        self.assertIn(
            'ARRAY_TO_STRING(charges, "|") AS charges',
            sessions_configs[1].query,
        )
        self.assertIn(
            'ARRAY_TO_STRING(violations, "|") AS violations',
            sessions_configs[1].query,
        )
        self.assertIn(
            'SELECT STRING_AGG(FORMAT_DATE("%Y-%m-%d", d),"|") FROM UNNEST(violation_dates) d) AS violation_dates',
            sessions_configs[1].query,
        )

    def test_generate_state_specific_export_configs_for_non_specified_state(
        self,
    ) -> None:
        exports = generate_state_specific_export_configs(
            "recidiviz-456",
            "US_XX",
            date.fromisoformat("1900-01-01"),
            "gs://export_bucket",
        )

        self.assertEqual(len(exports), 0)

    def test_generate_state_specific_export_configs_for_us_pa(self) -> None:
        exports = generate_state_specific_export_configs(
            "recidiviz-456",
            "US_PA",
            date.fromisoformat("1900-01-01"),
            "gs://export_bucket",
        )

        self.assertEqual(len(exports), 6)
        self.assertIn("1900-01-01", exports[0].query)
        self.assertIn("1900-01-01", exports[2].query)
        self.assertIn("1900-01-01", exports[3].query)
        self.assertIn("1900-01-01", exports[4].query)
        self.assertIn("1900-01-01", exports[5].query)
