# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the source table update group -> DAG resolver."""

import unittest

from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.source_tables.source_table_update_group_dag import (
    _DAG_BY_UPDATE_GROUP,
    source_table_update_group_for_dag_id,
)
from recidiviz.utils.airflow_dag import AirflowDag

_PROJECT_ID = "recidiviz-testing"


class SourceTableUpdateGroupDagTest(unittest.TestCase):
    """Tests for the source table update group -> DAG resolver."""

    def test_every_update_group_owned_by_a_dag(self) -> None:
        self.assertEqual(set(SourceTableUpdateGroup), set(_DAG_BY_UPDATE_GROUP))

    def test_each_dag_owns_at_most_one_group(self) -> None:
        owning_dags = list(_DAG_BY_UPDATE_GROUP.values())
        self.assertEqual(len(owning_dags), len(set(owning_dags)))

    def test_dag_id_that_owns_no_group_raises(self) -> None:
        monitoring_dag_id = AirflowDag.MONITORING.dag_id(_PROJECT_ID)
        with self.assertRaisesRegex(
            ValueError,
            rf"No SourceTableUpdateGroup is owned by DAG \[{monitoring_dag_id}\]",
        ):
            source_table_update_group_for_dag_id(
                monitoring_dag_id, project_id=_PROJECT_ID
            )

    def test_unknown_dag_id_raises(self) -> None:
        fake_dag_id = "not_a_real_dag"
        with self.assertRaisesRegex(
            ValueError,
            rf"No AirflowDag matches dag_id \[{fake_dag_id}\] for project \[{_PROJECT_ID}\].",
        ):
            source_table_update_group_for_dag_id(fake_dag_id, project_id=_PROJECT_ID)
