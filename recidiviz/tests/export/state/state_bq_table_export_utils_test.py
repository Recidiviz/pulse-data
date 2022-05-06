# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for recidiviz.persistence.export.state.state_bq_table_export_utils.py."""

import unittest

from google.cloud import bigquery

from recidiviz.export.state.state_bq_table_export_utils import (
    state_table_export_query_str,
)


class StateBQTableExportUtilsTest(unittest.TestCase):
    """Tests for recidiviz.export.state.state_bq_table_export_utils.py."""

    def test_state_table_export_query_str(self) -> None:
        query = state_table_export_query_str(
            bigquery.table.TableListItem(
                {
                    "tableReference": {
                        "projectId": "recidiviz-456",
                        "datasetId": "state",
                        "tableId": "state_person_race",
                    }
                }
            ),
            state_codes=["us_pa"],
        )

        expected_query = (
            "SELECT state_person_race.person_race_id,state_person_race.state_code,"
            "state_person_race.race,state_person_race.race_raw_text,state_person_race.person_id "
            "FROM `recidiviz-456.state.state_person_race` state_person_race "
            "WHERE state_code IN ('US_PA');"
        )
        self.assertEqual(expected_query, query)

    def test_state_table_export_query_str_association_table(self) -> None:
        query = state_table_export_query_str(
            bigquery.table.TableListItem(
                {
                    "tableReference": {
                        "projectId": "recidiviz-456",
                        "datasetId": "state",
                        "tableId": "state_charge_supervision_sentence_association",
                    }
                }
            ),
            state_codes=["us_pa"],
        )

        expected_query = (
            "SELECT state_charge_supervision_sentence_association.charge_id,"
            "state_charge_supervision_sentence_association.supervision_sentence_id,"
            "state_charge_supervision_sentence_association.state_code AS state_code "
            "FROM `recidiviz-456.state.state_charge_supervision_sentence_association` "
            "state_charge_supervision_sentence_association "
            "WHERE state_code IN ('US_PA');"
        )
        self.assertEqual(query, expected_query)
