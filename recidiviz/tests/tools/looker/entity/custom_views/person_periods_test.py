# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the PersonPeriodsLookMLView class."""
import unittest

from google.cloud import bigquery

from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.persistence.entity.state import entities as state_entites
from recidiviz.persistence.entity.state import normalized_entities as normalized_entites
from recidiviz.tools.looker.entity.custom_views.person_periods import (
    PersonPeriodsJoinProvider,
    PersonPeriodsLookMLViewBuilder,
)


class TestPersonPeriodsJoinProvider(unittest.TestCase):
    """Tests for the PersonPeriodsJoinProvider class."""

    def test_get_state_join_relationship(self) -> None:
        expected_join = """state_person_periods {
    sql_on: ${state_person.person_id} = ${state_person_periods.person_id};;
    relationship: one_to_many
  }
"""
        join_provider = PersonPeriodsJoinProvider(dataset_id=STATE_BASE_DATASET)
        person_join = join_provider.get_join_relationship(
            entity_cls=state_entites.StatePerson
        )
        staff_join = join_provider.get_join_relationship(
            entity_cls=state_entites.StateStaff
        )

        # make mypy happy
        assert person_join is not None
        self.assertEqual(expected_join, person_join.value_text)
        self.assertIsNone(staff_join)

    def test_get_normalized_state_join_relationship(self) -> None:
        expected_join = """normalized_state_person_periods {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_periods.person_id};;
    relationship: one_to_many
  }
"""
        join_provider = PersonPeriodsJoinProvider(dataset_id=NORMALIZED_STATE_DATASET)
        person_join = join_provider.get_join_relationship(
            entity_cls=normalized_entites.NormalizedStatePerson
        )
        staff_join = join_provider.get_join_relationship(
            entity_cls=normalized_entites.NormalizedStateStaff
        )

        # make mypy happy
        assert person_join is not None
        self.assertEqual(expected_join, person_join.value_text)
        self.assertIsNone(staff_join)


class TestPersonPeriodsLookMLView(unittest.TestCase):
    """Tests for the PersonPeriodsLookMLView class."""

    def test_build(self) -> None:
        expected_view = """view: state_person_periods {
  derived_table: {
    sql: 
    SELECT
        external_id AS period_id,
	'incarceration_period' AS period_type,
	person_id AS person_id,
	admission_date AS start_date,
	admission_reason AS start_reason,
	IFNULL(release_date, CURRENT_DATE('US/Eastern')) AS end_date,
	release_reason AS end_reason
      FROM state.state_incarceration_period
      UNION ALL
      SELECT
        external_id AS period_id,
	'supervision_period' AS period_type,
	person_id AS person_id,
	start_date AS start_date,
	admission_reason AS start_reason,
	IFNULL(termination_date, CURRENT_DATE('US/Eastern')) AS end_date,
	termination_reason AS end_reason
      FROM state.state_supervision_period
 ;;
  }

  dimension_group: end {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.end_date ;;
  }

  dimension: end_reason {
    type: string
    sql: ${TABLE}.end_reason ;;
  }

  dimension: period_id {
    type: string
    sql: ${TABLE}.period_id ;;
  }

  dimension: period_type {
    type: string
    sql: ${TABLE}.period_type ;;
  }

  dimension: person_id {
    type: string
    sql: ${TABLE}.person_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.period_id, "_", ${TABLE}.period_type) ;;
  }

  dimension_group: start {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.start_date ;;
  }

  dimension: start_reason {
    type: string
    sql: ${TABLE}.start_reason ;;
  }
}"""
        bq_schema = {
            "state_incarceration_period": [
                bigquery.SchemaField("external_id", "STRING"),
                bigquery.SchemaField("person_id", "STRING"),
                bigquery.SchemaField("admission_date", "DATE"),
                bigquery.SchemaField("admission_reason", "STRING"),
                bigquery.SchemaField("release_date", "DATE"),
                bigquery.SchemaField("release_reason", "STRING"),
            ],
            "state_supervision_period": [
                bigquery.SchemaField("external_id", "STRING"),
                bigquery.SchemaField("person_id", "STRING"),
                bigquery.SchemaField("start_date", "DATE"),
                bigquery.SchemaField("admission_reason", "STRING"),
                bigquery.SchemaField("termination_date", "DATE"),
                bigquery.SchemaField("termination_reason", "STRING"),
            ],
        }

        view = PersonPeriodsLookMLViewBuilder.from_schema(
            dataset_id="state", bq_schema=bq_schema
        ).build()

        self.assertEqual(expected_view, view.build())

    def test_referenced_field_doesnt_exist(self) -> None:
        bq_schema = {
            "state_incarceration_period": [
                bigquery.SchemaField("external_id", "STRING"),
                bigquery.SchemaField("person_id", "STRING"),
                bigquery.SchemaField("admission_date", "DATE"),
                bigquery.SchemaField("admission_reason", "STRING"),
                bigquery.SchemaField("release_date", "DATE"),
                # bigquery.SchemaField("release_reason", "STRING"),
            ],
            "state_supervision_period": [
                bigquery.SchemaField("external_id", "STRING"),
                bigquery.SchemaField("person_id", "STRING"),
                bigquery.SchemaField("start_date", "DATE"),
                bigquery.SchemaField("admission_reason", "STRING"),
                bigquery.SchemaField("termination_date", "DATE"),
                bigquery.SchemaField("termination_reason", "STRING"),
            ],
        }
        with self.assertRaisesRegex(
            ValueError,
            r"Referenced field \[release_reason\] not found in schema fields"
            r" \['external_id', 'person_id', 'admission_date', 'admission_reason', 'release_date'\]",
        ):
            PersonPeriodsLookMLViewBuilder.from_schema(
                dataset_id="state", bq_schema=bq_schema
            ).build()
