#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
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
#  ============================================================================
"""Tests for the Workflows querier"""
import copy
import csv
import json
import os
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import patch

import pytest

from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    PersonRecordType,
    WorkflowsOpportunityConfig,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.workflows.schema import (
    Opportunity,
    OpportunityConfiguration,
    WorkflowsBase,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.workflows import fixtures
from recidiviz.workflows.querier.querier import WorkflowsQuerier
from recidiviz.workflows.types import OpportunityInfo, WorkflowsSystemType


def load_model_fixture(
    model: WorkflowsBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            for k, v in row.items():
                if k in {"snooze", "is_alert"} and v != "":
                    row[k] = json.loads(v)
                if v == "":
                    row[k] = None
            results.append(row)

    return results


WORK_RELEASE_INFO = OpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdCrcWorkRelease",
    system_type=WorkflowsSystemType.SUPERVISION,
    url_section="work_release_path",
    firestore_collection="work_release_collection",
)

FAST_FTRD_INFO = OpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdFastFTRD",
    system_type=WorkflowsSystemType.SUPERVISION,
    gating_feature_variant="someFeatureVariant",
    url_section="fast_path",
    firestore_collection="fast_collection",
)

SLD_INFO = OpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdSLD",
    system_type=WorkflowsSystemType.SUPERVISION,
    gating_feature_variant="someOtherFeatureVariant",
    url_section="sld_path",
    firestore_collection="sld_collection",
)


def generate_mock_config(
    opportunity_type: str, prefix: str
) -> WorkflowsOpportunityConfig:
    return WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type=opportunity_type,
        experiment_id=f"{prefix}_experiment_id",
        opportunity_record_view_name=f"{prefix}_record_1",
        task_completion_event=f"{prefix}_event",  # type: ignore
        source_filename=f"{prefix}_source",
        export_collection_name=f"{prefix}_collection",
        opportunity_type_path_str=f"{prefix}_path",
        person_record_type=PersonRecordType.CLIENT,
    )


MOCK_CONFIGS = {
    WORK_RELEASE_INFO.opportunity_type: generate_mock_config(
        WORK_RELEASE_INFO.opportunity_type, "work_release"
    ),
    FAST_FTRD_INFO.opportunity_type: generate_mock_config(
        FAST_FTRD_INFO.opportunity_type, "fast"
    ),
    SLD_INFO.opportunity_type: generate_mock_config(SLD_INFO.opportunity_type, "sld"),
}


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class TestOutliersQuerier(TestCase):
    """Implements tests for the OutliersQuerier."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        super().setUpClass()

    def setUp(self) -> None:
        self.system_for_opp_patcher = patch(
            "recidiviz.workflows.querier.querier.get_system_for_opportunity"
        )
        self.mock_system_for_opp = self.system_for_opp_patcher.start()
        self.mock_system_for_opp.return_value = WorkflowsSystemType.SUPERVISION

        self.config_for_opp_patcher = patch(
            "recidiviz.workflows.querier.querier.get_config_for_opportunity"
        )
        self.mock_config_for_opp = self.config_for_opp_patcher.start()
        self.mock_config_for_opp.side_effect = lambda type: MOCK_CONFIGS[type]

        self.database_key = SQLAlchemyDatabaseKey(SchemaType.WORKFLOWS, db_name="us_id")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            session.execute(
                """ALTER SEQUENCE opportunity_configuration_id_seq RESTART WITH 1;"""
            )

            for opportunity in load_model_fixture(Opportunity):
                session.add(Opportunity(**opportunity))
            for opportunity_config in load_model_fixture(OpportunityConfiguration):
                session.add(OpportunityConfiguration(**opportunity_config))
        super().setUp()

    def tearDown(self) -> None:
        self.system_for_opp_patcher.stop()

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )
        super().tearDownClass()

    def test_get_opportunities(self) -> None:

        actual = WorkflowsQuerier(StateCode.US_ID).get_opportunities()

        expected = [
            WORK_RELEASE_INFO,
            FAST_FTRD_INFO,
            SLD_INFO,
        ]

        self.assertCountEqual(expected, actual)

    def test_get_enabled_opportunities_no_fv(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_enabled_opportunities(
            [WorkflowsSystemType.SUPERVISION], []
        )

        expected = [WORK_RELEASE_INFO]

        self.assertCountEqual(expected, actual)

    def test_get_enabled_opportunities_one_fv(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_enabled_opportunities(
            [WorkflowsSystemType.SUPERVISION], ["someFeatureVariant"]
        )

        expected = [WORK_RELEASE_INFO, FAST_FTRD_INFO]

        self.assertCountEqual(expected, actual)

    def test_get_enabled_opportunities_both_fvs(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_enabled_opportunities(
            [WorkflowsSystemType.SUPERVISION],
            ["someFeatureVariant", "someOtherFeatureVariant"],
        )

        expected = [WORK_RELEASE_INFO, FAST_FTRD_INFO, SLD_INFO]

        self.assertCountEqual(expected, actual)

    def test_get_enabled_opportunities_wrong_system(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_enabled_opportunities(
            [WorkflowsSystemType.INCARCERATION],
            [],
        )

        self.assertListEqual([], actual)

    def test_get_enabled_opportunities_wrong_system_fv_enabled(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_enabled_opportunities(
            [WorkflowsSystemType.INCARCERATION],
            ["someFeatureVariant", "someOtherFeatureVariant"],
        )

        self.assertListEqual([], actual)

    def test_get_enabled_opportunities_filters_by_system(self) -> None:
        mock_mapping = {
            WORK_RELEASE_INFO.opportunity_type: WorkflowsSystemType.SUPERVISION,
            FAST_FTRD_INFO.opportunity_type: WorkflowsSystemType.INCARCERATION,
            SLD_INFO.opportunity_type: WorkflowsSystemType.SUPERVISION,
        }
        self.mock_system_for_opp.side_effect = lambda type: mock_mapping[type]

        actual_supervision = WorkflowsQuerier(
            StateCode.US_ID
        ).get_enabled_opportunities(
            [WorkflowsSystemType.SUPERVISION],
            ["someFeatureVariant", "someOtherFeatureVariant"],
        )

        expected_supervision = [WORK_RELEASE_INFO, SLD_INFO]

        self.assertCountEqual(expected_supervision, actual_supervision)

        actual_incarceration = WorkflowsQuerier(
            StateCode.US_ID
        ).get_enabled_opportunities(
            [WorkflowsSystemType.INCARCERATION],
            ["someFeatureVariant", "someOtherFeatureVariant"],
        )

        fast_info_copy = copy.copy(FAST_FTRD_INFO)
        fast_info_copy.system_type = WorkflowsSystemType.INCARCERATION

        expected_incarceration = [fast_info_copy]

        self.assertCountEqual(expected_incarceration, actual_incarceration)

    def test_get_active_configs_for_single_opportunity_type(self) -> None:
        actual = WorkflowsQuerier(
            StateCode.US_ID
        ).get_active_configs_for_opportunity_types(["usIdCrcWorkRelease"])

        self.assertEqual(1, len(actual))
        self.assertEqual("Approve them all", actual[0].call_to_action)
        self.assertEqual(
            "client[|s] eligible for WR", actual[0].dynamic_eligibility_text
        )

    def test_get_active_configs_for_multiple_opportunity_types(self) -> None:
        actual = WorkflowsQuerier(
            StateCode.US_ID
        ).get_active_configs_for_opportunity_types(["usIdCrcWorkRelease", "usIdSLD"])

        self.assertEqual(3, len(actual))
        self.snapshot.assert_match(actual, name="test_get_active_configs_for_multiple_opportunity_types")  # type: ignore[attr-defined]

    def test_get_top_config_for_single_type_no_fv(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCrcWorkRelease"], []
        )

        self.assertEqual(1, len(actual))
        self.snapshot.assert_match(actual, name="test_get_top_config_for_single_type_no_fv")  # type: ignore[attr-defined]

    def test_get_top_config_for_single_type_irrelevant_fvs(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCrcWorkRelease"], ["madeUpValue", "theyreAllMadeUp"]
        )

        self.assertEqual(1, len(actual))
        self.snapshot.assert_match(actual, name="test_get_top_config_for_single_type_irrelevant_fvs")  # type: ignore[attr-defined]

    def test_get_top_config_from_multiple_available(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSLD"], ["otherFeatureVariant"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(actual["usIdSLD"].description, "shorter snooze")
        self.snapshot.assert_match(actual, name="test_get_top_config_from_multiple_available")  # type: ignore[attr-defined]

    def test_get_top_config_from_multiple_available_no_relevant_fv_set(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSLD"], ["irrelevant"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(actual["usIdSLD"].description, "base config")
        self.snapshot.assert_match(actual, name="test_get_top_config_from_multiple_available_no_relevant_fv_set")  # type: ignore[attr-defined]

    def test_get_top_config_returns_config_for_each_type(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSLD", "usIdCrcWorkRelease"], ["otherFeatureVariant"]
        )

        self.assertEqual(2, len(actual))
        self.assertEqual(actual["usIdSLD"].description, "shorter snooze")
        self.assertEqual(actual["usIdCrcWorkRelease"].description, "base config")
        self.snapshot.assert_match(actual, name="test_get_top_config_returns_config_for_each_type")  # type: ignore[attr-defined]

    def test_get_top_config_ignores_inactive_configs_even_if_fv_matches(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCrcWorkRelease"], ["experimentalFeature"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(actual["usIdCrcWorkRelease"].description, "base config")
        self.snapshot.assert_match(actual, name="test_get_top_config_ignores_inactive_configs_even_if_fv_matches")  # type: ignore[attr-defined]
