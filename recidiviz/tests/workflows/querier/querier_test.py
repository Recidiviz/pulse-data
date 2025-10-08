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
import datetime
import json
import os
from typing import Any, Dict, List, Optional
from unittest import TestCase
from unittest.mock import patch

import pytest

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    PersonRecordType,
    WorkflowsOpportunityConfig,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.workflows.schema import (
    Opportunity,
    OpportunityConfiguration,
    OpportunityStatus,
    WorkflowsBase,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.tools.workflows import fixtures
from recidiviz.workflows.querier.querier import WorkflowsQuerier
from recidiviz.workflows.types import FullOpportunityInfo, WorkflowsSystemType


def load_model_fixture(
    model: WorkflowsBase, filename: Optional[str] = None
) -> List[Dict]:
    """Loads the model fixtures from CSV"""
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            for k, v in row.items():
                if (
                    k
                    in {
                        "snooze",
                        "is_alert",
                        "priority",
                        "hide_denial_revert",
                        "eligible_criteria_copy",
                        "ineligible_criteria_copy",
                        "strictly_ineligible_criteria_copy",
                        "sidebar_components",
                        "denial_reasons",
                        "tab_groups",
                        "compare_by",
                        "notifications",
                        "empty_tab_copy",
                        "tab_preface_copy",
                        "subcategory_headings",
                        "subcategory_orderings",
                        "mark_submitted_options_by_tab",
                        "non_oms_criteria",
                    }
                    and v != ""
                ):
                    if k == "sidebar_components":
                        # postgres expects arrays to be wrapped in braces, so we need to
                        # replace them with JSON-spec brackets
                        v = f"[{v[1:-1]}]"
                    row[k] = json.loads(v)
                if v == "":
                    row[k] = None
            results.append(row)

    return results


def make_add_config_arguments(
    opportunity_type: str, feature_variant: Optional[str] = None
) -> Dict[str, Any]:
    return {
        "opportunity_type": opportunity_type,
        "created_by": "Maria",
        "created_at": datetime.datetime(2024, 5, 12, tzinfo=datetime.timezone.utc),
        "variant_description": "variant_description",
        "revision_description": "revision_description",
        "display_name": "display_name",
        "methodology_url": "methodology_url",
        "initial_header": "initial_header",
        "denial_reasons": {},
        "eligible_criteria_copy": [
            {
                "key": "TESTING_CRITERIA_1",
                "text": "test text",
                "tooltip": "test tooltip",
            },
            {"key": "TESTING_CRITERIA_2", "text": "more text"},
        ],
        "ineligible_criteria_copy": [
            {
                "key": "TESTING_ALMOST_ELIGIBLE_CRITERIA",
                "text": "test text",
                "tooltip": "test tooltip",
            },
        ],
        "strictly_ineligible_criteria_copy": [
            {
                "key": "TESTING_STRICTLY_INELIGIBLE_CRITERIA",
                "text": "strictly ineligible text",
                "tooltip": "strictly ineligible tooltip",
            }
        ],
        "dynamic_eligibility_text": "dynamic_eligibility_text",
        "call_to_action": "call_to_action",
        "subheading": "subheading",
        "snooze": {},
        "feature_variant": feature_variant,
        "is_alert": False,
        "priority": "NORMAL",
        "denial_text": "denial_text",
        "sidebar_components": ["sidebarComponent"],
        "eligibility_date_text": "eligibility date",
        "hide_denial_revert": True,
        "tooltip_eligibility_text": "tooltip",
        "tab_groups": None,
        "compare_by": None,
        "notifications": [],
        "zero_grants_tooltip": "zero grants tooltip",
        "denied_tab_title": "Marked Ineligible",
        "denial_adjective": "Ineligible",
        "denial_noun": "Ineligibility",
        "supports_submitted": True,
        "supports_ineligible": False,
        "submitted_tab_title": "Submitted",
        "empty_tab_copy": [{"tab": "Eligible Now", "text": "No people are eligible"}],
        "tab_preface_copy": [{"tab": "Pending", "text": "Pending people"}],
        "subcategory_headings": [
            {"subcategory": "PENDING_1", "text": "Pending type 1"},
            {"subcategory": "PENDING_2", "text": "Pending type 2"},
        ],
        "subcategory_orderings": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2"]},
            {"tab": "Eligible Now", "texts": ["ELIGIBLE_1", "ELIGIBLE_2"]},
        ],
        "mark_submitted_options_by_tab": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2", "PENDING_3"]},
            {"tab": "Eligible Now", "texts": ["PENDING_1"]},
        ],
        "oms_criteria_header": "Validated by data from OMS",
        "non_oms_criteria_header": "Requirements to check",
        "non_oms_criteria": [
            {"text": "test text"},
            {"text": "test criteria with tooltip", "tooltip": "test tooltip"},
        ],
        "highlight_cases_on_homepage": False,
        "highlighted_case_cta_copy": "Opportunity name",
        "overdue_opportunity_callout_copy": "overdue for opportunity",
        "snooze_companion_opportunity_types": ["usNdOppType1", "usNdOppType2"],
        "case_notes_title": "Case notes title",
    }


WORK_RELEASE_INFO = FullOpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdCRCWorkRelease",
    system_type=WorkflowsSystemType.SUPERVISION,
    url_section="work_release_path",
    firestore_collection="work_release_collection",
    experiment_id="work_release_experiment_id",
    completion_event="work_release_event",
    homepage_position=1,
    last_updated_at=datetime.datetime(2023, 4, 26, tzinfo=datetime.timezone.utc),
    last_updated_by="tony@recidiviz.org",
)

FAST_FTRD_INFO = FullOpportunityInfo(
    state_code="US_ID",
    opportunity_type="pastFTRD",
    system_type=WorkflowsSystemType.SUPERVISION,
    gating_feature_variant="someFeatureVariant",
    url_section="fast_path",
    firestore_collection="fast_collection",
    experiment_id="fast_experiment_id",
    completion_event="fast_event",
    homepage_position=2,
    last_updated_at=datetime.datetime(2023, 4, 27, tzinfo=datetime.timezone.utc),
    last_updated_by="daniel@recidiviz.org",
)

SLD_INFO = FullOpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdSupervisionLevelDowngrade",
    system_type=WorkflowsSystemType.SUPERVISION,
    gating_feature_variant="someOtherFeatureVariant",
    url_section="sld_path",
    firestore_collection="sld_collection",
    experiment_id="sld_experiment_id",
    completion_event="sld_event",
    homepage_position=3,
    last_updated_at=datetime.datetime(2023, 4, 28, tzinfo=datetime.timezone.utc),
    last_updated_by="ken@recidiviz.org",
)

UNPROVISIONED_INFO = FullOpportunityInfo(
    state_code="US_ID",
    opportunity_type="usIdNewOpp",
    system_type=WorkflowsSystemType.SUPERVISION,
    gating_feature_variant=None,
    url_section="new_opp_path",
    firestore_collection="new_opp_collection",
    experiment_id="new_opp_experiment_id",
    completion_event="new_opp_event",
    homepage_position=None,
    last_updated_at=None,
    last_updated_by=None,
)


def generate_mock_config(
    opportunity_type: str, prefix: str
) -> WorkflowsOpportunityConfig:
    return WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type=opportunity_type,
        experiment_id=f"{prefix}_experiment_id",
        view_builder=SimpleBigQueryViewBuilder(
            dataset_id="mock_dataset",
            view_id=f"{prefix}_record",
            view_query_template="SELECT 1",
            description="mock description",
        ),
        task_completion_event=f"{prefix}_event",  # type: ignore
        export_collection_name=f"{prefix}_collection",
        opportunity_type_path_str=f"{prefix}_path",
        person_record_type=PersonRecordType.CLIENT,
    )


MOCK_CONFIGS = [
    generate_mock_config(WORK_RELEASE_INFO.opportunity_type, "work_release"),
    generate_mock_config(FAST_FTRD_INFO.opportunity_type, "fast"),
    generate_mock_config(SLD_INFO.opportunity_type, "sld"),
    generate_mock_config(UNPROVISIONED_INFO.opportunity_type, "new_opp"),
]


def lookup_type(opps: List[FullOpportunityInfo], opp_type: str) -> FullOpportunityInfo:
    return next(opp for opp in opps if opp.opportunity_type == opp_type)


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class TestWorkflowsQuerier(TestCase):
    """Implements tests for the WorkflowsQuerier."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        super().setUpClass()

    def setUp(self) -> None:
        self.system_for_config_patcher = patch(
            "recidiviz.workflows.querier.querier.get_system_for_config"
        )
        self.mock_system_for_config = self.system_for_config_patcher.start()
        self.mock_system_for_config.return_value = WorkflowsSystemType.SUPERVISION

        self.get_configs_patcher = patch(
            "recidiviz.workflows.querier.querier.get_configs",
        )
        self.mock_get_configs = self.get_configs_patcher.start()
        self.mock_get_configs.return_value = MOCK_CONFIGS

        self.database_key = SQLAlchemyDatabaseKey(SchemaType.WORKFLOWS, db_name="us_id")
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

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
        self.system_for_config_patcher.stop()
        self.get_configs_patcher.stop()

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )
        super().tearDownClass()

    def test_get_opportunities(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_opportunities()

        expected = [WORK_RELEASE_INFO, FAST_FTRD_INFO, SLD_INFO, UNPROVISIONED_INFO]

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
            UNPROVISIONED_INFO.opportunity_type: WorkflowsSystemType.SUPERVISION,
        }
        self.mock_system_for_config.side_effect = lambda config: mock_mapping[
            config.opportunity_type
        ]

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
        ).get_active_configs_for_opportunity_types(["usIdCRCWorkRelease"])

        self.assertEqual(1, len(actual))
        self.assertEqual("Approve them all", actual[0].call_to_action)
        self.assertEqual(
            "client[|s] eligible for WR", actual[0].dynamic_eligibility_text
        )

    def test_get_active_configs_for_multiple_opportunity_types(self) -> None:
        actual = WorkflowsQuerier(
            StateCode.US_ID
        ).get_active_configs_for_opportunity_types(
            ["usIdCRCWorkRelease", "usIdSupervisionLevelDowngrade"]
        )

        self.assertEqual(3, len(actual))

    def test_get_top_config_for_single_type_no_fv(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCRCWorkRelease"], []
        )

        self.assertEqual(1, len(actual))
        self.snapshot.assert_match(actual, name="test_get_top_config_for_single_type_no_fv")  # type: ignore[attr-defined]

    def test_get_top_config_for_single_type_irrelevant_fvs(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCRCWorkRelease"], ["madeUpValue", "theyreAllMadeUp"]
        )

        self.assertEqual(1, len(actual))
        self.snapshot.assert_match(actual, name="test_get_top_config_for_single_type_irrelevant_fvs")  # type: ignore[attr-defined]

    def test_get_top_config_from_multiple_available(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSupervisionLevelDowngrade"], ["otherFeatureVariant"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(
            actual["usIdSupervisionLevelDowngrade"].call_to_action,
            "Downgrade all of them",
        )
        self.snapshot.assert_match(actual, name="test_get_top_config_from_multiple_available")  # type: ignore[attr-defined]

    def test_get_top_config_from_multiple_available_no_relevant_fv_set(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSupervisionLevelDowngrade"], ["irrelevant"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(
            actual["usIdSupervisionLevelDowngrade"].call_to_action,
            "Downgrades all around",
        )
        self.snapshot.assert_match(actual, name="test_get_top_config_from_multiple_available_no_relevant_fv_set")  # type: ignore[attr-defined]

    def test_get_top_config_returns_config_for_each_type(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdSupervisionLevelDowngrade", "usIdCRCWorkRelease"],
            ["otherFeatureVariant"],
        )

        self.assertEqual(2, len(actual))
        self.assertEqual(
            actual["usIdSupervisionLevelDowngrade"].call_to_action,
            "Downgrade all of them",
        )
        self.assertEqual(
            actual["usIdCRCWorkRelease"].call_to_action, "Approve them all"
        )
        self.snapshot.assert_match(actual, name="test_get_top_config_returns_config_for_each_type")  # type: ignore[attr-defined]

    def test_get_top_config_ignores_inactive_configs_even_if_fv_matches(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_top_config_for_opportunity_types(
            ["usIdCRCWorkRelease"], ["experimentalFeature"]
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(
            actual["usIdCRCWorkRelease"].call_to_action, "Approve them all"
        )
        self.snapshot.assert_match(actual, name="test_get_top_config_ignores_inactive_configs_even_if_fv_matches")  # type: ignore[attr-defined]

    def test_get_configs_for_type_returns_newest_first(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdSupervisionLevelDowngrade"
        )

        self.assertEqual(2, len(actual))
        self.assertGreater(actual[0].created_at, actual[1].created_at)
        self.snapshot.assert_match(actual, name="test_get_configs_for_type_returns_newest_first")  # type: ignore[attr-defined]

    def test_get_configs_for_type_respects_offset(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdSupervisionLevelDowngrade", offset=1
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(
            datetime.datetime(2023, 5, 8, tzinfo=datetime.timezone.utc),
            actual[0].created_at,
        )

    def test_get_configs_for_type_respects_limit(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdSupervisionLevelDowngrade", limit=1
        )

        self.assertEqual(1, len(actual))
        self.assertEqual(
            datetime.datetime(2023, 5, 17, tzinfo=datetime.timezone.utc),
            actual[0].created_at,
        )

    def test_get_configs_for_type_respects_status(self) -> None:
        actual_active = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdCRCWorkRelease", status=OpportunityStatus.ACTIVE
        )

        self.assertEqual(1, len(actual_active))
        self.assertEqual(
            datetime.datetime(2023, 5, 3, tzinfo=datetime.timezone.utc),
            actual_active[0].created_at,
        )
        self.assertEqual("base config", actual_active[0].variant_description)

        actual_inactive = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdCRCWorkRelease", status=OpportunityStatus.INACTIVE
        )

        self.assertEqual(2, len(actual_inactive))
        self.assertEqual("experimental config", actual_inactive[0].variant_description)
        self.assertEqual("base config", actual_inactive[1].variant_description)

    def test_get_configs_for_type_respects_offset_and_status(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_configs_for_type(
            "usIdCRCWorkRelease", offset=1, status=OpportunityStatus.INACTIVE
        )

        self.assertEqual(1, len(actual))
        self.assertEqual("base config", actual[0].variant_description)

    def test_get_config_for_id(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_config_for_id(
            "usIdSupervisionLevelDowngrade", 3
        )

        self.assertIsNotNone(actual)
        self.assertEqual(datetime.datetime(2023, 5, 8, tzinfo=datetime.timezone.utc), actual.created_at)  # type: ignore

    def test_get_config_for_id_returns_none_for_nonexistent_id(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_config_for_id(
            "usIdSupervisionLevelDowngrade", 333
        )

        self.assertIsNone(actual)

    def test_get_config_for_id_returns_none_for_mismatched_id(self) -> None:
        actual = WorkflowsQuerier(StateCode.US_ID).get_config_for_id(
            "usIdCrcWorkRelease", 3
        )

        self.assertIsNone(actual)

    def test_update_opportunity_creates_new_opportunity(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)

        opp = lookup_type(querier.get_opportunities(), "usIdNewOpp")

        # Make sure we're actually unprovisioned
        self.assertEqual(None, opp.last_updated_by)

        querier.update_opportunity(
            opportunity_type="usIdNewOpp",
            updated_by="testerZZZ",
            updated_at=datetime.datetime(2024, 11, 12, tzinfo=datetime.timezone.utc),
            gating_feature_variant="test_fv",
            homepage_position=423,
        )

        opp = lookup_type(querier.get_opportunities(), "usIdNewOpp")
        self.assertEqual("testerZZZ", opp.last_updated_by)
        self.assertEqual(
            datetime.datetime(2024, 11, 12, tzinfo=datetime.timezone.utc),
            opp.last_updated_at,
        )
        self.assertEqual("test_fv", opp.gating_feature_variant)
        self.assertEqual(423, opp.homepage_position)

    def test_update_opportunity_updates_existing_opportunity(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)

        querier.update_opportunity(
            opportunity_type="usIdSupervisionLevelDowngrade",
            updated_by="testerZZZ",
            updated_at=datetime.datetime(2024, 11, 12, tzinfo=datetime.timezone.utc),
            gating_feature_variant=None,
            homepage_position=423,
        )

        opp = lookup_type(querier.get_opportunities(), "usIdSupervisionLevelDowngrade")
        self.assertEqual("testerZZZ", opp.last_updated_by)
        self.assertEqual(
            datetime.datetime(2024, 11, 12, tzinfo=datetime.timezone.utc),
            opp.last_updated_at,
        )
        self.assertEqual(None, opp.gating_feature_variant)
        self.assertEqual(423, opp.homepage_position)

    def test_update_opportunity_fails_on_bad_opportunity_type(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)

        with pytest.raises(ValueError):
            querier.update_opportunity(
                opportunity_type="badType",
                updated_by="testerZZZ",
                updated_at=datetime.datetime(
                    2024, 11, 12, tzinfo=datetime.timezone.utc
                ),
                gating_feature_variant=None,
                homepage_position=423,
            )

    def test_add_config_increments_id(self) -> None:
        new_id = WorkflowsQuerier(StateCode.US_ID).add_config(**make_add_config_arguments("usIdSupervisionLevelDowngrade"))  # type: ignore
        self.assertEqual(6, new_id)

    def test_add_config_deactivates_existing_config(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        querier.add_config(**make_add_config_arguments("usIdSupervisionLevelDowngrade"))  # type: ignore

        old_config = querier.get_config_for_id("usIdSupervisionLevelDowngrade", 3)

        self.assertEqual(OpportunityStatus.INACTIVE, old_config.status)  # type: ignore

        active = querier.get_configs_for_type(
            "usIdSupervisionLevelDowngrade", status=OpportunityStatus.ACTIVE
        )
        self.assertEqual(2, len(active))

    def test_add_config_does_not_deactivate_existing_configs_with_different_gating(
        self,
    ) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        querier.add_config(
            **make_add_config_arguments(
                "usIdSupervisionLevelDowngrade",
                feature_variant="someNewVariant",
            )
        )  # type: ignore

        active = querier.get_configs_for_type(
            "usIdSupervisionLevelDowngrade", status=OpportunityStatus.ACTIVE
        )
        self.assertEqual(3, len(active))

    def test_add_config_fails_on_bad_opportunity_type(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        with pytest.raises(Exception):
            querier.add_config(**make_add_config_arguments("badType"))  # type: ignore

    def test_deactivate_config(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        config_id = querier.add_config(
            **make_add_config_arguments(
                "usIdSupervisionLevelDowngrade",
                feature_variant="someNewVariant",
            )
        )

        querier.deactivate_config("usIdSupervisionLevelDowngrade", config_id)

        actual = querier.get_config_for_id("usIdSupervisionLevelDowngrade", config_id)

        self.assertEqual(actual.status, OpportunityStatus.INACTIVE)  # type: ignore

    def test_deactivate_config_nonexistent(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        with pytest.raises(ValueError, match="Config does not exist"):
            querier.deactivate_config("usIdSupervisionLevelDowngrade", 100)

    def test_deactivate_config_already_inactive(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        config_id = querier.add_config(
            **make_add_config_arguments(
                "usIdSupervisionLevelDowngrade",
                feature_variant="someNewVariant",
            )
        )

        # deactivate it once
        querier.deactivate_config("usIdSupervisionLevelDowngrade", config_id)

        # attempt to deactivate a second time
        with pytest.raises(ValueError, match="Config is already inactive"):
            querier.deactivate_config("usIdSupervisionLevelDowngrade", config_id)

    def test_deactivate_config_no_feature_variant(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)

        with pytest.raises(ValueError, match="Cannot deactivate default config"):
            querier.deactivate_config("usIdSupervisionLevelDowngrade", 3)

    def test_activate_config(self) -> None:
        # create a config
        querier = WorkflowsQuerier(StateCode.US_ID)
        config_fields = make_add_config_arguments(
            "usIdSupervisionLevelDowngrade",
            feature_variant="someNewVariant",
        )
        config_id = querier.add_config(**config_fields)

        # deactivate the config and ensure it is deactivated
        querier.deactivate_config("usIdSupervisionLevelDowngrade", config_id)
        deactivated = querier.get_config_for_id(
            "usIdSupervisionLevelDowngrade", config_id
        )
        self.assertEqual(deactivated.status, OpportunityStatus.INACTIVE)  # type: ignore

        # activate the config
        new_active_id = querier.activate_config(
            "usIdSupervisionLevelDowngrade", config_id
        )
        actual = querier.get_config_for_id(
            "usIdSupervisionLevelDowngrade", new_active_id
        )

        # ensure a new activated config was created
        self.assertNotEqual(config_id, new_active_id)
        self.assertEqual(actual.status, OpportunityStatus.ACTIVE)  # type: ignore

        # assert all the other activated config data is identical to the deactivated config
        for field in actual.__dict__:
            if field in ("status", "id"):
                continue
            self.assertEqual(getattr(actual, field), getattr(deactivated, field))

    def test_activate_config_nonexistent(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        with pytest.raises(ValueError, match="Config does not exist"):
            querier.activate_config("usIdSupervisionLevelDowngrade", 100)

    def test_activate_config_already_active(self) -> None:
        querier = WorkflowsQuerier(StateCode.US_ID)
        with pytest.raises(ValueError, match="Config is already active"):
            querier.activate_config("usIdSupervisionLevelDowngrade", 3)
