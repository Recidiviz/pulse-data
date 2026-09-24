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
"""Tests for recidiviz/tools/analyst/notion_linear_roadmap_snapshot.py"""
import unittest

import pandas as pd

from recidiviz.tools.analyst.notion_linear_roadmap_snapshot import (
    LINEAR_PROJECT_ID_COLUMN,
    LINEAR_PROJECT_NAME_COLUMN,
    LINEAR_PROJECT_SLUG_ID_COLUMN,
    LINEAR_PROJECT_STATE_COLUMN,
    LINEAR_PROJECT_URL_COLUMN,
    LaunchStage,
    _dedupe_column_names,
    _extract_slug_id,
    _to_bq_safe_column_name,
    build_slug_id_index,
    classify_milestone_stage,
    derive_launch_stage_dates,
    get_roadmap_snapshot_source_table_config,
    join_notion_and_linear,
    pick_stage_milestone,
)


class ColumnRenamingTest(unittest.TestCase):
    """Tests for _to_bq_safe_column_name() and _dedupe_column_names()."""

    def test_lowercases_and_collapses_non_alphanumeric_runs(self) -> None:
        self.assertEqual(
            _to_bq_safe_column_name("Contract to Data (weeks)"),
            "contract_to_data_weeks",
        )

    def test_strips_leading_and_trailing_symbols(self) -> None:
        self.assertEqual(
            _to_bq_safe_column_name("*Global Prio Flag"), "global_prio_flag"
        )

    def test_raises_on_no_usable_characters(self) -> None:
        with self.assertRaises(ValueError):
            _to_bq_safe_column_name("***")

    def test_dedupes_collisions(self) -> None:
        self.assertEqual(
            _dedupe_column_names(["state", "state", "state"]),
            ["state", "state_2", "state_3"],
        )


class ExtractSlugIdTest(unittest.TestCase):
    """Tests for _extract_slug_id()."""

    def test_extracts_slug_id_from_overview_url(self) -> None:
        self.assertEqual(
            _extract_slug_id(
                "https://linear.app/recidiviz/project/us-mo-tasks-v2-74c48ad27169/overview"
            ),
            "74c48ad27169",
        )

    def test_extracts_slug_id_at_end_of_url(self) -> None:
        self.assertEqual(
            _extract_slug_id(
                "https://linear.app/recidiviz/project/us-mo-tasks-v2-74c48ad27169"
            ),
            "74c48ad27169",
        )

    def test_returns_none_for_blank(self) -> None:
        self.assertIsNone(_extract_slug_id(""))

    def test_returns_none_when_no_match(self) -> None:
        self.assertIsNone(_extract_slug_id("https://example.com/not-a-linear-url"))


class ClassifyMilestoneStageTest(unittest.TestCase):
    """Tests for classify_milestone_stage()."""

    def test_matches_fsl_keywords(self) -> None:
        self.assertEqual(classify_milestone_stage("Full State Launch"), LaunchStage.FSL)

    def test_matches_tt_keywords(self) -> None:
        self.assertEqual(
            classify_milestone_stage("Trusted Tester Launch"), LaunchStage.TT
        )

    def test_matches_partial_keywords_pilot_spelling(self) -> None:
        self.assertEqual(classify_milestone_stage("Pilot Launch"), LaunchStage.PARTIAL)

    def test_matches_partial_keywords_partial_spelling(self) -> None:
        self.assertEqual(
            classify_milestone_stage("Partial Launch"), LaunchStage.PARTIAL
        )

    def test_matches_core_scoping_complete_keywords(self) -> None:
        self.assertEqual(
            classify_milestone_stage("Core Scoping Complete"),
            LaunchStage.CORE_SCOPING_COMPLETE,
        )

    def test_matches_core_scoping_complete_keywords_without_core(self) -> None:
        self.assertEqual(
            classify_milestone_stage("Scoping Complete"),
            LaunchStage.CORE_SCOPING_COMPLETE,
        )

    def test_case_insensitive(self) -> None:
        self.assertEqual(classify_milestone_stage("fsl kickoff"), LaunchStage.FSL)

    def test_no_match_returns_none(self) -> None:
        self.assertIsNone(classify_milestone_stage("Design Review"))

    def test_name_matching_multiple_stages_returns_the_earliest(self) -> None:
        # A single Linear milestone must not count for more than one launch
        # stage; the earlier stage in rollout order (TT precedes FSL) wins.
        self.assertEqual(classify_milestone_stage("TT to FSL"), LaunchStage.TT)

    def test_core_scoping_complete_takes_priority_over_tt(self) -> None:
        self.assertEqual(
            classify_milestone_stage("Core Scoping Complete / TT"),
            LaunchStage.CORE_SCOPING_COMPLETE,
        )


class PickStageMilestoneTest(unittest.TestCase):
    """Tests for pick_stage_milestone()."""

    def test_returns_none_when_no_match(self) -> None:
        milestones = [{"name": "Design Review", "targetDate": "2026-01-01"}]
        self.assertIsNone(pick_stage_milestone(milestones, stage=LaunchStage.FSL))

    def test_returns_single_match(self) -> None:
        milestones = [{"name": "FSL", "targetDate": "2026-06-01"}]
        result = pick_stage_milestone(milestones, stage=LaunchStage.FSL)
        assert result is not None
        self.assertEqual(result["targetDate"], "2026-06-01")

    def test_prefers_earliest_dated_match_on_ambiguity(self) -> None:
        milestones = [
            {"name": "FSL (original)", "targetDate": "2026-03-01"},
            {"name": "FSL (replan)", "targetDate": "2026-06-01"},
        ]
        result = pick_stage_milestone(milestones, stage=LaunchStage.FSL)
        assert result is not None
        self.assertEqual(result["name"], "FSL (original)")

    def test_falls_back_to_first_when_no_matches_are_dated(self) -> None:
        milestones = [
            {"name": "FSL (first)", "targetDate": None},
            {"name": "FSL (second)", "targetDate": None},
        ]
        result = pick_stage_milestone(milestones, stage=LaunchStage.FSL)
        assert result is not None
        self.assertEqual(result["name"], "FSL (first)")


class DeriveLaunchStageDatesTest(unittest.TestCase):
    """Tests for derive_launch_stage_dates()."""

    def test_populates_matching_stages_and_nulls_the_rest(self) -> None:
        project = {
            "startDate": "2026-01-01",
            "targetDate": "2026-07-01",
            "health": "onTrack",
            "healthUpdatedAt": "2026-06-15T12:00:00.000Z",
            "milestones": {
                "nodes": [
                    {"name": "TT Launch", "targetDate": "2026-02-01"},
                ]
            },
        }
        result = derive_launch_stage_dates(project)

        self.assertEqual(result["tt_date"], "2026-02-01")
        self.assertEqual(result["tt_milestone_name"], "TT Launch")
        self.assertIsNone(result["fsl_date"])
        self.assertIsNone(result["fsl_milestone_name"])
        self.assertIsNone(result["partial_date"])
        self.assertIsNone(result["partial_milestone_name"])
        self.assertIsNone(result["core_scoping_complete_date"])
        self.assertIsNone(result["core_scoping_complete_milestone_name"])
        self.assertEqual(result["linear_project_start_date"], "2026-01-01")
        self.assertEqual(result["linear_project_target_date"], "2026-07-01")
        self.assertEqual(result["linear_project_health"], "onTrack")
        self.assertEqual(
            result["linear_project_health_updated_at"], "2026-06-15T12:00:00.000Z"
        )

    def test_health_fields_are_null_when_no_update_posted(self) -> None:
        project: dict = {
            "targetDate": "2026-07-01",
            "health": None,
            "healthUpdatedAt": None,
            "milestones": {"nodes": []},
        }
        result = derive_launch_stage_dates(project)

        self.assertIsNone(result["linear_project_health"])
        self.assertIsNone(result["linear_project_health_updated_at"])

    def test_does_not_fall_back_to_project_level_date(self) -> None:
        # A bare project-level targetDate with no matching milestone must not
        # be attributed to any specific launch stage.
        project = {"targetDate": "2026-07-01", "milestones": {"nodes": []}}
        result = derive_launch_stage_dates(project)

        self.assertIsNone(result["fsl_date"])
        self.assertIsNone(result["tt_date"])
        self.assertIsNone(result["partial_date"])
        self.assertIsNone(result["core_scoping_complete_date"])


class BuildSlugIdIndexTest(unittest.TestCase):
    """Tests for build_slug_id_index()."""

    def test_indexes_by_slug_id(self) -> None:
        projects = [
            {"slugId": "abc123def456", "name": "Project A"},
            {"slugId": "789abc012def", "name": "Project B"},
        ]
        index = build_slug_id_index(projects)
        self.assertEqual(index["abc123def456"]["name"], "Project A")
        self.assertEqual(index["789abc012def"]["name"], "Project B")

    def test_later_project_wins_on_duplicate_slug_id(self) -> None:
        projects = [
            {"slugId": "abc123def456", "name": "Old"},
            {"slugId": "abc123def456", "name": "New"},
        ]
        index = build_slug_id_index(projects)
        self.assertEqual(index["abc123def456"]["name"], "New")


class JoinNotionAndLinearTest(unittest.TestCase):
    """Tests for join_notion_and_linear()."""

    def test_joins_matching_rows_and_nulls_unmatched(self) -> None:
        notion_df = pd.DataFrame(
            {
                "initiative": ["US_MO Tasks V2", "US_XX Unmatched"],
                LINEAR_PROJECT_URL_COLUMN: [
                    "https://linear.app/recidiviz/project/us-mo-tasks-v2-74c48ad27169/overview",
                    "https://linear.app/recidiviz/project/us-xx-unmatched-000000000000/overview",
                ],
                LINEAR_PROJECT_SLUG_ID_COLUMN: ["74c48ad27169", "000000000000"],
            }
        )
        linear_projects: list[dict] = [
            {
                "id": "proj-1",
                "slugId": "74c48ad27169",
                "name": "US_MO Tasks V2",
                "state": "started",
                "targetDate": None,
                "milestones": {"nodes": []},
            }
        ]

        result = join_notion_and_linear(notion_df, linear_projects)

        matched_row = result[result["initiative"] == "US_MO Tasks V2"].iloc[0]
        self.assertEqual(matched_row[LINEAR_PROJECT_ID_COLUMN], "proj-1")
        self.assertEqual(matched_row[LINEAR_PROJECT_NAME_COLUMN], "US_MO Tasks V2")
        self.assertEqual(matched_row[LINEAR_PROJECT_STATE_COLUMN], "started")

        unmatched_row = result[result["initiative"] == "US_XX Unmatched"].iloc[0]
        self.assertIsNone(unmatched_row[LINEAR_PROJECT_ID_COLUMN])
        self.assertIsNone(unmatched_row[LINEAR_PROJECT_NAME_COLUMN])

    def test_blank_slug_id_gets_null_linear_columns(self) -> None:
        notion_df = pd.DataFrame(
            {
                "initiative": ["No Linear Project"],
                LINEAR_PROJECT_URL_COLUMN: [""],
                LINEAR_PROJECT_SLUG_ID_COLUMN: [None],
            }
        )

        result = join_notion_and_linear(notion_df, linear_projects=[])

        self.assertIsNone(result.iloc[0][LINEAR_PROJECT_ID_COLUMN])


class GetRoadmapSnapshotSourceTableConfigTest(unittest.TestCase):
    """Tests for get_roadmap_snapshot_source_table_config()."""

    def test_returns_config_matching_registered_yaml(self) -> None:
        config = get_roadmap_snapshot_source_table_config()

        self.assertEqual(config.address.dataset_id, "linear_snapshots")
        self.assertEqual(config.address.table_id, "notion_linear_roadmap_snapshots")
        self.assertTrue(config.has_column("snapshot_date"))
        self.assertTrue(config.has_column("initiative"))
        self.assertTrue(config.has_column(LINEAR_PROJECT_SLUG_ID_COLUMN))
        self.assertTrue(config.has_column("fsl_date"))
        self.assertTrue(config.has_column("partial_date"))
        self.assertTrue(config.has_column("core_scoping_complete_date"))
        self.assertTrue(config.has_column("linear_project_start_date"))
        self.assertTrue(config.has_column("linear_project_health"))
        self.assertTrue(config.has_column("linear_project_health_updated_at"))
