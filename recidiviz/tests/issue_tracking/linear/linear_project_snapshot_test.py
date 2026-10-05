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
"""Tests for recidiviz/issue_tracking/linear/linear_project_snapshot.py"""
import datetime
import unittest
from unittest.mock import MagicMock

from recidiviz.issue_tracking.linear.linear_project_snapshot import (
    LINEAR_ENGINEERING_SIZE_LABEL_COLUMN,
    LINEAR_LABELS_COLUMN,
    LINEAR_PRODUCT_LABEL_COLUMN,
    LINEAR_STATE_LABEL_COLUMN,
    LaunchStage,
    build_linear_project_snapshot_rows,
    classify_milestone_stage,
    derive_label_columns,
    derive_launch_stage_dates,
    pick_stage_milestone,
    write_linear_project_snapshot,
)
from recidiviz.issue_tracking.roadmap_snapshot_tables import (
    LINEAR_PROJECT_SNAPSHOTS_ADDRESS,
)
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)


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
            "milestones": {"nodes": [{"name": "TT Launch", "targetDate": "2026-02-01"}]}
        }

        self.assertEqual(
            derive_launch_stage_dates(project),
            {
                "core_scoping_complete_date": None,
                "core_scoping_complete_milestone_name": None,
                "tt_date": "2026-02-01",
                "tt_milestone_name": "TT Launch",
                "partial_date": None,
                "partial_milestone_name": None,
                "fsl_date": None,
                "fsl_milestone_name": None,
            },
        )

    def test_project_with_no_milestones_gets_null_stage_columns(self) -> None:
        self.assertEqual(
            derive_launch_stage_dates({"milestones": {"nodes": []}}),
            {
                "core_scoping_complete_date": None,
                "core_scoping_complete_milestone_name": None,
                "tt_date": None,
                "tt_milestone_name": None,
                "partial_date": None,
                "partial_milestone_name": None,
                "fsl_date": None,
                "fsl_milestone_name": None,
            },
        )


class DeriveLabelColumnsTest(unittest.TestCase):
    """Tests for derive_label_columns()."""

    def test_splits_grouped_labels_into_columns(self) -> None:
        project = {
            "name": "US_XX Meetings",
            "labels": {
                "nodes": [
                    {"name": "US_XX", "parent": {"name": "States"}},
                    {"name": "Meetings", "parent": {"name": "Products"}},
                    {"name": "M", "parent": {"name": "Engineering Size"}},
                    {"name": "H2 2026", "parent": None},
                    {"name": "📐", "parent": {"name": "Design"}},
                ]
            },
        }

        result = derive_label_columns(project)

        self.assertEqual(result[LINEAR_STATE_LABEL_COLUMN], "US_XX")
        self.assertEqual(result[LINEAR_PRODUCT_LABEL_COLUMN], "Meetings")
        self.assertEqual(result[LINEAR_ENGINEERING_SIZE_LABEL_COLUMN], "M")
        self.assertEqual(
            result[LINEAR_LABELS_COLUMN],
            "Design/📐, Engineering Size/M, H2 2026, Products/Meetings, States/US_XX",
        )

    def test_unlabeled_project_gets_null_columns(self) -> None:
        result = derive_label_columns({"name": "US_XX Tool", "labels": {"nodes": []}})

        self.assertEqual(
            result,
            {
                LINEAR_STATE_LABEL_COLUMN: None,
                LINEAR_PRODUCT_LABEL_COLUMN: None,
                LINEAR_ENGINEERING_SIZE_LABEL_COLUMN: None,
                LINEAR_LABELS_COLUMN: None,
            },
        )

    def test_raises_on_two_labels_in_one_group(self) -> None:
        project = {
            "name": "US_XX Tool",
            "labels": {
                "nodes": [
                    {"name": "Tasks", "parent": {"name": "Products"}},
                    {"name": "Meetings", "parent": {"name": "Products"}},
                ]
            },
        }

        with self.assertRaisesRegex(
            ValueError,
            r"^Linear project \[US_XX Tool\] has more than one label in group "
            r"\[Products\]: \[Tasks\] and \[Meetings\]\.",
        ):
            derive_label_columns(project)


class BuildLinearProjectSnapshotRowsTest(unittest.TestCase):
    """Tests for build_linear_project_snapshot_rows()."""

    def test_builds_one_row_per_project(self) -> None:
        projects: list[dict] = [
            {
                "id": "proj-1",
                "slugId": "74c48ad27169",
                "name": "[US_XX] Tasks V2",
                "state": "started",
                "status": {"name": "TT"},
                "startDate": "2026-06-01",
                "targetDate": "2026-11-02",
                "health": "atRisk",
                "healthUpdatedAt": "2026-09-28T15:00:00.000Z",
                "milestones": {
                    "nodes": [
                        {"name": "TT Launch", "targetDate": "2026-09-14"},
                        {"name": "FSL", "targetDate": "2026-11-02"},
                    ]
                },
                "labels": {
                    "nodes": [
                        {"name": "US_XX", "parent": {"name": "States"}},
                        {"name": "Tasks", "parent": {"name": "Products"}},
                    ]
                },
            },
            {
                "id": "proj-2",
                "slugId": "5c55d9954a2a",
                "name": "Ingest backlog",
                "state": "backlog",
                "status": {"name": "Backlog"},
                "startDate": None,
                "targetDate": None,
                "health": None,
                "healthUpdatedAt": None,
                "milestones": {"nodes": []},
                "labels": {"nodes": []},
            },
        ]

        rows = build_linear_project_snapshot_rows(
            projects,
            snapshot_datetime=datetime.datetime(
                2026, 10, 1, 9, 30, tzinfo=datetime.timezone.utc
            ),
        )

        self.assertEqual(
            rows,
            [
                {
                    "linear_project_id": "proj-1",
                    "linear_project_slug_id": "74c48ad27169",
                    "linear_project_name": "[US_XX] Tasks V2",
                    "linear_project_state": "started",
                    "linear_project_status": "TT",
                    "linear_project_start_date": "2026-06-01",
                    "linear_project_target_date": "2026-11-02",
                    "linear_project_health": "atRisk",
                    "linear_project_health_updated_at": "2026-09-28T15:00:00.000Z",
                    "core_scoping_complete_date": None,
                    "core_scoping_complete_milestone_name": None,
                    "tt_date": "2026-09-14",
                    "tt_milestone_name": "TT Launch",
                    "partial_date": None,
                    "partial_milestone_name": None,
                    "fsl_date": "2026-11-02",
                    "fsl_milestone_name": "FSL",
                    "linear_state_label": "US_XX",
                    "linear_product_label": "Tasks",
                    "linear_engineering_size_label": None,
                    "linear_labels": "Products/Tasks, States/US_XX",
                    "snapshot_date": "2026-10-01",
                    "upload_datetime": "2026-10-01T09:30:00+00:00",
                },
                {
                    "linear_project_id": "proj-2",
                    "linear_project_slug_id": "5c55d9954a2a",
                    "linear_project_name": "Ingest backlog",
                    "linear_project_state": "backlog",
                    "linear_project_status": "Backlog",
                    "linear_project_start_date": None,
                    "linear_project_target_date": None,
                    "linear_project_health": None,
                    "linear_project_health_updated_at": None,
                    "core_scoping_complete_date": None,
                    "core_scoping_complete_milestone_name": None,
                    "tt_date": None,
                    "tt_milestone_name": None,
                    "partial_date": None,
                    "partial_milestone_name": None,
                    "fsl_date": None,
                    "fsl_milestone_name": None,
                    "linear_state_label": None,
                    "linear_product_label": None,
                    "linear_engineering_size_label": None,
                    "linear_labels": None,
                    "snapshot_date": "2026-10-01",
                    "upload_datetime": "2026-10-01T09:30:00+00:00",
                },
            ],
        )

    def test_row_columns_match_registered_schema(self) -> None:
        project: dict = {
            "id": "proj-2",
            "slugId": "5c55d9954a2a",
            "name": "Ingest backlog",
            "state": "backlog",
            "status": {"name": "Backlog"},
            "startDate": None,
            "targetDate": None,
            "health": None,
            "healthUpdatedAt": None,
            "milestones": {"nodes": []},
            "labels": {"nodes": []},
        }
        config = build_source_table_repository_for_yaml_managed_tables(
            project_id=None
        ).get_config(LINEAR_PROJECT_SNAPSHOTS_ADDRESS)

        rows = build_linear_project_snapshot_rows(
            [project],
            snapshot_datetime=datetime.datetime(
                2026, 10, 1, tzinfo=datetime.timezone.utc
            ),
        )

        self.assertEqual(
            list(rows[0].keys()), [field.name for field in config.schema_fields]
        )


class WriteLinearProjectSnapshotTest(unittest.TestCase):
    """Tests for write_linear_project_snapshot()."""

    def test_loads_rows_into_snapshot_table(self) -> None:
        bq_client = MagicMock()
        rows: list[dict[str, str | None]] = [{"linear_project_id": "proj-1"}]

        write_linear_project_snapshot(bq_client=bq_client, rows=rows)

        bq_client.load_into_table_async.assert_called_once_with(
            address=LINEAR_PROJECT_SNAPSHOTS_ADDRESS, rows=rows
        )
        bq_client.load_into_table_async.return_value.result.assert_called_once()

    def test_raises_on_no_rows(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No Linear projects to snapshot; the Linear API returned none\.",
        ):
            write_linear_project_snapshot(bq_client=MagicMock(), rows=[])
