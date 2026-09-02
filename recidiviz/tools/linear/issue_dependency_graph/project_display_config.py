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
"""Checked-in configuration that says which slice of a Linear project the
dependency graph shows."""
import os
from pathlib import Path

import attr

from recidiviz.common import attr_validators
from recidiviz.tools.linear import issue_dependency_graph
from recidiviz.utils.yaml_dict import YAMLDict

CONFIG_FILE_SUFFIX = ".yaml"


def project_display_configs_dir() -> str:
    """Returns the directory holding the checked-in project display configs."""
    return os.path.join(
        os.path.dirname(issue_dependency_graph.__file__), "project_display_configs"
    )


def config_path_for_name(config_name: str) -> str:
    """Returns the path of the checked-in config with the given name, which is the
    config's file name without its `.yaml` suffix.

    For example, `case_note_insights` resolves to
    `.../project_display_configs/case_note_insights.yaml`.
    """
    return os.path.join(
        project_display_configs_dir(), f"{config_name}{CONFIG_FILE_SUFFIX}"
    )


@attr.define(frozen=True, kw_only=True)
class MilestoneDisplayConfig:
    """One Linear project milestone that the dependency graph shows."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The milestone's name in Linear. Must match exactly."""

    short_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Abbreviation shown on the milestone's box and on each of its issues, e.g.
    `P1`. Linear has no field for this, so the config supplies it. Milestone names
    are too long to fit on a graph node.
    """

    @classmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> "MilestoneDisplayConfig":
        """Returns the milestone config parsed from one entry of a config file's
        `milestones` list."""
        config = MilestoneDisplayConfig(
            name=yaml_dict.pop("name", str),
            short_name=yaml_dict.pop("short_name", str),
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for milestone [{config.name}]: "
                f"{repr(yaml_dict.get())}"
            )
        return config


@attr.define(frozen=True, kw_only=True)
class ProjectDisplayConfig:
    """Configuration that says which slice of a Linear project the dependency graph
    shows, and how it displays that slice."""

    project_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project's name in Linear. Must match exactly, and must match exactly one
    project.
    """

    milestones: list[MilestoneDisplayConfig] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(MilestoneDisplayConfig),
        ]
    )
    """The project milestones the graph shows. This is an include list: a milestone
    the project gains later stays out of the graph until it is added here. Graph
    order comes from each milestone's `sortOrder` in Linear, not from this list.
    """

    exclude_parent_issues: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether to drop issues that have sub-issues. These are usually containers
    whose blocking relations duplicate their children's.
    """

    labels_excluded_by_default: list[str] = attr.ib(
        validator=attr_validators.is_list_of_non_empty_str
    )
    """Labels whose issues the rendered page hides on load. Every label is a
    checkbox in the page's "Labels to exclude" dropdown, so a reader can hide or
    restore any label without a re-fetch. These labels start checked.
    """

    def __attrs_post_init__(self) -> None:
        names = [milestone.name for milestone in self.milestones]
        duplicate_names = {name for name in names if names.count(name) > 1}
        if duplicate_names:
            raise ValueError(
                f"Project display config for [{self.project_name}] lists duplicate "
                f"milestone names: {sorted(duplicate_names)}."
            )
        short_names = [milestone.short_name for milestone in self.milestones]
        duplicate_short_names = {
            short_name
            for short_name in short_names
            if short_names.count(short_name) > 1
        }
        if duplicate_short_names:
            raise ValueError(
                f"Project display config for [{self.project_name}] lists duplicate "
                f"milestone short names: {sorted(duplicate_short_names)}."
            )

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "ProjectDisplayConfig":
        """Returns the project display config parsed from the YAML file at the given
        path."""
        file_contents = YAMLDict.from_path(str(yaml_path))
        config = ProjectDisplayConfig(
            project_name=file_contents.pop("project_name", str),
            milestones=[
                MilestoneDisplayConfig.from_yaml_dict(milestone_dict)
                for milestone_dict in file_contents.pop_dicts("milestones")
            ],
            exclude_parent_issues=file_contents.pop("exclude_parent_issues", bool),
            labels_excluded_by_default=file_contents.pop_list_optional(
                "labels_excluded_by_default", str
            )
            or [],
        )
        if file_contents:
            raise ValueError(
                f"Found unexpected config values in [{yaml_path}]: "
                f"{repr(file_contents.get())}"
            )
        return config
