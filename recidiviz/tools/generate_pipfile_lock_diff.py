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
#  =============================================================================
"""
Script to generate a comment body containing information about Pipfile.lock changes

  pipenv run python -m recidiviz.tools.generate_pipfile_lock_diff \
    --old-pipfile-lock-path Pipfile.lock.backup \
    --new-pipfile-lock-path Pipfile.lock \
    --output-path pipfile-changes.md
"""
import argparse
import enum
import json
from typing import Any

import pandas


class PipfileGroup(enum.StrEnum):
    DEFAULT = "default"
    DEVELOP = "develop"


PipfileLockJSON = dict[str, dict[str, dict[str, Any]]]


def _package_summary_dataframe_for_group(
    pipfile: PipfileLockJSON, group: PipfileGroup
) -> pandas.DataFrame:
    """Returns a dataframe containing information about the packages for a Pipfile group, without hashes information"""
    return pandas.DataFrame.from_records(
        data=[
            {"package": package_name, **contents}
            for package_name, contents in pipfile[group.value].items()
        ],
        index=["package"],
    ).drop(labels=["hashes"], axis="columns")


def _format_change_row(row: dict) -> str:
    self = str(row["self"]).lstrip("=") or "—"
    other = str(row["other"]).lstrip("=") or "—"

    if self != other:
        contents = f"```diff\n- {self}\n+ {other}\n```"
    else:
        contents = f"{self}<br/>**{other}**"

    # The newline before the contents is significant; GitHub won't process markdown without it
    return f"<td>\n\n{contents}\n</td>"


def _generate_markdown_table_of_package_group_difference(
    old_pipfile_lock_json: PipfileLockJSON,
    new_pipfile_lock_json: PipfileLockJSON,
    group: PipfileGroup,
) -> str:
    """Outputs a table row of the differences between two package versions"""
    old_pipfile_lock = _package_summary_dataframe_for_group(
        old_pipfile_lock_json, group
    )
    new_pipfile_lock = _package_summary_dataframe_for_group(
        new_pipfile_lock_json, group
    )

    # Ensure dataframes are identically labeled by merging their indices
    all_package_names = {*old_pipfile_lock.index.values, *new_pipfile_lock.index.values}
    old_pipfile_lock = old_pipfile_lock.reindex(all_package_names).sort_index()
    new_pipfile_lock = new_pipfile_lock.reindex(all_package_names).sort_index()

    changes = old_pipfile_lock.compare(new_pipfile_lock).fillna("—")

    rows = "\n".join(
        [
            "<tr>"
            + "\n".join(
                [
                    # The newline before the contents is significant; GitHub won't process markdown without it
                    f"<td>\n\n`{package_name}`</td>",
                    _format_change_row(row["version"]),
                    _format_change_row(row.get("markers", {"self": "", "other": ""})),
                    _format_change_row(row.get("extras", {"self": "", "other": ""})),
                ]
            )
            + "</tr>"
            for package_name, row in changes.iterrows()
        ]
    )

    # Add some additional padding to the version column width
    column_padding = "&nbsp;" * 32
    return f"""<table>
    <thead>
    <tr>
        <th scope="col" align="left">Package Name</th>
        <th scope="col" align="left">Version{column_padding}</th>
        <th scope="col" align="left">Markers</th>
        <th scope="col" align="left">Extras</th>
    </tr>
    </thead>
    <tbody>
        {rows}
    </tbody>
    </table>
    """


def main() -> None:
    """Runs the CLI entrypoint for this script. See module docstring for more info"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-pipfile-lock-path", type=str, required=True)
    parser.add_argument("--new-pipfile-lock-path", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True)
    args = parser.parse_args()

    with open(args.old_pipfile_lock_path, "r", encoding="utf-8") as f:
        old_pipfile_lock_json = json.load(f)

    with open(args.new_pipfile_lock_path, "r", encoding="utf-8") as f:
        new_pipfile_lock_json = json.load(f)

    default_change_table = _generate_markdown_table_of_package_group_difference(
        old_pipfile_lock_json,
        new_pipfile_lock_json,
        PipfileGroup.DEFAULT,
    )

    develop_change_table = _generate_markdown_table_of_package_group_difference(
        old_pipfile_lock_json,
        new_pipfile_lock_json,
        PipfileGroup.DEVELOP,
    )

    with open(args.output_path, mode="w", encoding="utf-8") as f:
        f.write(
            "# `Pipfile.lock` change summary \n"
            "* `-` for both (old) and (new) indicates no change \n"
            "<details>\n<summary>\n\n## `default` \n</summary>"
            f"{default_change_table} \n"
            "</details>"
            "<details>\n<summary>\n\n## `develop` \n</summary>"
            f"{develop_change_table} \n"
            "</details>"
        )


if __name__ == "__main__":
    main()
