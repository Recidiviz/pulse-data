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
Script to generate a comment body containing information about uv.lock changes

  uv run python -m recidiviz.tools.generate_uv_lock_diff \
    --old-uv-lock-path uv.lock.backup \
    --new-uv-lock-path uv.lock \
    --output-path uv-changes.md
"""
import argparse
import tomllib
from typing import Any

import pandas


def _parse_uv_lock(path: str) -> dict[str, dict[str, Any]]:
    """Parse uv.lock file and return a dict mapping package names to their info."""
    with open(path, "rb") as f:
        data = tomllib.load(f)

    packages = {}
    for pkg in data.get("package", []):
        name = pkg.get("name", "")
        packages[name] = {
            "version": pkg.get("version", ""),
            "source": pkg.get("source", {}).get("registry", ""),
        }
    return packages


def _package_summary_dataframe(packages: dict[str, dict[str, Any]]) -> pandas.DataFrame:
    """Returns a dataframe containing information about packages."""
    return pandas.DataFrame.from_records(
        data=[{"package": name, **info} for name, info in packages.items()],
        index=["package"],
    )


def _format_change_row(row: dict) -> str:
    self_val = str(row.get("self", "")) or "—"
    other_val = str(row.get("other", "")) or "—"

    if self_val != other_val:
        contents = f"```diff\n- {self_val}\n+ {other_val}\n```"
    else:
        contents = f"{self_val}<br/>**{other_val}**"

    return f"<td>\n\n{contents}\n</td>"


def _generate_markdown_table_of_package_difference(
    old_packages: dict[str, dict[str, Any]],
    new_packages: dict[str, dict[str, Any]],
) -> str:
    """Outputs a table of the differences between two package versions."""
    old_df = _package_summary_dataframe(old_packages)
    new_df = _package_summary_dataframe(new_packages)

    # Ensure dataframes are identically labeled
    all_package_names = {*old_df.index.values, *new_df.index.values}
    old_df = (
        old_df.reindex(all_package_names)
        .sort_index(axis="index")
        .sort_index(axis="columns")
    )
    new_df = (
        new_df.reindex(all_package_names)
        .sort_index(axis="index")
        .sort_index(axis="columns")
    )

    changes = old_df.compare(new_df).fillna("—")

    if changes.empty:
        return "<p>No package changes detected.</p>"

    rows = "\n".join(
        [
            "<tr>"
            + "\n".join(
                [
                    f"<td>\n\n`{package_name}`</td>",
                    _format_change_row(row.get("version", {"self": "", "other": ""})),
                    _format_change_row(row.get("source", {"self": "", "other": ""})),
                ]
            )
            + "</tr>"
            for package_name, row in changes.iterrows()
        ]
    )

    column_padding = "&nbsp;" * 32
    return f"""<table>
    <thead>
    <tr>
        <th scope="col" align="left">Package Name</th>
        <th scope="col" align="left">Version{column_padding}</th>
        <th scope="col" align="left">Source</th>
    </tr>
    </thead>
    <tbody>
        {rows}
    </tbody>
    </table>
    """


def main() -> None:
    """Runs the CLI entrypoint for this script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-uv-lock-path", type=str, required=True)
    parser.add_argument("--new-uv-lock-path", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True)
    args = parser.parse_args()

    old_packages = _parse_uv_lock(args.old_uv_lock_path)
    new_packages = _parse_uv_lock(args.new_uv_lock_path)

    change_table = _generate_markdown_table_of_package_difference(
        old_packages,
        new_packages,
    )

    with open(args.output_path, mode="w", encoding="utf-8") as f:
        f.write(
            "# `uv.lock` change summary \n"
            "* `-` for both (old) and (new) indicates no change \n"
            "<details>\n<summary>\n\n## Package Changes \n</summary>"
            f"{change_table} \n"
            "</details>"
        )


if __name__ == "__main__":
    main()
